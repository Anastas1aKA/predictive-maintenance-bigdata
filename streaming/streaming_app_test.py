from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.functions import vector_to_array
from collections import deque
import pandas as pd

from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime


# ========================= НАСТРОЙКИ =========================
MODEL_PATH = "file:///opt/spark/models/content/best_model_fixed"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "my-topic"

CHECKPOINT_LOCATION = "/tmp/checkpoint/test_streaming"

M = 10
# Глобальный буфер для последних M записей (для rolling features)
history_buffer = deque(maxlen=M)


client = InfluxDBClient(
    url="http://influxdb:8086",
    token="my-super-secret-token-123456",
    org="myorg"
)

# Создаем bucket, если его нет
bucket_name = "predictions"
buckets_api = client.buckets_api()

# Проверяем, существует ли bucket
existing_buckets = buckets_api.find_buckets().buckets
bucket_exists = any(bucket.name == bucket_name for bucket in existing_buckets)

if not bucket_exists:
    # Создаем bucket с retention policy (например, 30 дней)
    retention_seconds = 30 * 24 * 60 * 60  # 30 дней
    buckets_api.create_bucket(
        bucket_name=bucket_name,
        retention_rules=[{"type": "expire", "everySeconds": retention_seconds}],
        org_id=client.org
    )
    print(f"Bucket '{bucket_name}' создан")
else:
    print(f"Bucket '{bucket_name}' уже существует")


write_api = client.write_api()
# ============================================================

# Spark Session
spark = SparkSession.builder \
    .appName("PredictiveMaintenance-Test") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
    .getOrCreate()

# ====================== ЗАГРУЗКА МОДЕЛИ ======================
model = None

try:
    model = PipelineModel.load(MODEL_PATH)
    print("Модель успешно загружена")
    print(type(model))
except Exception as e:
    print("Ошибка загрузки модели:", e)



# ====================== СХЕМА ======================
schema = StructType([
    StructField("Air temperature [K]", DoubleType(), True),
    StructField("Process temperature [K]", DoubleType(), True),
    StructField("Rotational speed [rpm]", IntegerType(), True),
    StructField("Torque [Nm]", DoubleType(), True),
    StructField("Tool wear [min]", IntegerType(), True),
    StructField("Timestamp", StringType(), True)
])

# ====================== KAFKA STREAM ======================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df = raw_df.selectExpr("CAST(value AS STRING) as line") \
    .select(from_csv(
        col("line"),
        "`Air temperature [K]` DOUBLE, `Process temperature [K]` DOUBLE, `Rotational speed [rpm]` INT, `Torque [Nm]` DOUBLE, `Tool wear [min]` INT, Timestamp STRING",
        {"sep": ","}
    ).alias("data")) \
    .select("data.*")

df = df.withColumn("Timestamp", to_timestamp(col("Timestamp")))

# ====================== FEATURE PIPELINE ======================

features = [
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]"
]

# ====================== STREAMING LOGIC ======================

def process_batch(batch_df, epoch_id):
    global history_buffer
    
    if batch_df.isEmpty():
        print(f"Epoch {epoch_id}: Пустой batch")
        return

    pdf = batch_df.toPandas()
    results = []

    for _, row in pdf.iterrows():
        current = row.to_dict()
        
        # Добавляем текущую запись в буфер
        history_buffer.append(current)
        hist_df = pd.DataFrame(list(history_buffer))

        # ==================== FEATURE ENGINEERING ====================
        row_dict = {
            "Air temperature [K]": current["Air temperature [K]"],
            "Process temperature [K]": current["Process temperature [K]"],
            "Rotational speed [rpm]": current["Rotational speed [rpm]"],
            "Torque [Nm]": current["Torque [Nm]"],
            "Tool wear [min]": current["Tool wear [min]"],
            
            # Rolling features
            "torque_mean": hist_df["Torque [Nm]"].mean(),
            "torque_std": hist_df["Torque [Nm]"].std(ddof=0),
            "torque_max": hist_df["Torque [Nm]"].max(),
            "rot_speed_mean": hist_df["Rotational speed [rpm]"].mean(),
            "tool_wear_mean": hist_df["Tool wear [min]"].mean(),
            
            # Tool wear rate
            "tool_wear_rate": current["Tool wear [min]"] - hist_df["Tool wear [min]"].iloc[-2] 
                              if len(hist_df) > 1 else 0.0,
            
            "power": current["Torque [Nm]"] * current["Rotational speed [rpm]"],
            "temp_diff": current["Process temperature [K]"] - current["Air temperature [K]"],
        }

        # Создаём pandas DataFrame
        input_pdf = pd.DataFrame([row_dict])
        input_pdf = input_pdf.fillna(0)

        # ==================== ПРЕДСКАЗАНИЕ ====================
        spark_input = spark.createDataFrame(input_pdf)
        pred_df = model.transform(spark_input)
        
        # ИСПРАВЛЕННЫЙ БЛОК:
        pred_row = pred_df.select("prediction", "probability").first()
        prediction = int(pred_row[0])
        # Конвертируем DenseVector в массив и берем второй элемент (вероятность класса 1)
        probability = float(pred_row[1].toArray()[1])  # [0] - класс 0, [1] - класс 1

        # ==================== РЕЗУЛЬТАТ ====================
        results.append({
            "time": datetime.utcnow(),
            "air_temp": float(current["Air temperature [K]"]),
            "process_temp": float(current["Process temperature [K]"]),
            "rot_speed": int(current["Rotational speed [rpm]"]),
            "torque": float(current["Torque [Nm]"]),
            "tool_wear": int(current["Tool wear [min]"]),
            "prediction": prediction,
            "failure_probability": probability,
            "prediction_label": "FAILURE" if prediction == 1 else "NORMAL"
        })

    # ====================== ЗАПИСЬ В INFLUXDB ======================
    if results:
        result_pdf = pd.DataFrame(results)
        print(f"\nEpoch {epoch_id} — {len(results)} записей")
        print(result_pdf[["prediction_label", "failure_probability"]].round(4))
        
        for _, r in result_pdf.iterrows():
            point = (
                Point("machine_metrics")
                .time(r["time"], WritePrecision.NS)
                .field("air_temp", r["air_temp"])
                .field("process_temp", r["process_temp"])
                .field("rot_speed", r["rot_speed"])
                .field("torque", r["torque"])
                .field("tool_wear", r["tool_wear"])
                .field("prediction", r["prediction"])
                .field("failure_probability", r["failure_probability"])
                .tag("label", r["prediction_label"])
            )
            try:
                write_api.write(bucket="predictions", org="myorg", record=point)
            except Exception as e:
                print("Influx write error:", e)

    print(f"Batch {epoch_id} обработан успешно")


query = df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

query.awaitTermination()
print("Streaming запущен")
