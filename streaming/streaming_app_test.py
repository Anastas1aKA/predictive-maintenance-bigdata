from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.functions import vector_to_array

from influxdb_client import InfluxDBClient, Point, WritePrecision
from datetime import datetime


# ========================= НАСТРОЙКИ =========================
MODEL_PATH = "file:///opt/spark/models/content/best_model_fixed"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "my-topic"

CHECKPOINT_LOCATION = "/tmp/checkpoint/test_streaming"


client = InfluxDBClient(
    url="http://influxdb:8086",
    token="my-super-secret-token-123456",
    org="myorg"
)

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
    .option("startingOffsets", "earliest") \
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

    if model is None:
        print("Модель не загружена — пропуск batch")
        return

    pred = model.transform(batch_df)

    result = pred.select(
        current_timestamp().alias("time"),
        col("Air temperature [K]").alias("air_temp"),
        col("Process temperature [K]").alias("process_temp"),
        col("Rotational speed [rpm]").alias("rot_speed"),
        col("Torque [Nm]").alias("torque"),
        col("Tool wear [min]").alias("tool_wear"),
        col("prediction"),
        vector_to_array(col("probability"))[1].alias("failure_probability"),
        when(col("prediction") == 1, "FAILURE")
            .otherwise("NORMAL")
            .alias("prediction_label")
    )

    result.show(truncate=False)

    rows = result.collect()

    for row in rows:

        point = (
            Point("machine_metrics")
            .time(row.time)
            .field("air_temp", float(row.air_temp))
            .field("process_temp", float(row.process_temp))
            .field("rot_speed", float(row.rot_speed))
            .field("torque", float(row.torque))
            .field("tool_wear", float(row.tool_wear))
            .field("prediction", int(row.prediction))
            .field("failure_probability", float(row.failure_probability))
            .tag("label", row.prediction_label)
        )

        write_api.write(
            bucket="predictions",
            org="myorg",
            record=point
        )

    print(f"Batch {epoch_id} сохранён")


query = df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "./checkpoint") \
    .start()

query.awaitTermination()
print("Streaming запущен")
