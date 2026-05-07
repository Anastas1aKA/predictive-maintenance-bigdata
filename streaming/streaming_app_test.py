from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.functions import vector_to_array

# ========================= НАСТРОЙКИ =========================
MODEL_PATH = "file:///opt/spark/models/content/best_model_fixed"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "my-topic"

CHECKPOINT_LOCATION = "/tmp/checkpoint/test_streaming"
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

    # применяем модель к батчу
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
        when(col("prediction") == 1, "FAILURE").otherwise("NORMAL").alias("prediction_label")
    )

    result.show(truncate=False)

# ====================== STREAM START ======================

query = df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

print("Streaming запущен")

query.awaitTermination()