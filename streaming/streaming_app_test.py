from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

# ========================= НАСТРОЙКИ =========================
MODEL_PATH = "/opt/spark/models/content/best_model"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "my-topic"

CHECKPOINT_LOCATION = "/tmp/checkpoint/test_streaming"
# ============================================================

spark = SparkSession.builder \
    .appName("PredictiveMaintenance-Test") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
    .getOrCreate()

try:
    model = PipelineModel.load(MODEL_PATH)
    print("✅ Модель успешно загружена!")
except Exception as e:
    print(f"❌ Ошибка загрузки модели: {e}")
    print("Будем работать без модели")

# ====================== СХЕМА ======================
schema = StructType([
    StructField("Air temperature [K]", DoubleType(), True),
    StructField("Process temperature [K]", DoubleType(), True),
    StructField("Rotational speed [rpm]", IntegerType(), True),
    StructField("Torque [Nm]", DoubleType(), True),
    StructField("Tool wear [min]", IntegerType(), True),
    StructField("Timestamp", StringType(), True)
])

# ====================== ЧТЕНИЕ ИЗ KAFKA ======================
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

df = raw_df \
    .selectExpr("CAST(value AS STRING) as line") \
    .select(from_csv(col("line"), "`Air temperature [K]` DOUBLE, `Process temperature [K]` DOUBLE, `Rotational speed [rpm]` INT, `Torque [Nm]` DOUBLE, `Tool wear [min]` INT, Timestamp STRING", {"header": "false", "sep": ","}).alias("data")) \
    .select("data.*")

# Приводим Timestamp
df = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

# ====================== ПОДГОТОВКА ДЛЯ МОДЕЛИ ======================
features = [
    'Air temperature [K]', 
    'Process temperature [K]', 
    'Rotational speed [rpm]', 
    'Torque [Nm]', 
    'Tool wear [min]'
]

# Берем только нужные фичи для модели
input_df = df.select(features)

# Применяем модель
try:
    predictions = model.transform(input_df)
    print("✅ Предсказания выполнены!")
    final_df = predictions \
        .withColumn("failure_probability", element_at(col("probability"), 2)) \
        .withColumn("prediction_label", when(col("prediction") == 1, "FAILURE").otherwise("NORMAL")) \
        .select(
            current_timestamp().alias("time"),
            col("`Air temperature [K]`").alias("air_temp"),
            col("`Process temperature [K]`").alias("process_temp"),
            col("`Rotational speed [rpm]`").alias("rot_speed"),
            col("`Torque [Nm]`").alias("torque"),
            col("`Tool wear [min]`").alias("tool_wear"),
            col("prediction"),
            col("prediction_label"),
            round(col("failure_probability"), 4).alias("failure_probability")
        )
except NameError:
    print("⚠️ Работаем без модели")
    final_df = input_df \
        .withColumn("failure_probability", lit(0.5)) \
        .withColumn("prediction_label", lit("UNKNOWN")) \
        .withColumn("prediction", lit(0)) \
        .select(
            current_timestamp().alias("time"),
            col("`Air temperature [K]`").alias("air_temp"),
            col("`Process temperature [K]`").alias("process_temp"),
            col("`Rotational speed [rpm]`").alias("rot_speed"),
            col("`Torque [Nm]`").alias("torque"),
            col("`Tool wear [min]`").alias("tool_wear"),
            col("prediction"),
            col("prediction_label"),
            col("failure_probability")
        )

# ====================== ВЫВОД В КОНСОЛЬ ======================
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 15) \
    .trigger(processingTime="5 seconds") \
    .start()

print("🚀 Тестовый стриминг запущен! Ждём данные из Kafka...")
query.awaitTermination()