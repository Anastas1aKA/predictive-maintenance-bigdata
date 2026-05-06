from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

spark = SparkSession.builder \
    .appName("PredictiveMaintenance") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

MODEL_PATH = "../models/content/best_model"

model = PipelineModel.load(MODEL_PATH)

schema = StructType([
    StructField("Air temperature [K]", DoubleType(), True),
    StructField("Process temperature [K]", DoubleType(), True),
    StructField("Rotational speed [rpm]", IntegerType(), True),
    StructField("Torque [Nm]", DoubleType(), True),
    StructField("Tool wear [min]", IntegerType(), True),
    StructField("Timestamp", StringType(), True)
])

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "earliest") \
    .load()

df = raw_df \
    .selectExpr("CAST(value AS STRING) as line") \
    .select(from_csv(col("line"), schema, {"header": "false", "sep": ","}).alias("data")) \
    .select("data.*")

df = df.withColumn("Timestamp", to_timestamp(col("Timestamp"), "yyyy-MM-dd HH:mm:ss"))

features = ['Air temperature [K]', 'Process temperature [K]', 'Rotational speed [rpm]', 'Torque [Nm]', 'Tool wear [min]']

input_df = df.select(features)

predictions = model.transform(input_df)

final_df = predictions.withColumn("Timestamp", df["Timestamp"])


