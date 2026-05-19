from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("SensorPipeline") \
    .getOrCreate()

schema = StructType([
    StructField("sensor", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("timestamp", LongType()),
    StructField("source", StringType()),
    StructField("anomaly", BooleanType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-events") \
    .load()

json_df = df.select(from_json(col("value").cast("string"), schema).alias("d")) \
            .select("d.*") \
            .withColumn("event_time", from_unixtime(col("timestamp") / 1000).cast("timestamp"))

# 异常检测（独立）
anomaly_df = json_df.withColumn(
    "is_anomaly",
    col("sensor") == "temperature" & (col("value") > 35) |
    col("sensor") == "humidity" & (col("value") > 90) |
    col("sensor") == "pressure" & ((col("value") < 990) | (col("value") > 1030))
)

# 窗口聚合
windowed = anomaly_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window("event_time", "5 minutes"), "sensor") \
    .agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count")
    )

query = windowed.writeStream \
    .format("parquet") \
    .option("path", "/tmp/datalake/consumption") \
    .option("checkpointLocation", "/tmp/checkpoints/consumption") \
    .outputMode("append") \
    .start()

query.awaitTermination()