spark.read.parquet("/tmp/datalake/consumption") \
      .createOrReplaceTempView("sensor_stats")

spark.sql("""
SELECT hour(event_time) AS hr, SUM(anomaly_count) AS anomalies
FROM sensor_stats
GROUP BY hr
ORDER BY anomalies DESC
LIMIT 5
""").show()