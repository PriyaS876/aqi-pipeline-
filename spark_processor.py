"""
spark_processor.py

Kafka topic 'aqi-readings' se real-time PM2.5 data padhta hai,
process karta hai (parsing + anomaly flagging), aur PostgreSQL mein save karta hai.

Chalane ka tareeka:
    python spark_processor.py

Isse chalane se pehle fetch_data.py ek alag terminal mein chal raha hona chahiye
(taaki Kafka topic mein naya data aata rahe), aur PostgreSQL container bhi chal raha ho.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ---------------------------------------------------------
# 1. Spark session banao, Kafka + PostgreSQL connectors ke saath
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("AQI-Stream-Processor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata") \
    .config("spark.sql.session.timeZone", "Asia/Kolkata") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # kam logs, sirf warnings/errors dikhenge

# PostgreSQL connection settings
PG_URL = "jdbc:postgresql://localhost:5433/aqi_db"
PG_PROPERTIES = {
    "user": "aqi_user",
    "password": "aqi_password",
    "driver": "org.postgresql.Driver"
}

# ---------------------------------------------------------
# 2. Kafka messages ka JSON structure define karo
#    (yeh match karna chahiye fetch_data.py jo bhejta hai usse)
# ---------------------------------------------------------
aqi_schema = StructType([
    StructField("station", StringType()),
    StructField("location_id", StringType()),
    StructField("sensor_id", StringType()),
    StructField("parameter", StringType()),
    StructField("value", DoubleType()),
    StructField("unit", StringType()),
    StructField("timestamp_utc", StringType()),
    StructField("fetched_at", StringType()),
])

# ---------------------------------------------------------
# 3. Kafka topic se stream padhna shuru karo
# ---------------------------------------------------------
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "aqi-readings") \
    .option("startingOffsets", "earliest") \
    .load()

# ---------------------------------------------------------
# 4. Kafka messages binary hote hain -> JSON mein parse karo
# ---------------------------------------------------------
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), aqi_schema).alias("data")
).select("data.*")

# ---------------------------------------------------------
# 5. Anomaly detection: PM2.5 = 0.0 ya negative hona suspicious hai
#    (Delhi-NCR jaise shehron mein asli air kabhi itni clean nahi hoti)
# ---------------------------------------------------------
processed_stream = parsed_stream.withColumn(
    "is_anomaly",
    when(col("value") <= 0.0, True)
    .when(col("value") > 999, True)  # bahut extreme high value bhi suspicious
    .otherwise(False)
).withColumn(
    "processed_at", current_timestamp()
)

# ---------------------------------------------------------
# 6. Har micro-batch ko PostgreSQL table mein likho
# ---------------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    print(f"\n--- Batch {batch_id}: {batch_df.count()} records PostgreSQL mein likh rahe hain ---")
    batch_df.write \
        .jdbc(url=PG_URL, table="aqi_readings", mode="append", properties=PG_PROPERTIES)

query = processed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "./checkpoint") \
    .start()

print("Spark Streaming shuru ho gaya — Kafka topic 'aqi-readings' sun raha hai...")
print("Data ab PostgreSQL 'aqi_readings' table mein save ho raha hai.")
print("Rokne ke liye Ctrl+C dabao.\n")

query.awaitTermination()