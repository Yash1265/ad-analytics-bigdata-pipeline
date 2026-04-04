# ----------------------------------------
# IMPORT LIBRARIES
# ----------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, FloatType
import logging


# ----------------------------------------
# CONFIGURE LOGGING
# ----------------------------------------

logging.basicConfig(
    filename="logs/data_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting Spark Kafka Streaming job")


# ----------------------------------------
# CREATE SPARK SESSION
# ----------------------------------------

spark = SparkSession.builder \
    .appName("KafkaStreaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

logging.info("Spark session created successfully")


# ----------------------------------------
# DEFINE SCHEMA FOR KAFKA DATA
# ----------------------------------------

# This schema must match the structure of data sent by Kafka producer
schema = StructType() \
    .add("campaign_id", IntegerType()) \
    .add("clicks", IntegerType()) \
    .add("spend", FloatType())

logging.info("Schema defined for incoming Kafka data")


# ----------------------------------------
# READ STREAMING DATA FROM KAFKA
# ----------------------------------------

# Kafka sends data in binary format (key, value)
# We subscribe to topic 'ad-events'

try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ad-events") \
        .load()

    logging.info("Connected to Kafka topic 'ad-events'")
except Exception as e:
    logging.error(f"Error connecting to Kafka: {e}")
    raise


# ----------------------------------------
# CONVERT RAW KAFKA DATA → STRUCTURED DATA
# ----------------------------------------

# Kafka 'value' is in bytes → convert to string → parse JSON → extract fields

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

logging.info("Kafka data parsed into structured format")


# ----------------------------------------
# AGGREGATION (REAL-TIME)
# ----------------------------------------

# Count number of events per campaign_id

agg_df = parsed_df.groupBy("campaign_id").count()

logging.info("Aggregation applied on streaming data")


# ----------------------------------------
# OUTPUT STREAM TO CONSOLE
# ----------------------------------------

# outputMode = complete → shows full aggregated result every batch

query = agg_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

logging.info("Streaming query started successfully")


# ----------------------------------------
# KEEP STREAM RUNNING
# ----------------------------------------

# This keeps the streaming job active

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Streaming stopped manually")
