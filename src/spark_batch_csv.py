from pyspark.sql import SparkSession
import logging


logging.basicConfig(
    filename="logs/data_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting Spark batch job (CSV source)")


spark = SparkSession.builder.appName("AdBatchCSV").getOrCreate()

df = spark.read.csv(
    "data/processed/clean_ads_data.csv",
    header=True,
    inferSchema=True
)

logging.info("CSV data loaded into Spark")

df.groupBy("campaign_id").sum().show()

spark.stop()

logging.info("Spark CSV batch job completed")
