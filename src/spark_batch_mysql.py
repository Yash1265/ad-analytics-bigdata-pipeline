# ----------------------------------------
# IMPORT LIBRARIES
# ----------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import logging


# ----------------------------------------
# CONFIGURE LOGGING
# ----------------------------------------

logging.basicConfig(
    filename="logs/data_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting Spark batch job (MySQL source)")


# ----------------------------------------
# CREATE SPARK SESSION
# ----------------------------------------

spark = SparkSession.builder \
    .appName("AdAnalyticsMySQL") \
    .config("spark.jars", "/home/penitent_one/mysql-connector-j-8.0.33/mysql-connector-j-8.0.33.jar") \
    .getOrCreate()

logging.info("Spark session created")


# ----------------------------------------
# READ DATA FROM MYSQL
# ----------------------------------------

try:
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3306/ad_analytics") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "campaign_data") \
        .option("user", "root") \
        .option("password", "YOUR_PASSWORD") \
        .load()

    logging.info("Data successfully read from MySQL")
except Exception as e:
    logging.error(f"Error reading from MySQL: {e}")
    raise


# ----------------------------------------
# AGGREGATE DATA (CAMPAIGN LEVEL)
# ----------------------------------------

logging.info("Performing aggregation")

agg_df = df.groupBy("campaign_id").agg(
    sum("impressions").alias("total_impressions"),
    sum("clicks").alias("total_clicks"),
    sum("spend").alias("total_spend"),
    sum("revenue").alias("total_revenue")
)


# ----------------------------------------
# FEATURE ENGINEERING IN SPARK
# ----------------------------------------

logging.info("Calculating metrics (CTR, CPC, ROI)")

metrics_df = agg_df.withColumn(
    "CTR", col("total_clicks") / col("total_impressions")
).withColumn(
    "CPC", col("total_spend") / col("total_clicks")
).withColumn(
    "ROI", (col("total_revenue") - col("total_spend")) / col("total_spend")
)


# ----------------------------------------
# DISPLAY RESULT
# ----------------------------------------

metrics_df.show()


# ----------------------------------------
# SAVE OUTPUT (PARQUET FORMAT)
# ----------------------------------------

try:
    metrics_df.write.mode("overwrite").parquet("hdfs/campaign_metrics")
    logging.info("Output saved as Parquet successfully")
except Exception as e:
    logging.error(f"Error saving output: {e}")
    raise


# ----------------------------------------
# STOP SPARK
# ----------------------------------------

spark.stop()
logging.info("Spark job completed successfully")

print("Spark batch processing completed!")
