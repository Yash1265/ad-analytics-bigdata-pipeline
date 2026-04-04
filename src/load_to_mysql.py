# ----------------------------------------
# IMPORT LIBRARIES
# ----------------------------------------

import pandas as pd
import logging
from sqlalchemy import create_engine


# ----------------------------------------
# CONFIGURE LOGGING
# ----------------------------------------

logging.basicConfig(
    filename="logs/data_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting data load to MySQL")


# ----------------------------------------
# LOAD CLEAN DATA
# ----------------------------------------

try:
    df = pd.read_csv("data/processed/clean_ads_data.csv")
    logging.info(f"Clean data loaded successfully. Rows: {len(df)}")
except Exception as e:
    logging.error(f"Error loading clean data: {e}")
    raise


# ----------------------------------------
# CONNECT TO MYSQL
# ----------------------------------------

try:
    # Replace YOUR_PASSWORD with actual password
    engine = create_engine("mysql+pymysql://root:admin123@localhost/ad_analytics")
    logging.info("MySQL connection established")
except Exception as e:
    logging.error(f"MySQL connection failed: {e}")
    raise


# ----------------------------------------
# LOAD DATA INTO MYSQL
# ----------------------------------------

try:
    df.to_sql("campaign_data", con=engine, if_exists="replace", index=False)
    logging.info("Data successfully loaded into MySQL table")
except Exception as e:
    logging.error(f"Error inserting data into MySQL: {e}")
    raise


print("Data successfully loaded into MySQL!")
logging.info("MySQL load process completed")
