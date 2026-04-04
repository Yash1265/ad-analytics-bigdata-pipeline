import pandas as pd
import logging


# ----------------------------------------
# LOGGING CONFIGURATION
# ----------------------------------------

logging.basicConfig(
    filename="logs/data_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting data cleaning pipeline")


# ----------------------------------------
# LOAD DATA
# ----------------------------------------

try:
    df = pd.read_csv("data/raw/raw_ads_data.csv")
    logging.info(f"Raw data loaded. Rows: {len(df)}")
except Exception as e:
    logging.error(f"Error loading data: {e}")
    raise


# ----------------------------------------
# REMOVE MISSING VALUES
# ----------------------------------------

missing_before = df.isnull().sum().sum()
df = df.dropna()
logging.info(f"Missing values removed: {missing_before}")


# ----------------------------------------
# REMOVE DUPLICATES
# ----------------------------------------

before_dup = len(df)
df = df.drop_duplicates()
after_dup = len(df)

logging.info(f"Duplicates removed: {before_dup - after_dup}")


# ----------------------------------------
# LOGICAL VALIDATION
# ----------------------------------------

df = df[df["clicks"] <= df["impressions"]]
df = df[df["spend"] >= 0]
df = df[df["revenue"] >= 0]

logging.info("Logical validation applied")


# ----------------------------------------
# OUTLIER REMOVAL (IQR METHOD)
# ----------------------------------------

def remove_outliers(column):

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)

    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    return df[(df[column] >= lower) & (df[column] <= upper)]


rows_before = len(df)

for col in ["impressions", "clicks", "spend", "revenue"]:
    df = remove_outliers(col)

rows_after = len(df)

logging.info(f"Outliers removed: {rows_before - rows_after}")


# ----------------------------------------
# FEATURE ENGINEERING
# ----------------------------------------

df["CTR"] = df["clicks"] / df["impressions"]
df["CPC"] = df["spend"] / df["clicks"]
df["ROI"] = (df["revenue"] - df["spend"]) / df["spend"]

logging.info("Feature engineering completed")


# ----------------------------------------
# SAVE CLEAN DATA
# ----------------------------------------

df.to_csv("data/processed/clean_ads_data.csv", index=False)

logging.info(f"Final dataset rows: {len(df)}")
logging.info("Data cleaning pipeline completed successfully")


print("Full data cleaning (including duplicates & outliers) completed!")
