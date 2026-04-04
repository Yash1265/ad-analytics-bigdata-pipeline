# ----------------------------------------
# IMPORT REQUIRED LIBRARIES
# ----------------------------------------

# pandas → used to create structured dataset (DataFrame)
import pandas as pd

# random → used to generate realistic synthetic values
import random

# datetime → used to generate date-based data
from datetime import datetime, timedelta


# ----------------------------------------
# STEP 1: DEFINE BASE PARAMETERS
# ----------------------------------------

# List of campaign IDs (simulating different ad campaigns)
campaigns = [101, 102, 103, 104, 105]

# Start date for dataset
start_date = datetime(2026, 1, 1)

# Empty list to store generated data
data = []


# ----------------------------------------
# STEP 2: GENERATE SYNTHETIC DATA
# ----------------------------------------

# Loop through 30 days
for i in range(30):

    # Loop through each campaign
    for campaign in campaigns:

        # Generate impressions (number of times ad was shown)
        impressions = random.randint(5000, 20000)

        # Generate clicks (user interactions)
        clicks = random.randint(100, 1000)

        # Generate spend (money spent on ads)
        spend = round(random.uniform(100, 1000), 2)

        # Generate revenue (earnings from campaign)
        revenue = round(spend * random.uniform(0.8, 1.5), 2)

        # Append row to dataset
        data.append([
            campaign,
            start_date + timedelta(days=i),
            impressions,
            clicks,
            spend,
            revenue
        ])


# ----------------------------------------
# STEP 3: INJECT DIRTY DATA (IMPORTANT)
# ----------------------------------------

# Add a duplicate row intentionally
# Purpose: to test duplicate removal logic
data.append(data[0])


# Add an outlier row intentionally
# Purpose: to test outlier detection (IQR method)
data.append([
    999,                     # Fake campaign ID (not in original list)
    start_date,              # Same date for simplicity
    1000000,                 # Extremely high impressions (OUTLIER)
    5,                       # Very low clicks
    99999,                   # Extremely high spend (OUTLIER)
    10                       # Very low revenue (poor performance)
])


# ----------------------------------------
# STEP 4: CREATE DATAFRAME
# ----------------------------------------

# Convert list into pandas DataFrame
df = pd.DataFrame(data, columns=[
    "campaign_id",   # campaign identifier
    "date",          # date of campaign
    "impressions",   # ad views
    "clicks",        # user clicks
    "spend",         # cost of campaign
    "revenue"        # revenue generated
])


# ----------------------------------------
# STEP 5: SAVE DATA TO CSV
# ----------------------------------------

# Save raw dataset to CSV file
df.to_csv("data/raw/raw_ads_data.csv", index=False)


# ----------------------------------------
# FINAL OUTPUT
# ----------------------------------------

print("Raw ad campaign data generated successfully!")
print(f"Total rows generated: {len(df)}")
