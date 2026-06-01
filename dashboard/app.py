# ============================================================
# PROJECT:
# Ad Analytics Big Data Dashboard
#
# PURPOSE:
# This dashboard is the visualization layer of our
# Data Engineering pipeline.
#
# Pipeline Flow:
# Data Generator
#       ↓
# Data Cleaning
#       ↓
# MySQL Storage
#       ↓
# Streamlit Dashboard
#
# Technologies:
# - Streamlit : Web dashboard framework
# - Pandas    : Data manipulation
# - Plotly    : Interactive charts
# - SQLAlchemy: MySQL database connection
# ============================================================


# -----------------------------
# IMPORT REQUIRED LIBRARIES
# -----------------------------

# Streamlit is used to create the web application/dashboard
import streamlit as st

# Pandas is used for handling tabular data using DataFrames
import pandas as pd

# Plotly Express is used for creating interactive graphs
import plotly.express as px

# SQLAlchemy creates connection between Python application and MySQL
from sqlalchemy import create_engine



# ============================================================
# DASHBOARD PAGE CONFIGURATION
# ============================================================

# Configure browser tab name and dashboard layout
# wide layout uses full screen width
st.set_page_config(
    page_title="Ad Analytics Dashboard",
    layout="wide"
)


# Dashboard main heading
st.title(
    "🚀 Ad Analytics Big Data Dashboard"
)


# Dashboard description
# Shows technologies used in backend pipeline
st.write(
    """
    Batch + Real-Time Analytics Pipeline
    
    Tech Stack:
    PySpark | Kafka | MySQL | Streamlit
    """
)



# ============================================================
# MYSQL DATABASE CONNECTION
# ============================================================


# create_engine() creates database connection object
#
# Format:
# mysql+pymysql://username:password@host/database
#
# This connects dashboard directly with processed data
# stored in MySQL after ETL pipeline execution

engine = create_engine(
    "mysql+pymysql://root:admin123@localhost/ad_analytics"
)



# ============================================================
# LOAD DATA FROM MYSQL
# ============================================================


# @st.cache_data stores the query result temporarily
#
# Benefit:
# Without cache:
# Every refresh → Query MySQL again
#
# With cache:
# Load once → reuse data → faster dashboard


@st.cache_data
def load_data():


    # SQL query to fetch processed campaign data
    query = """

    SELECT *
    FROM campaign_data

    """


    # read_sql()
    #
    # Executes SQL query
    # Converts database table into Pandas DataFrame

    df = pd.read_sql(
        query,
        engine
    )


    # Return dataframe to dashboard
    return df



# Calling function and storing data
# df now contains complete campaign dataset

df = load_data()



# ============================================================
# KPI METRICS SECTION
# ============================================================

# KPIs provide quick business insights


# Calculate total advertisement impressions

total_impressions = df["impressions"].sum()



# Calculate total user clicks

total_clicks = df["clicks"].sum()



# Calculate average ROI
#
# ROI formula already created during cleaning:
#
# ROI = (Revenue - Spend) / Spend

avg_roi = round(
    df["ROI"].mean(),
    2
)



# Create three columns horizontally
# Example:
#
# Impressions | Clicks | ROI

col1, col2, col3 = st.columns(3)



# Display total impressions KPI card

col1.metric(
    "Total Impressions",
    f"{total_impressions:,}"
)



# Display total clicks KPI card

col2.metric(
    "Total Clicks",
    f"{total_clicks:,}"
)



# Display ROI KPI card

col3.metric(
    "Average ROI",
    avg_roi
)



# ============================================================
# CAMPAIGN LEVEL AGGREGATION
# ============================================================


# groupBy campaign_id
#
# Similar Spark logic:
#
# df.groupBy("campaign_id").agg()
#
# Here pandas is aggregating for visualization


campaign = df.groupby(
    "campaign_id"
).agg(
    {

        # Total clicks per campaign
        "clicks": "sum",

        # Total money spent
        "spend": "sum",

        # Total revenue generated
        "revenue": "sum"

    }
).reset_index()



# ============================================================
# BAR CHART - CAMPAIGN CLICK PERFORMANCE
# ============================================================


# Creating interactive bar chart using Plotly

fig1 = px.bar(

    campaign,

    # X-axis = Campaign
    x="campaign_id",

    # Y-axis = Total clicks
    y="clicks",

    title="Campaign Click Performance"
)



# Display chart in Streamlit dashboard

st.plotly_chart(

    fig1,

    # Chart adjusts according to screen size
    use_container_width=True
)



# ============================================================
# LINE CHART - SPEND VS REVENUE
# ============================================================


# Compare money spent vs revenue generated

fig2 = px.line(

    campaign,


    # Campaign IDs
    x="campaign_id",


    # Multiple lines:
    # spend line
    # revenue line

    y=[
        "spend",
        "revenue"
    ],


    title="Spend vs Revenue"
)



# Display spend vs revenue chart

st.plotly_chart(

    fig2,

    use_container_width=True
)



# ============================================================
# RAW PROCESSED DATA VIEW
# ============================================================


# Section heading

st.subheader(
    "Processed Campaign Data"
)



# Display dataframe in table format
#
# Allows:
# sorting
# scrolling
# viewing processed records

st.dataframe(
    df
)



# ============================================================
# END OF DASHBOARD APPLICATION
# ============================================================
