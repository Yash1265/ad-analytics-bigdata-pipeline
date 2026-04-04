# Ad Analytics Big Data Pipeline 🚀

## Overview
End-to-end data pipeline for ad campaign analytics using batch and real-time processing.

## Tech Stack
- Python
- Pandas
- MySQL
- PySpark
- Kafka
- Spark Streaming

## Features
- Data Generation
- Data Cleaning (with outlier removal)
- Batch Processing (Spark)
- Real-time Processing (Kafka + Spark Streaming)
- HDFS Simulation (Parquet)
- Logging & Monitoring

## Architecture
Producer → Kafka → Spark Streaming → HDFS  
CSV → Spark → MySQL → Analytics

## How to Run
```bash
python src/data_generator.py
python src/data_cleaner.py
python src/load_to_mysql.py
python src/spark_batch_mysql.py
