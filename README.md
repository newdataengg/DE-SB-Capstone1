In this capstone, we will create a data pipeline for high-frequency financial data, which are collected and updated throughout each day, every day.

# Overview #

Spring Capital is an investment bank who owe their success to Big Data analytics. They make
critical decisions about investments based on high-frequency trading data analysis.
High-frequency financial data relate to important financial market events, like the price of a
stock, that are collected many times throughout each day.

## Objective ##
The goal of this project is to build an end-to-end data pipeline to ingest and process daily stock
market data from multiple stock exchanges. The pipeline should maintain the source data in a
structured format, organized by date. It also needs to produce analytical results that support
business analysis.

### Data Source ###
The source data used in this project is randomly generated stock exchange data.
● Trades: records that indicate transactions of stock shares between broker-dealers.
● Quotes: records of updates best bid/ask price for a stock symbol on a certain exchange.


# Prerequisites
- Python: basics, string manipulation, control flow, exception handling, JSON parsing
- PySpark: RDD from text file, custom DataFrames, write with partitions, Parquet
- Azure Databricks setup
- Azure Blob Storage setup

## Step 1: Data Source
- First, we upload data from local folder to Blob Storage using upload_files.sh
   check logs folder

## Step 2: Create Azure Databricks cluster

Cluster - springboard-de-ws

## Step 3: Run the data_ingestion.py
   tokens are removed from the script


## Step 4: Check the output dataframe




## Step 5: Check the output directory


