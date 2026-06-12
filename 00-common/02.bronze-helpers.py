# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

def add_ingestion_metadata(df):
    return df.withColumns({
        "ingestion_date": current_timestamp(),
        "source_file": col("_metadata.file_path")
    })

# COMMAND ----------

