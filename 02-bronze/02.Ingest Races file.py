# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file
# MAGIC 1. Read the csv file using the spark dataframe reader API
# MAGIC 1. Add metadata columns
# MAGIC   - Source file
# MAGIC   - Ingestion date
# MAGIC 1. Write to bronze delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1 Read the CSV file using the dataframe reader API

# COMMAND ----------

# MAGIC %run ../00-common/01.Environment-Config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name = f"{catelog_name}.{bronze_schema}.races"


# COMMAND ----------

from pyspark.sql import types as T

races_schema = T.StructType([
    T.StructField("season",T.IntegerType()),
    T.StructField("round",T.IntegerType()),
    T.StructField("url",T.StringType()),
    T.StructField("raceName",T.StringType()),
    T.StructField("date",T.DateType()),
    T.StructField("circuitId",T.StringType())
])

# COMMAND ----------

races_df = (spark.read.format("csv")
            .option("header",True)
            .schema(races_schema)
            .option("mode","FAILFAST")
            .load(source_file))

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add Metadata Columns
# MAGIC    - Source File
# MAGIC    - Ingestion Timestamp

# COMMAND ----------

from pyspark.sql import functions as F
races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - write to bronze delta table

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from formula1.bronze.races