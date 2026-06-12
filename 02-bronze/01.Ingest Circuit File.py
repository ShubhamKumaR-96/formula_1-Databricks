# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file
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
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catelog_name}.{bronze_schema}.circuits"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,DoubleType

circuits_schema = StructType([
      StructField("circuitId",StringType()),
      StructField("url",StringType()),
      StructField("circuitName",StringType()),
      StructField("lat",DoubleType()),
      StructField("long",DoubleType()),
      StructField("locality",StringType()),
      StructField("country",StringType())
])

# COMMAND ----------

circuits_df =( spark.read.format("csv")
                .option("header",True)
                .schema(circuits_schema)
                .option("mode","PERMISSIVE").load(source_file)
)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add Metadata Columns
# MAGIC    - Source File
# MAGIC    - Ingestion Timestamp

# COMMAND ----------

from pyspark.sql import functions as F

cirucits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(cirucits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - write to bronze delta table

# COMMAND ----------

(
    cirucits_final_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from formula1.bronze.circuits;

# COMMAND ----------

display(spark.table(table_name))