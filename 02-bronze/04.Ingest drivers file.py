# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers.json file
# MAGIC 1. Read the json file using the spark dataframe reader API
# MAGIC 1. Add metadata columns
# MAGIC   - Source file
# MAGIC   - Ingestion date
# MAGIC 1. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.Environment-Config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# Define source_file and table_name
source_file = f"{landing_folder_path}/drivers.json"
table_name = f"{catelog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType,StructField,DateType,StringType

name_schema = StructType([
    StructField("givenName",StringType()),
    StructField("familyName",StringType())
])

drivers_schema = StructType([
    StructField("driverId",StringType()),
    StructField("name",name_schema),
    StructField("dateOfBirth",DateType()),
    StructField("nationality",StringType()),
    StructField("url",StringType())
])

# COMMAND ----------

driver_df = (
     spark.read.format("json")
     .option("header",True)
     .schema(drivers_schema)
     .option("mode","FAILFAST")
     .load(source_file)
)

# COMMAND ----------

display(driver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add Metadata Columns
# MAGIC    - Source File
# MAGIC    - Ingestion Timestamp

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(driver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - write to bronze delta table

# COMMAND ----------

(drivers_final_df.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(table_name)
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.drivers;