# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file
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
source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catelog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# Define the schema
constructor_schema = "constructorId STRING, name STRING, nationality STRING, url string"

# COMMAND ----------

constructors_df = (
     spark.read.format("json")
     .option("header",True)
     .schema(constructor_schema)
     .option("mode","FAILFAST")
     .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add Metadata Columns
# MAGIC    - Source File
# MAGIC    - Ingestion Timestamp

# COMMAND ----------

constructor_final_df = add_ingestion_metadata(constructors_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - write to bronze delta table

# COMMAND ----------

(constructor_final_df.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(table_name)
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.bronze.constructors