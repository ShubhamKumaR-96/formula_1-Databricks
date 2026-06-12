# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Sprints.json file
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
source_file = f"{landing_folder_path}/sprints/"
table_name = f"{catelog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType,StructField,DateType,StringType,IntegerType,FloatType

sprints_schema = StructType([
    StructField("date",DateType()),
    StructField("raceName",StringType()),
    StructField("round",IntegerType()),
    StructField("season",IntegerType()),
    StructField("url",StringType()),
    StructField("constructorId",StringType()),
    StructField("driverId",StringType()),
    StructField("grid",IntegerType()),
    StructField("laps",IntegerType()),
    StructField("number",IntegerType()),
    StructField("points",FloatType()),
    StructField("position",IntegerType()),
    StructField("positionText",StringType()),
    StructField("status",StringType()),
])


# COMMAND ----------

sprints_df = (
     spark.read.format("json")
     .option("header",True)
     .schema(results_schema)
     .option("mode","FAILFAST")
     .option("multiLine",True)
     .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Add Metadata Columns
# MAGIC    - Source File
# MAGIC    - Ingestion Timestamp

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Step 3 - write to bronze delta table

# COMMAND ----------

(sprints_final_df.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(table_name)
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select season , count(*) from formula1.bronze.sprints
# MAGIC group by season 
# MAGIC order by season