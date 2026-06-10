-- Databricks Project notebook 
SHOW CATALOGS;


-- COMMAND ----------


CREATE CATALOG IF NOT EXISTS formula1
COMMENT 'This is main catalog for the Formula1 Project';



-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

use catalog formula1;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
CREATE SCHEMA IF NOT EXISTS formula1.bronze;
CREATE SCHEMA IF NOT EXISTS formula1.silver;
CREATE SCHEMA IF NOT EXISTS formula1.gold;

-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

show schemas

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS formula1.landing.raw_files
