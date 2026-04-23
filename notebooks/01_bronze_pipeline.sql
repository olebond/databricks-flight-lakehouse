-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Azure + Databricks Bronze Pipeline
-- MAGIC
-- MAGIC This notebook reads raw flight data from Azure Storage and writes it into the Bronze layer.
-- MAGIC
-- MAGIC ## Azure Storage
-- MAGIC
-- MAGIC Storage account:
-- MAGIC demostorageacct
-- MAGIC
-- MAGIC Container:
-- MAGIC demo-container
-- MAGIC
-- MAGIC Folders:
-- MAGIC raw
-- MAGIC bronze
-- MAGIC silver
-- MAGIC gold
-- MAGIC
-- MAGIC Dataset used:
-- MAGIC flights.csv
-- MAGIC
-- MAGIC Raw file location:
-- MAGIC demo-container/raw/flights.csv
-- MAGIC
-- MAGIC ## Databricks / Unity Catalog
-- MAGIC
-- MAGIC Catalog:
-- MAGIC dbr_dev
-- MAGIC
-- MAGIC Schemas:
-- MAGIC dbr_dev.flight_raw
-- MAGIC dbr_dev.flight_bronze
-- MAGIC dbr_dev.flight_silver
-- MAGIC dbr_dev.flight_gold
-- MAGIC
-- MAGIC ## Access objects
-- MAGIC
-- MAGIC External Location:
-- MAGIC demostorageacct_raw
-- MAGIC
-- MAGIC Raw Volume:
-- MAGIC dbr_dev.flight_raw.raw_files
-- MAGIC
-- MAGIC ## Pipeline steps
-- MAGIC
-- MAGIC 1. Read raw CSV from Azure Storage
-- MAGIC 2. Apply a basic transformation with dropDuplicates()
-- MAGIC 3. Add metadata columns:
-- MAGIC    - source_file
-- MAGIC    - ingestion_timestamp
-- MAGIC    - load_date
-- MAGIC 4. Write Delta files into Azure bronze folder
-- MAGIC 5. Register an external Bronze table in Unity Catalog
-- MAGIC
-- MAGIC ## Idempotency
-- MAGIC
-- MAGIC The pipeline is idempotent because:
-- MAGIC - dropDuplicates() removes duplicate rows inside the dataset
-- MAGIC - mode("overwrite") rewrites the Bronze output on every run

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dbr_dev.flight_raw;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dbr_dev.flight_bronze;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dbr_dev.flight_silver;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS dbr_dev.flight_gold;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS demostorageacct_raw
URL 'abfss://demo-container@demostorageacct.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL demo_storage_credential);

-- COMMAND ----------

CREATE EXTERNAL VOLUME IF NOT EXISTS dbr_dev.flight_raw.raw_files
LOCATION 'abfss://demo-container@demostorageacct.dfs.core.windows.net/raw/';

-- COMMAND ----------

DROP VOLUME IF EXISTS dbr_dev.flight_bronze.bronze_files;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp, current_date, col

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_path = "/Volumes/dbr_dev/flight_raw/raw_files/flights.csv"
-- MAGIC bronze_output_path = "abfss://demo-container@demostorageacct.dfs.core.windows.net/bronze/flights_bronze"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_df = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("inferSchema", True)
-- MAGIC     .csv(raw_path)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bronze_df = (
-- MAGIC     raw_df
-- MAGIC     .dropDuplicates()
-- MAGIC     .withColumn("source_file", col("_metadata.file_path"))
-- MAGIC     .withColumn("ingestion_timestamp", current_timestamp())
-- MAGIC     .withColumn("load_date", current_date())
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC bronze_df.write \
-- MAGIC     .format("delta") \
-- MAGIC     .mode("overwrite") \
-- MAGIC     .save(bronze_output_path)

-- COMMAND ----------

DROP TABLE IF EXISTS dbr_dev.flight_bronze.flights_bronze_ext;

-- COMMAND ----------

CREATE TABLE dbr_dev.flight_bronze.flights_bronze_ext
USING DELTA
LOCATION 'abfss://demo-container@demostorageacct.dfs.core.windows.net/bronze/flights_bronze';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC test_df = spark.read.format("delta").load(bronze_output_path)
-- MAGIC
-- MAGIC test_df.limit(10).display()