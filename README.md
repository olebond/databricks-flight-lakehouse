# Databricks Flight Lakehouse

Training lakehouse project for ingesting and processing flight data with Azure Data Lake Storage, Databricks, Unity Catalog, PySpark, and Delta Lake.

The project demonstrates a practical Bronze/Silver data pipeline, plus file-based streaming ingestion with Auto Loader.

## What This Project Shows

- Raw CSV ingestion from Azure Data Lake Storage
- Databricks notebooks with PySpark and SQL
- Unity Catalog schemas, external locations, and volumes
- Bronze Delta table creation with ingestion metadata
- Silver layer transformations, validation, and deduplication
- Delta Lake `MERGE` logic for upserts
- SCD Type 2 table structure preparation
- File-based streaming with Auto Loader and Structured Streaming
- Event streaming design with Azure Event Hub

## Architecture

```text
Azure Data Lake Storage
  raw/flights.csv
        |
        v
Databricks Bronze Layer
  raw data + ingestion metadata
        |
        v
Databricks Silver Layer
  standardized, validated, deduplicated flight records
        |
        v
Delta Lake Tables
  merge/upsert logic and SCD Type 2 structure
```

## Project Structure

```text
docs/
  architecture.md
notebooks/
  01_bronze_ingestion.py
  01_bronze_pipeline.sql
  02_silver_pipeline.py
  03_streaming_ingestion.sql
```

## Notebook Overview

| Notebook | Purpose |
| --- | --- |
| `01_bronze_ingestion.py` | Reads raw CSV data and writes a Bronze Delta table. |
| `01_bronze_pipeline.sql` | Creates Unity Catalog objects and registers an external Bronze table. |
| `02_silver_pipeline.py` | Builds Silver transformations, validation, deduplication, Delta `MERGE`, and SCD2 preparation. |
| `03_streaming_ingestion.sql` | Demonstrates Auto Loader file streaming and prepared Event Hub streaming design. |

## How To Use

1. Import the notebooks into a Databricks workspace.
2. Create or update the Unity Catalog objects for your own catalog, schemas, storage credential, and external location.
3. Replace the placeholder Azure paths with your own ADLS Gen2 account and container.
4. Run the Bronze notebook first, then the Silver notebook, then the streaming notebook.

## Interview Summary

This project demonstrates a Junior Data Engineer learning path around a modern lakehouse stack: Azure storage, Databricks, Delta Lake, governed access with Unity Catalog, and basic batch/streaming ingestion patterns.

## Notes

- Notebooks were developed as hands-on Databricks learning labs.
- Azure resource names were generalized for public portfolio use.
- The Event Hub section contains prepared producer/consumer logic and should be described as design-ready unless executed with a real Event Hub.
