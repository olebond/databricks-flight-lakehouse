# Architecture Notes

## Data Flow

```text
ADLS Gen2 raw folder
  flights.csv
      |
      | batch read
      v
Bronze Delta table
  raw records
  source_file
  ingestion_timestamp
  load_date
      |
      | validation and standardization
      v
Silver Delta table
  clean flight records
  deduplicated business keys
  merge/upsert logic
      |
      | historical tracking design
      v
SCD Type 2 table
```

## Layers

Bronze:
- Stores raw source records in Delta format.
- Adds ingestion metadata for traceability.
- Uses overwrite mode for lab idempotency.

Silver:
- Standardizes column names.
- Creates a proper `flight_date`.
- Applies null checks and range validation.
- Deduplicates records using business keys and window functions.
- Uses Delta Lake `MERGE` for upsert behavior.

Streaming:
- Uses Auto Loader for file-based streaming ingestion.
- Uses checkpoint and schema locations for repeatable processing.
- Includes prepared Event Hub producer/consumer logic for event-based ingestion.

## Portfolio Notes

This project is intended to demonstrate learning and hands-on practice with Databricks lakehouse concepts. It is not presented as a production deployment.
