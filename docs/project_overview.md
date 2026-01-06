# Real-Time E-Commerce Data Ingestion (Spark Streaming → PostgreSQL)

## What this project is

This project simulates an e-commerce platform that produces user activity events (like **view** and **purchase**), then ingests those events in near real time using **Spark Structured Streaming**, and stores them in **PostgreSQL** for querying and verification.

## Main components

### 1) Data Generator (Python)

A Python script continuously writes new CSV files into `data/incoming/`.

Each file contains rows representing e-commerce events.

### 2) Spark Streaming Job (PySpark)

Spark watches `data/incoming/` for new CSV files.

When new files appear, Spark:

- reads them as a stream (micro-batches)

- cleans and safely casts fields using `try_cast`

- drops invalid rows (bad timestamps, missing IDs, wrong actions)

- writes valid rows into PostgreSQL using `foreachBatch` + JDBC

### 3) PostgreSQL (Database)

Postgres stores the final cleaned events in a table called `ecommerce_events`.

You can run SQL queries to confirm ingestion is working and inspect the data.

## Data flow (high-level)

1.  `data_generator.py` creates new CSV files → `data/incoming/`

2.  Spark reads those files as a stream

3.  Spark transforms + validates the rows

4.  Spark writes micro-batches into Postgres (`ecommerce_events`)

5.  We verify by querying Postgres

## Project files (what each one does)

- `src/data_generator.py`

Generates fake e-commerce activity and writes CSV files into `data/incoming/`.

- `src/spark_streaming_to_postgres.py`

Watches for new CSVs, cleans/casts the data, filters bad rows, and writes valid rows into Postgres.

- `src/jdbc_write_test.py`

Writes a single test row into Postgres to confirm JDBC connectivity is working.

- `sql/bootstrap.sql`

Creates the database and user (kept separate because some statements must run outside transactions).

- `sql/postgres_setup.sql`

Creates the `ecommerce_events` table and any helpful indexes.

- `scripts/reset_pipeline.sh`

Resets the pipeline by truncating the Postgres table and clearing the Spark checkpoint.

- `scripts/run_stream_to_postgres.sh`

Runs the Spark streaming job with the Postgres JDBC jar included.

## What “success” looks like

- CSV files keep appearing in `data/incoming/`

- Spark prints batch logs like: `✅ batch 3: 124 rows | 0.20s | 621.0 rows/s`

- Postgres row count increases over time:

`SELECT COUNT(*) FROM ecommerce_events;`
