
# Real-Time E-Commerce Lab (Spark Structured Streaming → PostgreSQL)

This project simulates an e-commerce app that produces user events (like **view** and **purchase**), then streams those events in near real-time using **Spark Structured Streaming**, and stores the cleaned results in **PostgreSQL**.

---

## What’s inside (high level)
- **Python generator** creates CSV event files into `data/incoming/`
- **Spark streaming job** watches that folder, cleans/casts the data, filters bad rows, then writes to Postgres in micro-batches
- **PostgreSQL** stores the final events in `ecommerce_events`

Architecture diagram: `system_architecture.png`

---

## Prerequisites
You need:
- Python 3.10+
- PostgreSQL
- Apache Spark (`spark-submit`)
- Java (Spark needs it)

Quick check:
```bash
python3 --version
psql --version
spark-submit --version
java -version

```

----------

## Setup (Postgres)

Create DB/user (bootstrap):

```bash
psql -f sql/bootstrap.sql

```

Create tables:

```bash
psql -d realtime_ecommerce -f sql/postgres_setup.sql

```

----------

## Run it (3 terminals)

### Terminal A — Start the generator

```bash
source .venv/bin/activate
python src/data_generator.py

```

### Terminal B — Start Spark streaming to Postgres

```bash
bash scripts/run_stream_to_postgres.sh

```

You should see batch logs like:  
`✅ batch 3: 124 rows | 0.20s | 621.0 rows/s`

### Terminal C — Verify rows in Postgres

```bash
psql -d realtime_ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"
psql -d realtime_ecommerce -c "SELECT action, COUNT(*) FROM ecommerce_events GROUP BY action;"

```

----------

## Reset (clean rerun)

Stop Spark + generator (`Ctrl + C`), then:

```bash
bash scripts/reset_pipeline.sh
rm -f data/incoming/*.csv

```

----------

## Deliverables (where to find them)

All write-ups are in `docs/`:

-   `docs/project_overview.md`
    
-   `docs/user_guide.md`
    
-   `docs/test_cases.md`
    
-   `docs/performance_metrics.md`
    

Other key files:

-   `system_architecture.png`
    
-   `postgres_connection_details.example.txt`
    
