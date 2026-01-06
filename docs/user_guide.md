# User Guide (Run the Project Locally)

This guide shows how to run the full pipeline on your machine:
**Generator → Spark Streaming → PostgreSQL**.

---

## 1) Quick prerequisites

You’ll need:

- **Python 3.10+**
- **PostgreSQL** running locally
- **Apache Spark** (so `spark-submit` works)
- **Java** (Spark needs it)

Quick checks:

```bash
python3 --version
psql --version
spark-submit --version
java -version

```

---

## 2) Python environment setup

From the project root:

```bash
python3 -m venv .venv
source .venv/bin/activate

# install dependencies if you have a requirements.txt
pip install -r requirements.txt

```

If you don’t have `requirements.txt`, you can still run the generator if it only uses the Python standard library.

---

## 3) PostgreSQL setup

### A) Create DB + user (bootstrap)

Run:

```bash
psql -f sql/bootstrap.sql

```

### B) Create table(s)

Run:

```bash
psql -d realtime_ecommerce -f sql/postgres_setup.sql

```

Quick check:

```bash
psql -d realtime_ecommerce -c "\dt"
psql -d realtime_ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"

```

---

## 4) JDBC “one-row” smoke test

This confirms Spark can connect and write to Postgres.

Run:

```bash
spark-submit \
  --jars /Users/isaacamfoh/.ivy2.5.2/cache/org.postgresql/postgresql/jars/postgresql-42.7.4.jar \
  src/jdbc_write_test.py

```

Verify:

```bash
psql -d realtime_ecommerce -c "SELECT event_id, action, product_id, source FROM ecommerce_events ORDER BY stored_at DESC LIMIT 5;"

```

Expected:

- You should see the test row in the results (or a duplicate key warning if you ran it before).

---

## 5) Reset the pipeline (recommended before a clean run)

Stop any running generator or Spark job (`Ctrl + C`), then:

```bash
bash scripts/reset_pipeline.sh

```

Optional: clear old CSVs for a totally fresh run:

```bash
rm -f data/incoming/*.csv

```

---

## 6) Start the generator (Terminal A)

```bash
source .venv/bin/activate
python src/data_generator.py

```

Confirm files are appearing:

```bash
ls -lt data/incoming | head

```

---

## 7) Start Spark streaming to Postgres (Terminal B)

```bash
bash scripts/run_stream_to_postgres.sh

```

Expected:

- You’ll see logs like:  
  `✅ batch 0: 196 rows | 0.88s | 223.5 rows/s`

---

## 8) Verify ingestion in Postgres (Terminal C)

Run this a few times:

```bash
psql -d realtime_ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"

```

Spot-check:

```bash
psql -d realtime_ecommerce -c "SELECT action, COUNT(*) FROM ecommerce_events GROUP BY action;"
psql -d realtime_ecommerce -c "SELECT event_id, action, product_id, source, stored_at FROM ecommerce_events ORDER BY stored_at DESC LIMIT 5;"

```

Expected:

- Row count increases over time while generator + Spark are running.

---

## 9) Common issues (quick fixes)

### Duplicate key error (event_id already exists)

This usually happens if old data is still in Postgres and Spark replays old files.  
Fix:

```bash
bash scripts/reset_pipeline.sh
rm -f data/incoming/*.csv

```

### Spark “falling behind” warning

This can happen if you’re writing a lot of rows to Postgres quickly.  
Fix:

- Increase trigger interval (e.g., 10 seconds) in your run script.

---

## 10) How to stop everything

- Generator terminal: `Ctrl + C`
- Spark streaming terminal: `Ctrl + C`
