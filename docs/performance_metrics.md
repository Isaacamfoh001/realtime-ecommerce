# Performance Metrics

This report captures simple, practical performance observations from running the pipeline locally.
We measured performance using Spark’s per-batch logs:

- **Rows per batch**
- **Batch duration (seconds)**
- **Throughput (rows/sec)**

The pipeline writes to PostgreSQL via JDBC using `foreachBatch`.

---

## Run A — Trigger: 5 seconds (backlog / catch-up batch)

In this run, Spark started while there were already many CSV files sitting in `data/incoming/`, so the first batch processed a large backlog.

### Key observation

- Spark warned that it was “falling behind” because the first batch took longer than the 5s trigger interval.

### Sample results (from logs)

- **Batch 0:** `87694 rows | 9.19s | 9539.7 rows/s`
- Warning observed:
  - Trigger interval was **5000 ms**
  - Batch cycle time exceeded the trigger (warning printed)

### Interpretation

- The first batch was effectively a “catch-up” batch (processing everything already in the folder).
- JDBC writes to Postgres are heavier than writing to files, so it’s normal for a large initial batch to push beyond a short trigger.

---

## Run B — Trigger: 10 seconds (clean steady-state)

In this run, the pipeline was restarted cleanly:

- trigger interval increased to **10 seconds**
- pipeline reset was run
- old CSVs were cleared before observing steady-state

### Sample results (from logs)

- **Batch 0:** `196 rows | 0.88s | 223.5 rows/s`
- **Batch 1:** `74 rows | 0.20s | 375.6 rows/s`
- **Batch 2:** `124 rows | 0.25s | 487.5 rows/s`
- **Batch 3:** `122 rows | 0.20s | 621.4 rows/s`
- **Batch 4:** `125 rows | 0.19s | 664.5 rows/s`
- **Batch 5:** `123 rows | 0.19s | 663.7 rows/s`
- **Batch 6:** `125 rows | 0.26s | 477.1 rows/s`
- **Batch 7:** `123 rows | 0.18s | 688.0 rows/s`
- **Batch 8:** `125 rows | 0.21s | 588.5 rows/s`
- **Batch 9:** `124 rows | 0.15s | 802.6 rows/s`
- **Batch 10:** `125 rows | 0.22s | 571.2 rows/s`
- **Batch 11:** `124 rows | 0.18s | 687.5 rows/s`
- **Batch 12:** `124 rows | 0.20s | 621.0 rows/s`
- **Batch 13:** `123 rows | 0.19s | 646.3 rows/s`

### Steady-state summary

From the sample batches:

- Typical batch size: **~120–125 rows**
- Typical batch time: **~0.15–0.26s**
- Typical throughput: **~475–800 rows/sec** (varies with batch size)

### Interpretation

- With a 10s trigger and no backlog, the pipeline ran comfortably without “falling behind”.
- Batch durations were consistently well below the trigger interval, meaning the system had time to spare.

---

## Takeaways

- When Spark starts with many files already waiting, the first batch can be huge and may exceed the trigger interval.
- For a JDBC sink (Postgres), a slightly larger trigger interval (like 10 seconds) makes the system more stable and easier to observe.
- Once running in steady-state, the pipeline handles continuous arrivals efficiently and writes to Postgres consistently.
