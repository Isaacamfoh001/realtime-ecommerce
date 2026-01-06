## Manual Test Cases (Completed)

This document shows the manual checks we ran to confirm the pipeline works end-to-end:
**Generator → Spark Structured Streaming → PostgreSQL**.

---

## Test Case 1 — Generator creates CSV files

**Goal:** Confirm the generator produces CSV files in the incoming folder.

How to run:

```bash
source .venv/bin/activate
python src/data_generator.py

```

Verification:

```bash
ls -lt data/incoming | head

```

Expected:

- New `.csv` files appear in `data/incoming/` over time.
- Files are non-empty and include headers.

Actual:

- Generator produced files successfully (Spark ingestion below confirms new files were being created and processed).

Result:

- ✅ Pass

---

## Test Case 2 — Spark detects and processes new files

**Goal:** Confirm Spark Structured Streaming picks up newly created CSV files.

How to run:

```bash
bash scripts/run_stream_to_postgres.sh

```

Expected:

- Spark prints batch logs like `✅ batch X: ... rows ...`
- Stream stays running (no crash).

Actual:

- Spark streamed and wrote multiple batches successfully (batch logs observed in terminal).

Result:

- ✅ Pass

---

## Test Case 3 — Data lands in PostgreSQL

**Goal:** Confirm rows are being inserted into Postgres.

Query:

```bash
psql -d realtime_ecommerce -c "SELECT COUNT(*) FROM ecommerce_events;"

```

Expected:

- Count is > 0 and increases while generator + Spark are running.

Actual:

- Count returned:

  - `2228`

Result:

- ✅ Pass

---

## Test Case 4 — Actions look correct (views vs purchases)

**Goal:** Confirm event actions are stored and grouped correctly.

Query:

```bash
psql -d realtime_ecommerce -c "SELECT action, COUNT(*) FROM ecommerce_events GROUP BY action;"

```

Expected:

- Only valid actions (mainly `view` and `purchase`) are present.

Actual:

- Results:

  - `view`: 1788
  - `purchase`: 440

Result:

- ✅ Pass

---

## Test Case 5 — Spot-check latest inserted rows

**Goal:** Confirm the latest rows look realistic (event_id present, action present, product_id present, stored_at recent).

Query:

```bash
psql -d realtime_ecommerce -c "SELECT event_id, action, product_id, source, stored_at FROM ecommerce_events ORDER BY stored_at DESC LIMIT 5;"

```

Actual (sample):

- `0984fa86-ca30-4cbf-9104-cf86cd57d048 | purchase | 1002 | mobile | 2026-01-06 16:48:40.065+00`
- `0a1cf227-623d-4fa5-ba77-3bf2460f9623 | purchase | 1005 | web | 2026-01-06 16:48:40.065+00`
- `0227610d-2bfb-4e84-967d-714d5f88bfae | view | 1002 | web | 2026-01-06 16:48:40.065+00`
- `04b77ac4-bb69-4072-9fe3-9189da30869b | view | 1002 | web | 2026-01-06 16:48:40.065+00`
- `0aa9a1f9-e6fd-420d-948c-7ece280e2081 | view | 1002 | web | 2026-01-06 16:48:40.065+00`

Result:

- ✅ Pass

---

## Test Case 6 — Restart works after reset (avoids duplicate key errors)

**Goal:** Confirm a clean reset avoids `duplicate key violates unique constraint` errors.

How to run:

1.  Stop Spark + generator (`Ctrl + C`)
2.  Reset pipeline:

    ```bash
    bash scripts/reset_pipeline.sh

    ```

3.  Optional fresh input:

    ```bash
    rm -f data/incoming/*.csv

    ```

4.  Start generator again
5.  Start Spark streaming again

Expected:

- Spark runs without duplicate key errors.
- Row count starts from 0 and increases.

Actual:

- Reset workflow used successfully to prevent duplicate key errors during restarts.

Result:

- ✅ Pass
