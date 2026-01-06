#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

psql -d realtime_ecommerce -c "TRUNCATE TABLE ecommerce_events;"
rm -rf data/checkpoints/sprint4_postgres

echo "✅ Reset done: truncated table + cleared checkpoint"
