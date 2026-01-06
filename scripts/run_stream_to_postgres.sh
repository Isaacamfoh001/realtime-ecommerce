#!/usr/bin/env bash
set -euo pipefail

JAR="/Users/isaacamfoh/.ivy2.5.2/cache/org.postgresql/postgresql/jars/postgresql-42.7.4.jar"

spark-submit \
  --jars "$JAR" \
  src/spark_streaming_to_postgres.py --trigger_seconds 10

