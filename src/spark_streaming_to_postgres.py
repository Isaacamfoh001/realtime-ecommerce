import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp, expr
import time


def build_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_dir", default="data/incoming", help="Folder Spark watches for CSVs")
    parser.add_argument("--checkpoint_dir", default="data/checkpoints/sprint4_postgres", help="Streaming checkpoint dir")
    parser.add_argument("--trigger_seconds", type=int, default=5, help="Micro-batch interval")

    parser.add_argument("--pg_host", default="localhost")
    parser.add_argument("--pg_port", default="5432")
    parser.add_argument("--pg_db", default="realtime_ecommerce")
    parser.add_argument("--pg_user", default="realtime_user")
    parser.add_argument("--pg_password", default="realtime_pass")
    parser.add_argument("--pg_table", default="ecommerce_events")

    args = parser.parse_args()

    spark = build_spark("Sprint4-Streaming-To-Postgres")
    spark.sparkContext.setLogLevel("WARN")

    # 1) Explicit schema (always do this for streaming)
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("source", StringType(), True),
    ])

    # 2) Read CSV files as STREAM
    raw = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(args.input_dir)
    )

    # 3) Clean + cast safely (try_cast prevents crashes on messy rows)
    cleaned = (
        raw
        .withColumn("event_time_ts", expr("try_cast(event_time as timestamp)"))
        .withColumn("user_id_int", expr("try_cast(user_id as int)"))
        .withColumn("product_id_int", expr("try_cast(product_id as int)"))
        .withColumn("price_num", expr("try_cast(price as decimal(10,2))"))
        .withColumn("quantity_int", expr("try_cast(quantity as int)"))
        .withColumn("processed_at", current_timestamp())
    )

    # 4) Filter out bad rows
    valid = (
        cleaned
        .filter(col("action").isin("view", "purchase"))
        .filter(col("event_id").isNotNull())
        .filter(col("event_time_ts").isNotNull())
        .filter(col("user_id_int").isNotNull())
        .filter(col("product_id_int").isNotNull())
    )

    # 5) Final columns mapped to Postgres schema (NO UUID CAST)
    final_df = valid.select(
        col("event_id").alias("event_id"),                 # TEXT in Postgres
        col("event_time_ts").alias("event_time"),
        col("processed_at").alias("stored_at"),
        col("user_id_int").alias("user_id"),
        col("action"),
        col("product_id_int").alias("product_id"),
        col("product_name"),
        col("category"),
        col("price_num").alias("price"),
        col("quantity_int").alias("quantity"),
        col("source"),
    )

    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    jdbc_props = {
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": "org.postgresql.Driver",
    }



    def write_batch(batch_df, batch_id: int):
        """
        Called once per micro-batch.
        Writes micro-batch DataFrame into Postgres with JDBC.
        Also logs simple performance metrics.
        """
        if batch_df.rdd.isEmpty():
            print(f"ℹ️ batch {batch_id}: 0 rows (skipped)")
            return

        start = time.time()

        # de-dupe within the batch
        deduped = batch_df.dropDuplicates(["event_id"])

        # Count once (so we can log + avoid doing an extra action after the write)
        rows = deduped.count()

        deduped.write.jdbc(
            url=jdbc_url,
            table=args.pg_table,
            mode="append",
            properties=jdbc_props
        )

        elapsed = time.time() - start
        rps = rows / elapsed if elapsed > 0 else rows

        print(f"✅ batch {batch_id}: {rows} rows | {elapsed:.2f}s | {rps:.1f} rows/s")


    query = (
        final_df.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
