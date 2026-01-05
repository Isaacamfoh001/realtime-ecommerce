import os
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)
from pyspark.sql.functions import col, current_timestamp, expr


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="data/incoming", help="Folder Spark will watch for new CSV files")
    parser.add_argument("--checkpoint_dir", default="data/checkpoints/sprint3", help="Checkpoint directory for streaming state")
    parser.add_argument("--parquet_out", default="data/processed/events_parquet", help="Where cleaned output parquet will be written")
    parser.add_argument("--trigger_seconds", type=int, default=5, help="How often Spark processes new files (micro-batch interval)")
    args = parser.parse_args()

    spark = build_spark("Sprint3-CSV-Streaming-Cleaning")
    spark.sparkContext.setLogLevel("WARN")

    # 1) Explicit schema (important for streaming; avoid inference)
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

    # 2) Read CSV files as a STREAM
    raw = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(args.input_dir)
    )

    # 3) Transform / clean / cast
    # - Parse event_time → timestamp (invalid ones become NULL)
    # - Cast ids and numbers
    cleaned = (
        raw
        .withColumn("event_time_ts", expr("try_cast(event_time as timestamp)"))
        .withColumn("user_id_int", expr("try_cast(user_id as int)"))
        .withColumn("product_id_int", expr("try_cast(product_id as int)"))
        .withColumn("price_num", expr("try_cast(price as decimal(10,2))"))
        .withColumn("quantity_int", expr("try_cast(quantity as int)"))
        .withColumn("processed_at", current_timestamp())
    )


    # 4) Filter out bad rows (this is where your "robust/messy" generator helps)
    valid = (
        cleaned
        .filter(col("action").isin("view", "purchase"))
        .filter(col("event_id").isNotNull())
        .filter(col("event_time_ts").isNotNull())
        .filter(col("user_id_int").isNotNull())
        .filter(col("product_id_int").isNotNull())
    )

    # Keep columns tidy
    final_df = (
        valid.select(
            col("event_id"),
            col("event_time_ts").alias("event_time"),
            col("processed_at"),
            col("user_id_int").alias("user_id"),
            col("action"),
            col("product_id_int").alias("product_id"),
            col("product_name"),
            col("category"),
            col("price_num").alias("price"),
            col("quantity_int").alias("quantity"),
            col("source"),
        )
    )

    # 5a) Console sink (quick live visibility)
    console_query = (
        final_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # 5b) Parquet sink (useful for later inspection)
    parquet_query = (
        final_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", args.parquet_out)
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # Wait for termination (Ctrl+C)
    console_query.awaitTermination()


if __name__ == "__main__":
    main()
