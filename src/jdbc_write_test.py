from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)

spark = SparkSession.builder.appName("JDBC-Write-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

url = "jdbc:postgresql://localhost:5432/realtime_ecommerce"
props = {
    "user": "realtime_user",
    "password": "realtime_pass",
    "driver": "org.postgresql.Driver",
}

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", StringType(), False),   # cast later
    StructField("user_id", IntegerType(), False),
    StructField("action", StringType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),         # cast later
    StructField("quantity", IntegerType(), True),
    StructField("source", StringType(), True),
])

data = [
    (
        "00000000-0000-0000-0000-000000000001",
        "2026-01-06 00:00:00",
        1,
        "view",
        1001,
        "Test Product",
        "Test",
        None,
        None,
        "web",
    )
]

df = spark.createDataFrame(data, schema=schema)

# Cast into the types Postgres expects (NO UUID CAST)
df2 = df.selectExpr(
    "event_id",
    "cast(event_time as timestamp) as event_time",
    "current_timestamp() as stored_at",
    "user_id",
    "action",
    "product_id",
    "product_name",
    "category",
    "cast(price as decimal(10,2)) as price",
    "cast(quantity as int) as quantity",
    "source"
)

df2.write.jdbc(url=url, table="ecommerce_events", mode="append", properties=props)

print("✅ Wrote 1 row into ecommerce_events")

spark.stop()
