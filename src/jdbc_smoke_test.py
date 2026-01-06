from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JDBC-Smoke-Test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

url = "jdbc:postgresql://localhost:5432/realtime_ecommerce"
props = {
    "user": "realtime_user",
    "password": "realtime_pass",
    "driver": "org.postgresql.Driver",
}

# This reads a simple query through JDBC. If the driver isn't loaded, this fails.
df = spark.read.jdbc(url=url, table="(SELECT 1 AS ok) t", properties=props)
df.show()

spark.stop()
