# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("JDBC-Driver-Check").getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")

# try:
#     # This will only work if the postgres JDBC jar is on Spark's classpath
#     spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
#     print("✅ Postgres JDBC driver is available to Spark (org.postgresql.Driver loaded).")
# except Exception as e:
#     print("❌ Postgres JDBC driver NOT found on Spark classpath.")
#     print("   Error:", e)
# finally:
#     spark.stop()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JDBC-Driver-Check").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

jvm = spark._jvm

try:
    # Use Spark/Thread context classloader (this is where --jars attaches)
    cl = jvm.Thread.currentThread().getContextClassLoader()
    cl.loadClass("org.postgresql.Driver")
    print("✅ Postgres JDBC driver is available via Spark context classloader.")
except Exception as e:
    print("❌ Postgres JDBC driver NOT found via Spark context classloader.")
    print("   Error:", e)
finally:
    spark.stop()

