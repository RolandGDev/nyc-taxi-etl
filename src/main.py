from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("nyc-taxi-etl") \
    .master("local[*]") \
    .getOrCreate()

print(spark.version)

spark.stop()