import os

from pyspark.sql import SparkSession
from extract import extract

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


spark = SparkSession.builder \
    .appName("nyc-taxi-etl") \
    .master("local[*]") \
    .getOrCreate()

df = extract(spark)

if df is not None:
    df.printSchema()
    df.show(5)
    print(f"Total de linhas : {df.count()}")

spark.stop()