import os

from pyspark.sql import SparkSession
from extract import extract
from src.transform import transform_func

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


spark = SparkSession.builder \
    .appName("nyc-taxi-etl") \
    .master("local[*]") \
    .getOrCreate()

df = extract(spark)

df_transformed = transform_func(df)

if df_transformed is not None:
    df_transformed.show(5)
    print(f"total apos a transformacao : {df_transformed.count()}")


spark.stop()