import os

from pyspark.sql import SparkSession
from extract import extract
from src.config import OUTPUT_PATH
from src.load import load
from src.transform import transform_func


spark = SparkSession.builder \
    .appName("nyc-taxi-etl") \
    .master("local[*]") \
    .getOrCreate()

df = extract(spark)

if df is not None:
    df_transformed = transform_func(df)
    load(df_transformed,OUTPUT_PATH )
    print(f"total de linhas processadas: {df_transformed.count()}")


spark.stop()