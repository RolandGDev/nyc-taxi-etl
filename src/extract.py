import logging
import os
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def extract(spark : SparkSession):
    path = os.path.join(BASE_DIR, "data/yellow_tripdata_2024-01.parquet")
    try:
        df = spark.read.parquet(path)
        logging.info(f"Arquivo lido com sucesso: {path}")
        return df
    except Exception as e:
        logging.error(f"Error ao ler arquivo: {e}")
        return None
