import logging
import os
from pyspark.sql import SparkSession
from src.config import DATA_PATH

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def extract(spark : SparkSession):
    path = DATA_PATH
    try:
        df = spark.read.parquet(path)
        logging.info(f"Arquivo lido com sucesso: {path}")
        return df
    except Exception as e:
        logging.error(f"Error ao ler arquivo: {e}")
        return None
