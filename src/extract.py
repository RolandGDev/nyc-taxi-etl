"""
================================================================================
Extract Module — NYC Taxi ETL
================================================================================
Description : Handles data ingestion from the source Parquet file into a
              PySpark DataFrame. Provides validation and structured logging
              for observability.

Author      : Roland Garcia
Created     : 2026-01-01
================================================================================
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import logging

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
from pyspark.sql import DataFrame, SparkSession

# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------
from src.config import DATA_PATH


def extract(spark: SparkSession) -> DataFrame | None:
    """
    Read the NYC Taxi trip data from a Parquet file into a Spark DataFrame.

    Args:
        spark (SparkSession): Active Spark session used to read the file.

    Returns:
        DataFrame: Loaded Spark DataFrame if the file is read successfully.
        None: If an error occurs during reading.

    Raises:
        Does not raise — errors are caught, logged, and None is returned
        to allow the caller to handle the failure gracefully.
    """
    try:
        df = spark.read.parquet(DATA_PATH)
        logging.info(f"File read successfully: {DATA_PATH}")
        return df
    except Exception as err:
        logging.error(f"Failed to read file '{DATA_PATH}': {err}")
        return None
