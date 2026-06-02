"""
================================================================================
NYC Taxi ETL — Entry Point
================================================================================
Description : Orchestrates the full ETL pipeline for NYC Yellow Taxi trip data.
              Initializes a local Spark session, then executes the Extract →
              Transform → Load sequence.

Pipeline:
    1. Extract   : Read raw Parquet data from the configured source path.
    2. Transform : Filter invalid trips; add duration and speed columns.
    3. Load      : Write the processed DataFrame to the output path as Parquet.

Usage:
    Run from the project root:
        python -m src.main

Author      : Roland Garcia
Created     : 2026-01-01
================================================================================
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import logging
import sys

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------
from src.config import OUTPUT_PATH
from src.extract import extract
from src.load import load
from src.transform import transform

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("nyc-taxi-etl")
    .master("local[*]")
    .getOrCreate()
)

# Reduce Spark verbosity
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------
try:
    # Step 1 — Extract
    df_raw = extract(spark)
    if df_raw is None:
        logging.error("Extraction failed — aborting pipeline.")
        sys.exit(1)

    # Step 2 — Transform
    df_transformed = transform(df_raw)

    # Step 3 — Load
    success = load(df_transformed, OUTPUT_PATH)
    if success:
        logging.info("Pipeline completed. Total rows processed: %d", df_transformed.count())
    else:
        logging.warning("Pipeline finished but no data was written.")

finally:
    spark.stop()
