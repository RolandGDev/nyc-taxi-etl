"""
================================================================================
Configuration Module — NYC Taxi ETL
================================================================================
Description : Centralized path configuration for data sources and outputs.
              Paths are resolved dynamically relative to the project root,
              ensuring portability across different environments.

Author      : Roland Garcia
Created     : 2026-01-01
================================================================================
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
import os

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

# Project root: two levels up from this file (src/ -> project root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Input: NYC Yellow Taxi trip data (Parquet format)
DATA_PATH = os.path.join(BASE_DIR, "data", "yellow_tripdata_2024-01.parquet")

# Output: directory for processed/transformed Parquet files
OUTPUT_PATH = os.path.join(BASE_DIR, "output", "nyc_taxi_processed")
