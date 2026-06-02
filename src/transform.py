"""
================================================================================
Transform Module — NYC Taxi ETL
================================================================================
Description : Applies business rules and feature engineering to the raw NYC
              Taxi DataFrame. Filters out invalid records and enriches the
              dataset with derived columns for downstream analysis.

Transformations applied:
    - Filter  : Remove trips with non-positive distance, fare, or duration.
    - Enrich  : Add `trip_duration_min`  — trip duration in minutes.
    - Enrich  : Add `avg_speed_mph`      — average speed in miles per hour.

Author      : Roland Garcia
Created     : 2026-01-01
================================================================================
"""

# ---------------------------------------------------------------------------
# Third-party
# ---------------------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def transform(df: DataFrame) -> DataFrame:
    """
    Clean and enrich the raw NYC Taxi trip DataFrame.

    Steps:
        1. Filter out records where trip_distance, fare_amount, or trip
           duration are zero or negative.
        2. Add ``trip_duration_min``: elapsed time from pickup to dropoff
           in minutes, rounded to 2 decimal places.
        3. Add ``avg_speed_mph``: average speed (miles per hour) computed
           as trip_distance / duration_in_hours, rounded to 2 decimal places.

    Args:
        df (DataFrame): Raw Spark DataFrame loaded from the source Parquet file.

    Returns:
        DataFrame: Cleaned and enriched Spark DataFrame.
    """
    # Duration in seconds (used as intermediate value)
    duration_seconds = (
        F.unix_timestamp("tpep_dropoff_datetime")
        - F.unix_timestamp("tpep_pickup_datetime")
    )

    # Step 1 — Filter invalid trips
    df_filtered = df.filter(
        (F.col("trip_distance") > 0)
        & (F.col("fare_amount") > 0)
        & (duration_seconds > 0)
    )

    # Step 2 — Add trip duration in minutes
    df_with_duration = df_filtered.withColumn(
        "trip_duration_min",
        F.round(duration_seconds / 60, 2),
    )

    # Step 3 — Add average speed in mph
    df_with_speed = df_with_duration.withColumn(
        "avg_speed_mph",
        F.round(F.col("trip_distance") / (duration_seconds / 3600), 2),
    )

    return df_with_speed
