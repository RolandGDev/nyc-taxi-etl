"""
================================================================================
Load Module — NYC Taxi ETL
================================================================================
Description : Handles persisting the transformed Spark DataFrame to the
              target output path in Parquet format. Includes basic validation
              and structured error handling.

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
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import DataFrame


def load(df: DataFrame, path: str) -> bool:
    """
    Persist the transformed DataFrame to disk as Parquet files.

    Validates that the DataFrame is non-empty before writing. Overwrites
    any existing data at the target path.

    Args:
        df (DataFrame): Transformed Spark DataFrame to be saved.
        path (str): Destination directory path for the Parquet output.

    Returns:
        bool: True if the write succeeds, False if the DataFrame is empty
              or an error occurs.

    Raises:
        Does not raise — errors are caught, logged, and False is returned
        to allow the caller to handle the failure gracefully.
    """
    try:
        if df.isEmpty():
            logging.warning("DataFrame is empty — skipping write to '%s'.", path)
            return False

        logging.info("Writing data to '%s'...", path)
        df.write.mode("overwrite").parquet(path)
        logging.info("Data successfully written to '%s'.", path)
        return True

    except AnalysisException as err:
        logging.error("Spark analysis error while writing to '%s': %s", path, err)
        return False
    except Exception as err:
        logging.error("Unexpected error while writing to '%s': %s", path, err)
        return False
