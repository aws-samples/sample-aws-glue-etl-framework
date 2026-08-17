"""
Abstract base interface for all ETL Framework source readers.

All readers (built-in and custom) must implement this interface to be
compatible with the SourceReader orchestrator and the ReaderRegistry.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple

from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef
from etl_framework.glue.generic.utils.sql_validation import (
    validate_column_list,
    validate_temp_view_name,
)


class ReaderInterface(ABC):
    """
    Abstract base class for source readers.

    Readers are responsible for:
    1. Reading data from a source (S3, Redshift, API, etc.)
    2. Creating a Spark temporary view with the data
    3. Returning the temp view name for downstream processing

    Usage:
        class MyReader(ReaderInterface):
            def read(self, source_def):
                df = self.job_context.spark_session.read.parquet("s3://...")
                temp_view = f"temp_{source_def.source_key}"
                df.createOrReplaceTempView(temp_view)
                return False, temp_view
    """

    def __init__(self, job_context: JobContext):
        """
        Initialize the reader with job context.

        Args:
            job_context: The shared job runtime context
        """
        self.job_context = job_context
        self.logger = job_context.logger
        self.spark = job_context.spark_session

    @abstractmethod
    def read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]:
        """
        Read data from the source and create a temporary view.

        Args:
            source_def: Source definition containing connection details and parameters

        Returns:
            Tuple of (has_error, temp_view_name):
                - has_error: True if the read failed, False on success
                - temp_view_name: Name of the created temp view, or None on failure
                  May also be None when no new data is available (e.g., watermark
                  indicates no new records)
        """
        pass

    def _create_temp_view(self, df: Any, source_def: SourceDef) -> str:
        """
        Helper to create a temporary view from a DataFrame.

        Validates the source_key to prevent SQL injection when the
        resulting temp view name is later interpolated into SQL queries.

        Args:
            df: PySpark DataFrame
            source_def: Source definition (used for naming)

        Returns:
            The temp view name created

        Raises:
            SQLValidationError: If source_key contains unsafe characters
        """
        temp_view_name = validate_temp_view_name(source_def.source_key)
        df.createOrReplaceTempView(temp_view_name)
        self.logger.info(
            f"Created temp view '{temp_view_name}' with {df.count()} rows"
        )
        return temp_view_name

    def _validate_and_select_columns(self, df: Any, columns: Any) -> Any:
        """
        Validate column names and apply column selection to a DataFrame.

        Args:
            df: PySpark DataFrame
            columns: List of column names to select

        Returns:
            DataFrame with selected columns applied

        Raises:
            SQLValidationError: If any column name is invalid
        """
        if columns:
            validate_column_list(columns, "source_select_columns")
            df = df.select(*columns)
        return df
