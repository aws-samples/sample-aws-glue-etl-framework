"""
Abstract base interface for all ETL Framework target writers.

All writers (built-in and custom) must implement this interface to be
compatible with the TargetWriter orchestrator and the WriterRegistry.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Tuple

from etl_framework.glue.generic.models.job_config_models import JobContext, TargetDef


class WriterInterface(ABC):
    """
    Abstract base class for target writers.

    Writers are responsible for:
    1. Receiving a Spark DataFrame from a temp view
    2. Writing the data to the target (S3, Redshift, Iceberg, etc.)
    3. Returning success/failure status

    Usage:
        class MyWriter(WriterInterface):
            def write(self, df, target_def, raw_config):
                df.write.parquet("s3://my-bucket/output/")
                return False  # no error
    """

    def __init__(self, job_context: JobContext):
        """
        Initialize the writer with job context.

        Args:
            job_context: The shared job runtime context
        """
        self.job_context = job_context
        self.logger = job_context.logger
        self.spark = job_context.spark_session

    @abstractmethod
    def write(
        self,
        df: Any,
        target_def: TargetDef,
        raw_config: Optional[dict] = None,
    ) -> bool:
        """
        Write a DataFrame to the target.

        Args:
            df: PySpark DataFrame to write
            target_def: Target definition with connection/output details
            raw_config: Optional raw dictionary config for additional options

        Returns:
            has_error: True if the write failed, False on success
        """
        pass
