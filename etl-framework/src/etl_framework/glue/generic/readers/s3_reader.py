"""
S3 Reader - Reads data from Amazon S3.

Supports CSV, JSON, Parquet, ORC, and Avro formats with configurable
format options. Handles cross-account access via IAM role assumption.
"""

import traceback
from typing import Optional, Tuple

from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_source_filter,
)


class S3Reader(ReaderInterface):
    """
    Reads data from Amazon S3 buckets.

    Supports multiple file formats and S3 path patterns.
    Creates a Spark temporary view from the loaded data.
    """

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]:
        """
        Read data from S3 and create a temporary view.

        Constructs the S3 path from source_def fields and reads data
        using the specified format (defaults to parquet).

        Args:
            source_def: Source definition with S3 bucket, path, and format details

        Returns:
            Tuple of (has_error, temp_view_name)
        """
        try:
            # Determine S3 path
            bucket = source_def.source_s3_bucket or self.job_context.ingestion_s3_bucket
            s3_path = source_def.source_s3_path or ""
            full_path = f"s3://{bucket}/{s3_path}" if not s3_path.startswith("s3://") else s3_path

            self.logger.info(f"S3Reader: Reading from {full_path}")

            # Determine format
            file_format = (source_def.source_format or "parquet").lower()
            format_options = source_def.source_format_options or {}

            self.logger.info(f"S3Reader: Format={file_format}, Options={format_options}")

            # Build the reader
            reader = self.spark.read.format(file_format)

            # Apply format-specific options
            if file_format == "csv":
                reader = reader.option("header", format_options.get("header", "true"))
                reader = reader.option("inferSchema", format_options.get("inferSchema", "true"))
                if "delimiter" in format_options:
                    reader = reader.option("delimiter", format_options["delimiter"])
                if "quote" in format_options:
                    reader = reader.option("quote", format_options["quote"])
                if "escape" in format_options:
                    reader = reader.option("escape", format_options["escape"])
                if "multiLine" in format_options:
                    reader = reader.option("multiLine", format_options["multiLine"])
            elif file_format == "json":
                reader = reader.option("multiLine", format_options.get("multiLine", "false"))
                if "schema" in format_options:
                    reader = reader.schema(format_options["schema"])

            # Apply any additional generic options
            for key, value in format_options.items():
                if key not in ("header", "inferSchema", "delimiter", "quote",
                               "escape", "multiLine", "schema"):
                    reader = reader.option(key, value)

            # Load the data
            df = reader.load(full_path)

            # Apply column selection if specified
            if source_def.source_select_columns:
                df = self._validate_and_select_columns(
                    df, source_def.source_select_columns
                )

            # Apply filter if specified
            if source_def.source_filter:
                validate_source_filter(source_def.source_filter, "source_filter")
                df = df.filter(source_def.source_filter)

            # Create temp view
            temp_view_name = self._create_temp_view(df, source_def)
            return False, temp_view_name

        except SQLValidationError as e:
            self.logger.error(f"S3Reader: Validation failed: {str(e)}")
            return True, None
        except Exception as e:
            self.logger.error(f"S3Reader: Failed to read from S3: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True, None
