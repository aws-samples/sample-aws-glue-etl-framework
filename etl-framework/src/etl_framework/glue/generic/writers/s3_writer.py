"""
S3 Writer - Writes data to Amazon S3.

Supports CSV, JSON, Parquet, ORC, and Avro output formats with
configurable partitioning, compression, and write modes.
"""

import traceback
from typing import Any, Optional

from etl_framework.glue.generic.models.job_config_models import JobContext, TargetDef
from etl_framework.glue.generic.writers.writer_interface import WriterInterface


class S3Writer(WriterInterface):
    """
    Writes DataFrames to Amazon S3 in various formats.

    Supports partitioning, compression, and configurable write modes.
    """

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def write(
        self,
        df: Any,
        target_def: TargetDef,
        raw_config: Optional[dict] = None,
    ) -> bool:
        """
        Write DataFrame to S3.

        Args:
            df: PySpark DataFrame to write
            target_def: Target definition with S3 path, format, and options
            raw_config: Optional raw config for additional options

        Returns:
            True if write failed, False on success
        """
        try:
            raw_config = raw_config or {}

            # Determine output path
            s3_path = target_def.target_s3_path
            if not s3_path:
                s3_path = (
                    f"s3://{self.job_context.glue_s3_bucket}/data/"
                    f"{target_def.database_name}/{target_def.table_name}/"
                )

            self.logger.info(f"S3Writer: Writing to {s3_path}")

            # Determine format
            file_format = (target_def.target_format or "parquet").lower()
            write_mode = target_def.write_mode or "overwrite"
            format_options = target_def.target_format_options or {}

            self.logger.info(
                f"S3Writer: Format={file_format}, Mode={write_mode}"
            )

            # Build the writer
            writer = df.write.format(file_format).mode(write_mode)

            # Apply format-specific options
            if file_format == "csv":
                writer = writer.option("header", format_options.get("header", "true"))
                if "delimiter" in format_options:
                    writer = writer.option("delimiter", format_options["delimiter"])
                if "quote" in format_options:
                    writer = writer.option("quote", format_options["quote"])
            elif file_format == "json":
                if "compression" in format_options:
                    writer = writer.option("compression", format_options["compression"])

            # Apply compression
            if target_def.compression:
                writer = writer.option("compression", target_def.compression)

            # Apply max records per file
            if target_def.max_records_per_file:
                writer = writer.option(
                    "maxRecordsPerFile", target_def.max_records_per_file
                )

            # Apply additional format options
            for key, value in format_options.items():
                if key not in ("header", "delimiter", "quote", "compression"):
                    writer = writer.option(key, value)

            # Apply partitioning
            if target_def.partition_by:
                partition_cols = target_def.partition_by
                if isinstance(partition_cols, str):
                    partition_cols = [partition_cols]
                writer = writer.partitionBy(*partition_cols)
                self.logger.info(f"S3Writer: Partitioning by {partition_cols}")

            # Write
            writer.save(s3_path)
            self.logger.info(f"S3Writer: Write successful to {s3_path}")
            return False

        except Exception as e:
            self.logger.error(f"S3Writer: Failed to write: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True
