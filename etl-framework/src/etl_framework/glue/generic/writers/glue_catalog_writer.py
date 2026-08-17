"""
Glue Catalog Writer - Writes data to AWS Glue Data Catalog tables.

Registers or updates tables in the Glue Data Catalog using either
the Glue DynamicFrame API or Spark DataFrame API.
"""

import traceback
from typing import Any, Optional

from etl_framework.glue.generic.models.job_config_models import JobContext, TargetDef
from etl_framework.glue.generic.utils.sql_validation import (
    build_safe_qualified_name,
    SQLValidationError,
    validate_identifier,
)
from etl_framework.glue.generic.writers.writer_interface import WriterInterface


class GlueCatalogWriter(WriterInterface):
    """
    Writes DataFrames to AWS Glue Data Catalog tables.

    Uses Glue DynamicFrame sink when a Glue context is available,
    otherwise falls back to Spark DataFrame write with catalog integration.
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
        Write DataFrame to a Glue Catalog table.

        Args:
            df: PySpark DataFrame to write
            target_def: Target definition with database/table names and options
            raw_config: Optional raw config for additional options

        Returns:
            True if write failed, False on success
        """
        try:
            raw_config = raw_config or {}
            database_name = target_def.database_name
            table_name = target_def.table_name
            write_mode = target_def.write_mode or "overwrite"

            if not database_name or not table_name:
                self.logger.error(
                    "GlueCatalogWriter: database_name and table_name are required"
                )
                return True

            # Validate identifiers to prevent SQL injection
            validate_identifier(database_name, "database_name")
            validate_identifier(table_name, "table_name")

            self.logger.info(
                f"GlueCatalogWriter: Writing to {database_name}.{table_name} "
                f"(mode={write_mode})"
            )

            # Determine S3 path for the table data
            s3_path = target_def.target_s3_path
            if not s3_path:
                s3_path = (
                    f"s3://{self.job_context.glue_s3_bucket}/data/"
                    f"{database_name}/{table_name}/"
                )

            # Use Glue DynamicFrame if context available
            if self.job_context.glue_context:
                self._write_with_glue_context(
                    df, database_name, table_name, s3_path, write_mode, target_def
                )
            else:
                self._write_with_spark(
                    df, database_name, table_name, s3_path, write_mode, target_def
                )

            self.logger.info("GlueCatalogWriter: Write successful")
            return False

        except SQLValidationError as e:
            self.logger.error(f"GlueCatalogWriter: Validation failed: {str(e)}")
            return True
        except Exception as e:
            self.logger.error(f"GlueCatalogWriter: Failed to write: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True

    def _write_with_glue_context(
        self,
        df: Any,
        database_name: str,
        table_name: str,
        s3_path: str,
        write_mode: str,
        target_def: TargetDef,
    ) -> None:
        """Write using Glue DynamicFrame API."""
        from awsglue.dynamicframe import DynamicFrame

        dynamic_frame = DynamicFrame.fromDF(
            df, self.job_context.glue_context, f"dynamic_frame_{table_name}"
        )

        sink_options = {
            "database": database_name,
            "table_name": table_name,
            "path": s3_path,
        }

        # Add partitioning if specified
        partition_keys = target_def.partition_by
        if partition_keys:
            if isinstance(partition_keys, str):
                partition_keys = [partition_keys]
            sink_options["partitionKeys"] = partition_keys

        self.job_context.glue_context.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=database_name,
            table_name=table_name,
            additional_options={"path": s3_path},
        )

    def _write_with_spark(
        self,
        df: Any,
        database_name: str,
        table_name: str,
        s3_path: str,
        write_mode: str,
        target_def: TargetDef,
    ) -> None:
        """Write using Spark DataFrame API with catalog registration."""
        writer = df.write.mode(write_mode)

        # Apply partitioning
        if target_def.partition_by:
            partition_cols = target_def.partition_by
            if isinstance(partition_cols, str):
                partition_cols = [partition_cols]
            for col in partition_cols:
                validate_identifier(col, "partition_by column")
            writer = writer.partitionBy(*partition_cols)

        # Apply format (default parquet for catalog tables)
        file_format = target_def.target_format or "parquet"
        writer = writer.format(file_format)

        # Write as managed table (identifiers already validated in write())
        full_table_name = build_safe_qualified_name(
            database_name, table_name, escape=False
        )
        writer.saveAsTable(full_table_name)
