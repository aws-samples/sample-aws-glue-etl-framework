"""
Target Writer Orchestrator.

Routes target definitions to their registered writer implementations
via the WriterRegistry plugin system. Manages the write lifecycle
including error aggregation and metrics publishing.
"""

import traceback
from typing import Dict, Tuple

from pyspark.sql import functions as F

from etl_framework.glue.generic.models.job_config_models import (
    JobContext,
    JobConfig,
    TargetDef,
)
from etl_framework.glue.generic.registry import WriterRegistry
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_column_list,
    validate_identifier,
)

# Import built-in writers to trigger their registration
from etl_framework.glue.generic.writers.s3_writer import S3Writer
from etl_framework.glue.generic.writers.glue_catalog_writer import GlueCatalogWriter
from etl_framework.glue.generic.writers.iceberg_writer import IcebergWriter
from etl_framework.glue.generic.writers.redshift_writer import RedshiftWriter
from etl_framework.glue.generic.writers.dynamodb_writer import DynamoDBWriter

# Register built-in writers
WriterRegistry.register("S3", S3Writer)
WriterRegistry.register("GLUE", GlueCatalogWriter)
WriterRegistry.register("ICEBERG", IcebergWriter)
WriterRegistry.register("REDSHIFT", RedshiftWriter)
WriterRegistry.register("REDSHIFT_SERVERLESS", RedshiftWriter)
WriterRegistry.register("DYNAMODB", DynamoDBWriter)


class TargetWriter:
    """
    Orchestrates writing to all targets defined in a job configuration.

    Uses the WriterRegistry to resolve the appropriate writer for each
    target type. Supports custom writers registered by users.
    """

    def __init__(self, job_context: JobContext, job_config: JobConfig):
        self.job_context = job_context
        self.job_config = job_config
        self.logger = job_context.logger
        self.logger.info("TargetWriter initialized")
        self.logger.info(
            f"Registered writer types: {WriterRegistry.list_registered()}"
        )

    def write_target_list(
        self, source_views: Dict[str, str]
    ) -> Tuple[bool, Dict[str, str]]:
        """
        Write data from temp views to all configured targets.

        Args:
            source_views: Dictionary mapping source_key to temp_view_name

        Returns:
            Tuple of (has_error, target_results):
                - has_error: True if ANY target write failed
                - target_results: Dictionary mapping target identifier to status
                  ("SUCCESS", "FAILED", "SKIPPED")
        """
        self.logger.info("=== Starting write_target_list ===")
        target_results: Dict[str, str] = {}
        has_error = False

        if self.job_config is None:
            self.logger.error("Job config is None, cannot write targets")
            return True, target_results

        if not self.job_config.target_list:
            self.logger.warning("No targets defined in job config")
            return False, target_results

        self.logger.info(f"Processing {len(self.job_config.target_list)} target(s)")

        for idx, target in enumerate(self.job_config.target_list, 1):
            self.logger.info(
                f"--- Processing target {idx}/{len(self.job_config.target_list)} ---"
            )

            try:
                target_def = TargetDef.from_dict(target)
                target_id = self._get_target_id(target_def)
                self.logger.info(
                    f"Target type: {target_def.target_type}, "
                    f"Table: {target_id}"
                )

                # Resolve source data from temp view
                source_key = target_def.source_config_key or target.get("source_config_key")
                if not source_key or source_key not in source_views:
                    self.logger.error(
                        f"Source key '{source_key}' not found in source views. "
                        f"Available: {list(source_views.keys())}"
                    )
                    has_error = True
                    target_results[target_id] = "FAILED"
                    continue

                temp_view_name = source_views[source_key]

                # Skip if source has no data (temp view is None)
                if temp_view_name is None:
                    self.logger.info(
                        f"No temp view for source '{source_key}' "
                        f"(no new data available). Skipping target {target_id}."
                    )
                    target_results[target_id] = "SKIPPED"
                    continue

                # Validate temp view name before SQL interpolation
                validate_identifier(temp_view_name, "temp_view_name")

                # Read data from temp view
                df = self.job_context.spark_session.sql(
                    f"SELECT * FROM {temp_view_name}"
                )

                # Add internal record ID if configured
                if target_def.internal_record_id:
                    df = df.withColumn("source_id", F.expr("uuid()"))

                # Apply column selection if specified
                if target_def.target_select:
                    validate_column_list(
                        target_def.target_select, "target_select"
                    )
                    df = df.select(*target_def.target_select)

                # Apply column aliases if specified
                if target_def.column_aliases:
                    for old_name, new_name in target_def.column_aliases.items():
                        validate_identifier(old_name, "column_alias source")
                        validate_identifier(new_name, "column_alias target")
                        df = df.withColumnRenamed(old_name, new_name)

                row_count = df.count()
                self.logger.info(f"DataFrame has {row_count} rows")

                # Look up writer from registry
                writer_class = WriterRegistry.get(target_def.target_type)

                if writer_class is None:
                    self.logger.error(
                        f"No writer registered for target type: '{target_def.target_type}'. "
                        f"Available types: {WriterRegistry.list_registered()}"
                    )
                    has_error = True
                    target_results[target_id] = "FAILED"
                    continue

                # Instantiate and execute writer
                writer = writer_class(self.job_context)
                write_error = writer.write(df, target_def, target)

                if write_error:
                    self.logger.error(f"Write failed for target: {target_id}")
                    has_error = True
                    target_results[target_id] = "FAILED"
                else:
                    self.logger.info(f"Write successful for target: {target_id}")
                    target_results[target_id] = "SUCCESS"

            except SQLValidationError as e:
                self.logger.error(
                    f"Validation failed for target {idx}: {str(e)}"
                )
                has_error = True
                target_results[self._get_target_id_from_raw(target)] = "FAILED"
            except Exception as e:
                self.logger.error(
                    f"Exception processing target {idx}: {str(e)}"
                )
                self.logger.error(traceback.format_exc())
                has_error = True
                target_results[self._get_target_id_from_raw(target)] = "FAILED"

        self.logger.info(
            f"=== write_target_list complete. Results: {target_results} ==="
        )
        return has_error, target_results

    def _get_target_id(self, target_def: TargetDef) -> str:
        """Generate a unique identifier for a target."""
        parts = [
            target_def.database_name or "",
            target_def.schema_name or "",
            target_def.table_name or "",
        ]
        return ".".join(p for p in parts if p)

    def _get_target_id_from_raw(self, target: dict) -> str:
        """Generate target ID from raw config dict (for error cases)."""
        parts = [
            target.get("database_name", ""),
            target.get("schema_name", ""),
            target.get("table_name", "unknown"),
        ]
        return ".".join(p for p in parts if p)
