"""
Source Reader Orchestrator.

Routes source definitions to their registered reader implementations
via the ReaderRegistry plugin system. Manages the read lifecycle
including error aggregation and watermark tracking.
"""

import traceback
from typing import Dict, Tuple

from etl_framework.glue.generic.models.job_config_models import (
    JobContext,
    JobConfig,
    SourceDef,
)
from etl_framework.glue.generic.registry import ReaderRegistry
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_column_name,
    validate_identifier,
)

# Import built-in readers to trigger their registration
from etl_framework.glue.generic.readers.s3_reader import S3Reader
from etl_framework.glue.generic.readers.glue_reader import GlueReader
from etl_framework.glue.generic.readers.iceberg_reader import IcebergReader
from etl_framework.glue.generic.readers.redshift_reader import RedshiftReader
from etl_framework.glue.generic.readers.dynamodb_reader import DynamoDBReader
from etl_framework.glue.generic.readers.api_reader import APIReader

# Register built-in readers
ReaderRegistry.register("S3", S3Reader)
ReaderRegistry.register("GLUE", GlueReader)
ReaderRegistry.register("ICEBERG", IcebergReader)
ReaderRegistry.register("REDSHIFT", RedshiftReader)
ReaderRegistry.register("REDSHIFT_SERVERLESS", RedshiftReader)
ReaderRegistry.register("DYNAMODB", DynamoDBReader)
ReaderRegistry.register("API", APIReader)


class SourceReader:
    """
    Orchestrates reading from all sources defined in a job configuration.

    Uses the ReaderRegistry to resolve the appropriate reader for each
    source type. Supports custom readers registered by users.
    """

    def __init__(self, job_context: JobContext, job_config: JobConfig):
        self.job_context = job_context
        self.job_config = job_config
        self.logger = job_context.logger
        self.logger.info("SourceReader initialized")
        self.logger.info(
            f"Registered reader types: {ReaderRegistry.list_registered()}"
        )

    def read_source_list(self) -> Tuple[bool, Dict[str, str]]:
        """
        Read all sources in the job configuration and create temp views.

        Returns:
            Tuple of (has_no_error, source_views):
                - has_no_error: True if ALL sources read successfully, False if any failed
                - source_views: Dictionary mapping source_key to temp_view_name
                  (value may be None if source had no new data)
        """
        self.logger.info("=== Starting read_source_list ===")
        source_views: Dict[str, str] = {}
        has_error = False

        if self.job_config is None:
            self.logger.error("Job config is None, cannot read sources")
            return False, source_views

        if not self.job_config.source_list:
            self.logger.warning("No sources defined in job config")
            return True, source_views

        self.logger.info(f"Processing {len(self.job_config.source_list)} source(s)")

        for idx, source in enumerate(self.job_config.source_list, 1):
            self.logger.info(
                f"--- Processing source {idx}/{len(self.job_config.source_list)} ---"
            )

            try:
                source_def = SourceDef.from_dict(source)
                self.logger.info(
                    f"Source key: {source_def.source_key}, "
                    f"Type: {source_def.source_type}"
                )

                # Look up reader from registry
                reader_class = ReaderRegistry.get(source_def.source_type)

                if reader_class is None:
                    self.logger.error(
                        f"No reader registered for source type: '{source_def.source_type}'. "
                        f"Available types: {ReaderRegistry.list_registered()}"
                    )
                    has_error = True
                    continue

                # Instantiate and execute reader
                reader = reader_class(self.job_context)
                read_error, temp_view_name = reader.read(source_def)

                if read_error:
                    self.logger.error(
                        f"Read failed for source: {source_def.source_key}"
                    )
                    has_error = True
                else:
                    self.logger.info(
                        f"Read successful. Temp view: {temp_view_name}"
                    )
                    source_views[source_def.source_key] = temp_view_name

                    # Track watermark for later commit
                    if (
                        source_def.watermark_strategy
                        and source_def.watermark_strategy != "none"
                        and temp_view_name
                    ):
                        self._track_pending_watermark(source_def, temp_view_name)

            except Exception as e:
                self.logger.error(
                    f"Exception processing source {idx}: {str(e)}"
                )
                self.logger.error(traceback.format_exc())
                has_error = True

        self.logger.info(f"=== read_source_list complete. Sources read: {len(source_views)} ===")
        return not has_error, source_views

    def _track_pending_watermark(
        self, source_def: SourceDef, temp_view_name: str
    ) -> None:
        """
        Track a pending watermark for later commit after successful target writes.

        Watermarks are only committed if the corresponding target write succeeds.
        """
        if not source_def.watermark_column:
            return

        try:
            # Validate watermark_column to prevent SQL injection
            validate_column_name(
                source_def.watermark_column, "watermark_column"
            )
            # Validate temp_view_name as an identifier
            validate_identifier(temp_view_name, "temp_view_name")

            # Get the max value of the watermark column from the temp view
            df = self.job_context.spark_session.sql(
                f"SELECT MAX({source_def.watermark_column}) as max_watermark "
                f"FROM {temp_view_name}"
            )
            row = df.collect()
            if row and row[0]["max_watermark"] is not None:
                watermark_value = str(row[0]["max_watermark"])
                self.job_context.pending_watermarks[source_def.source_key] = {
                    "watermark_key": source_def.table_name_watermark or source_def.source_key,
                    "watermark_value": watermark_value,
                    "watermark_column": source_def.watermark_column,
                    "watermark_strategy": source_def.watermark_strategy,
                }
                self.logger.info(
                    f"Tracked pending watermark for '{source_def.source_key}': "
                    f"{watermark_value}"
                )
        except SQLValidationError as e:
            self.logger.error(
                f"Watermark tracking validation failed for '{source_def.source_key}': "
                f"{str(e)}"
            )
        except Exception as e:
            self.logger.warning(
                f"Failed to track watermark for '{source_def.source_key}': {str(e)}"
            )

    def commit_pending_watermarks(
        self, target_results: Dict[str, str], job_config: JobConfig
    ) -> None:
        """
        Commit watermarks for sources whose targets all succeeded.

        A source's watermark is only committed if ALL targets referencing
        that source completed successfully (status == "SUCCESS").

        Args:
            target_results: Dictionary mapping target identifiers to status strings
            job_config: The job configuration for source-target mapping
        """
        if not self.job_context.pending_watermarks:
            self.logger.info("No pending watermarks to commit")
            return

        if not self.job_context.table_name_watermark:
            self.logger.warning(
                "No watermark table configured, skipping watermark commit"
            )
            return

        import boto3

        dynamodb = boto3.resource("dynamodb", region_name=self.job_context.aws_region)
        table = dynamodb.Table(self.job_context.table_name_watermark)

        for source_key, watermark_info in self.job_context.pending_watermarks.items():
            # Check if all targets for this source succeeded
            source_targets_succeeded = self._all_targets_succeeded(
                source_key, target_results, job_config
            )

            if source_targets_succeeded:
                try:
                    table.put_item(
                        Item={
                            "watermark_key": watermark_info["watermark_key"],
                            "watermark_value": watermark_info["watermark_value"],
                            "watermark_column": watermark_info["watermark_column"],
                            "source_key": source_key,
                        }
                    )
                    self.logger.info(
                        f"Committed watermark for '{source_key}': "
                        f"{watermark_info['watermark_value']}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to commit watermark for '{source_key}': {str(e)}"
                    )
            else:
                self.logger.warning(
                    f"Skipping watermark commit for '{source_key}' - "
                    f"not all targets succeeded"
                )

    def _all_targets_succeeded(
        self,
        source_key: str,
        target_results: Dict[str, str],
        job_config: JobConfig,
    ) -> bool:
        """Check if all targets for a given source succeeded."""
        if not job_config.target_list:
            return True

        for target in job_config.target_list:
            if target.get("source_config_key") == source_key:
                target_id = f"{target.get('database_name', '')}.{target.get('table_name', '')}"
                status = target_results.get(target_id, "")
                if status != "SUCCESS":
                    return False
        return True
