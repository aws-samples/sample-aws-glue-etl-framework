"""
Iceberg Reader - Reads data from Apache Iceberg tables via Glue Catalog.

Supports time-travel queries, snapshot-based reads, and incremental
reads using Iceberg's built-in changelog capabilities.
"""

import traceback
from typing import Optional, Tuple

from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface
from etl_framework.glue.generic.utils.sql_validation import (
    build_safe_qualified_name,
    SQLValidationError,
    validate_identifier,
    validate_read_only_sql,
    validate_source_filter,
)


class IcebergReader(ReaderInterface):
    """
    Reads data from Apache Iceberg tables.

    Uses the Iceberg Spark catalog integration to read tables.
    Supports reading from specific snapshots or time-travel queries.
    """

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]:
        """
        Read data from an Iceberg table and create a temporary view.

        Args:
            source_def: Source definition with database_name, table_name,
                       and optional catalog_name

        Returns:
            Tuple of (has_error, temp_view_name)
        """
        try:
            database_name = source_def.database_name
            table_name = source_def.table_name
            catalog_name = source_def.catalog_name or "iceberg"

            if not database_name or not table_name:
                self.logger.error(
                    "IcebergReader: database_name and table_name are required"
                )
                return True, None

            # Validate identifiers to prevent SQL injection
            validate_identifier(catalog_name, "catalog_name")
            validate_identifier(database_name, "database_name")
            validate_identifier(table_name, "table_name")

            # Construct full table identifier with catalog prefix
            full_table_name = build_safe_qualified_name(
                catalog_name, database_name, table_name, escape=False
            )
            self.logger.info(f"IcebergReader: Reading from {full_table_name}")

            # Use custom SQL if provided, otherwise read full table
            if source_def.source_sql:
                # Validate SQL is read-only
                validate_read_only_sql(source_def.source_sql, "source_sql")
                self.logger.info(
                    f"IcebergReader: Using custom SQL: {source_def.source_sql}"
                )
                df = self.spark.sql(source_def.source_sql)
            else:
                df = self.spark.table(full_table_name)

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
            self.logger.error(f"IcebergReader: Validation failed: {str(e)}")
            return True, None
        except Exception as e:
            self.logger.error(f"IcebergReader: Failed to read: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True, None
