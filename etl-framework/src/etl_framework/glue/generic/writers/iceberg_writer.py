"""
Iceberg Writer - Writes data to Apache Iceberg tables.

Supports table creation, overwrite, and append operations using
Iceberg format version 2. Integrates with the Glue Data Catalog
as the Iceberg metastore.
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


class IcebergWriter(WriterInterface):
    """
    Writes DataFrames to Apache Iceberg tables.

    Supports:
    - Creating new tables with optional partitioning
    - Overwriting existing tables (createOrReplace)
    - Appending to existing tables
    - Iceberg format version 2
    - Cross-catalog table references
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
        Write DataFrame to an Iceberg table.

        Args:
            df: PySpark DataFrame to write
            target_def: Target definition with database, table, and catalog names
            raw_config: Optional raw config for additional options

        Returns:
            True if write failed, False on success
        """
        try:
            raw_config = raw_config or {}
            database_name = target_def.database_name
            table_name = target_def.table_name
            catalog_name = target_def.catalog_name or raw_config.get("catalog_name", "iceberg")
            write_mode = (target_def.write_mode or "overwrite").lower()

            if not database_name or not table_name:
                self.logger.error(
                    "IcebergWriter: database_name and table_name are required"
                )
                return True

            # Validate identifiers to prevent SQL injection
            validate_identifier(catalog_name, "catalog_name")
            validate_identifier(database_name, "database_name")
            validate_identifier(table_name, "table_name")

            full_table_name = build_safe_qualified_name(
                catalog_name, database_name, table_name, escape=False
            )
            self.logger.info(
                f"IcebergWriter: Writing to {full_table_name} (mode={write_mode})"
            )

            # Ensure database exists
            self._ensure_database(catalog_name, database_name)

            # Check if table exists
            table_exists = self._table_exists(full_table_name)

            if not table_exists:
                self._create_table(df, full_table_name, target_def)
            else:
                if write_mode == "overwrite":
                    self._overwrite_table(df, full_table_name, target_def)
                elif write_mode == "append":
                    self._append_to_table(df, full_table_name)
                else:
                    self.logger.error(
                        f"IcebergWriter: Unsupported write mode: {write_mode}"
                    )
                    return True

            # Log final statistics
            self._log_table_stats(full_table_name)
            self.logger.info(f"IcebergWriter: Write successful for {full_table_name}")
            return False

        except SQLValidationError as e:
            self.logger.error(f"IcebergWriter: Validation failed: {str(e)}")
            return True
        except Exception as e:
            self.logger.error(f"IcebergWriter: Failed to write: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True

    def _ensure_database(self, catalog_name: str, database_name: str) -> None:
        """Ensure the database exists in the Iceberg catalog."""
        try:
            # Identifiers already validated in write() method
            safe_name = f"{catalog_name}.{database_name}"
            self.spark.sql(
                f"CREATE DATABASE IF NOT EXISTS {safe_name}"
            )
            self.logger.info(
                f"IcebergWriter: Database {safe_name} ready"
            )
        except Exception as e:
            self.logger.warning(f"IcebergWriter: Could not create database: {str(e)}")

    def _table_exists(self, full_table_name: str) -> bool:
        """Check if an Iceberg table exists."""
        try:
            self.spark.sql(f"DESCRIBE TABLE {full_table_name}")
            return True
        except Exception:
            return False

    def _create_table(
        self, df: Any, full_table_name: str, target_def: TargetDef
    ) -> None:
        """Create a new Iceberg table."""
        self.logger.info(f"IcebergWriter: Creating new table {full_table_name}")
        writer = df.writeTo(full_table_name)
        writer = self._apply_partitioning(writer, target_def)
        writer = writer.tableProperty("format-version", "2")
        writer.create()

    def _overwrite_table(
        self, df: Any, full_table_name: str, target_def: TargetDef
    ) -> None:
        """Overwrite an existing Iceberg table."""
        self.logger.info(f"IcebergWriter: Overwriting table {full_table_name}")
        writer = df.writeTo(full_table_name)
        writer = self._apply_partitioning(writer, target_def)
        writer = writer.tableProperty("format-version", "2")
        writer.createOrReplace()

    def _append_to_table(self, df: Any, full_table_name: str) -> None:
        """Append data to an existing Iceberg table."""
        self.logger.info(f"IcebergWriter: Appending to table {full_table_name}")
        df.writeTo(full_table_name).tableProperty("format-version", "2").append()

    def _apply_partitioning(self, writer: Any, target_def: TargetDef) -> Any:
        """Apply partition columns to the Iceberg writer."""
        if target_def.partition_by:
            partition_cols = target_def.partition_by
            if isinstance(partition_cols, str):
                partition_cols = [partition_cols]
            for col in partition_cols:
                validate_identifier(col, "partition_by column")
                writer = writer.partitionedBy(col)
                self.logger.info(f"IcebergWriter: Added partition column: {col}")
        return writer

    def _log_table_stats(self, full_table_name: str) -> None:
        """Log row count of the Iceberg table after write."""
        try:
            stats_df = self.spark.sql(
                f"SELECT COUNT(*) as row_count FROM {full_table_name}"
            )
            row_count = stats_df.collect()[0]["row_count"]
            self.logger.info(
                f"IcebergWriter: Final row count in {full_table_name}: {row_count}"
            )
        except Exception as e:
            self.logger.warning(
                f"IcebergWriter: Could not get table stats: {str(e)}"
            )
