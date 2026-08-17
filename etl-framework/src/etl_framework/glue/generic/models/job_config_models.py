"""
Core data models for the ETL Framework.

Defines the dataclasses used throughout the framework for job context,
source/target definitions, and job configuration.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class JobContext:
    """
    Encapsulates the runtime context for an ETL job execution.

    Contains all shared state needed by readers, writers, and utilities
    during a single job run.
    """

    # Logging
    logger: Any

    # Job identification
    job_name: str
    environment: str
    config_key: str

    # AWS context
    aws_region: str
    glue_s3_bucket: str
    ingestion_s3_bucket: str

    # Spark context
    spark_context: Any
    spark_session: Any

    # Processing parameters
    chunk_size: int = 5000

    # DynamoDB table names
    table_name_watermark: str = ""

    # Timing
    job_start_time: Optional[datetime] = None

    # CloudWatch metrics client (optional)
    cloudwatch_client: Optional[Any] = None

    # Glue context (optional, for DynamicFrame operations)
    glue_context: Optional[Any] = None

    # Pending watermarks to commit after successful target writes
    pending_watermarks: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DateParameters:
    """
    Computed date parameters available to readers/writers for
    date-based partitioning and filtering.
    """

    run_date: str  # YYYY-MM-DD
    run_hour: str  # HH
    run_datetime: str  # YYYY-MM-DD HH:MM:SS
    path_date_format: str  # YYYY/MM/DD (for S3 paths)
    prev_month_end: str  # Last day of previous month (YYYY-MM-DD)
    prev_quarter_end: str  # Last day of previous quarter (YYYY-MM-DD)
    year: str  # YYYY
    month: str  # MM
    day: str  # DD


@dataclass
class SourceDef:
    """
    Defines a source dataset to read from.

    Created from the JSON job configuration 'source_list' entries.
    """

    source_key: str
    source_type: str
    create_temp_view: bool = True

    # S3 source fields
    source_s3_bucket: Optional[str] = None
    source_s3_path: Optional[str] = None
    source_format: Optional[str] = None
    source_format_options: Optional[Dict[str, Any]] = None

    # Glue Catalog / Iceberg source fields
    database_name: Optional[str] = None
    table_name: Optional[str] = None
    catalog_name: Optional[str] = None

    # Redshift source fields
    source_redshift_secrets_arn: Optional[str] = None
    source_redshift_arn: Optional[str] = None
    source_sql: Optional[str] = None
    source_connection_name: Optional[str] = None

    # DynamoDB source fields
    source_dynamodb_table: Optional[str] = None

    # API source fields
    source_api_url: Optional[str] = None
    source_api_method: Optional[str] = None
    source_api_resource_path: Optional[str] = None
    source_api_secrets_arn: Optional[str] = None
    source_api_headers: Optional[Dict[str, str]] = None
    source_api_params: Optional[Dict[str, str]] = None
    source_api_pagination: Optional[Dict[str, Any]] = None

    # Cross-account access
    cross_account_role_arn: Optional[str] = None

    # Watermark tracking for incremental loads
    watermark_strategy: Optional[str] = None
    watermark_column: Optional[str] = None
    table_name_watermark: Optional[str] = None

    # Filtering / transformation
    source_filter: Optional[str] = None
    source_select_columns: Optional[List[str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SourceDef":
        """
        Create a SourceDef from a dictionary (JSON config entry).

        Args:
            data: Dictionary containing source configuration fields

        Returns:
            SourceDef instance
        """
        return cls(
            source_key=data.get("source_key", ""),
            source_type=data.get("source_type", "UNKNOWN"),
            create_temp_view=data.get("create_temp_view", True),
            # S3
            source_s3_bucket=data.get("source_s3_bucket"),
            source_s3_path=data.get("source_s3_path"),
            source_format=data.get("source_format"),
            source_format_options=data.get("source_format_options"),
            # Glue / Iceberg
            database_name=data.get("database_name"),
            table_name=data.get("table_name"),
            catalog_name=data.get("catalog_name"),
            # Redshift
            source_redshift_secrets_arn=data.get("source_redshift_secrets_arn"),
            source_redshift_arn=data.get("source_redshift_arn"),
            source_sql=data.get("source_sql"),
            source_connection_name=data.get("source_connection_name"),
            # DynamoDB
            source_dynamodb_table=data.get("source_dynamodb_table"),
            # API
            source_api_url=data.get("source_api_url"),
            source_api_method=data.get("source_api_method"),
            source_api_resource_path=data.get("source_api_resource_path"),
            source_api_secrets_arn=data.get("source_api_secrets_arn"),
            source_api_headers=data.get("source_api_headers"),
            source_api_params=data.get("source_api_params"),
            source_api_pagination=data.get("source_api_pagination"),
            # Cross-account
            cross_account_role_arn=data.get("cross_account_role_arn"),
            # Watermark
            watermark_strategy=data.get("watermark_strategy"),
            watermark_column=data.get("watermark_column"),
            table_name_watermark=data.get("table_name_watermark"),
            # Filtering
            source_filter=data.get("source_filter"),
            source_select_columns=data.get("source_select_columns"),
        )


@dataclass
class TargetDef:
    """
    Defines a target dataset to write to.

    Created from the JSON job configuration 'target_list' entries.
    """

    target_type: str
    source_config_key: str = ""

    # Database / table identifiers
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    catalog_name: Optional[str] = None

    # S3 target fields
    target_s3_path: Optional[str] = None
    target_format: Optional[str] = None
    target_format_options: Optional[Dict[str, Any]] = None
    partition_by: Optional[Any] = None
    max_records_per_file: Optional[int] = None
    compression: Optional[str] = None

    # Redshift target fields
    target_redshift_secrets_arn: Optional[str] = None
    target_redshift_arn: Optional[str] = None
    target_connection_name: Optional[str] = None
    tempdir: Optional[str] = None
    pre_actions: Optional[str] = None
    post_actions: Optional[str] = None

    # DynamoDB target fields
    target_dynamodb_table: Optional[str] = None

    # Write behavior
    write_mode: str = "overwrite"
    internal_record_id: bool = False

    # Column selection / aliasing
    target_select: Optional[List[str]] = None
    column_aliases: Optional[Dict[str, str]] = None

    # Cross-account access
    cross_account_role_arn: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TargetDef":
        """
        Create a TargetDef from a dictionary (JSON config entry).

        Args:
            data: Dictionary containing target configuration fields

        Returns:
            TargetDef instance
        """
        return cls(
            target_type=data.get("target_type", "UNKNOWN"),
            source_config_key=data.get("source_config_key", ""),
            # Database / table
            database_name=data.get("database_name"),
            schema_name=data.get("schema_name"),
            table_name=data.get("table_name"),
            catalog_name=data.get("catalog_name"),
            # S3
            target_s3_path=data.get("target_s3_path") or data.get("s3_path"),
            target_format=data.get("target_format"),
            target_format_options=data.get("target_format_options"),
            partition_by=data.get("partition_by"),
            max_records_per_file=data.get("max_records_per_file"),
            compression=data.get("compression"),
            # Redshift
            target_redshift_secrets_arn=data.get("target_redshift_secrets_arn"),
            target_redshift_arn=data.get("target_redshift_arn"),
            target_connection_name=data.get("target_connection_name"),
            tempdir=data.get("tempdir"),
            pre_actions=data.get("pre_actions"),
            post_actions=data.get("post_actions"),
            # DynamoDB
            target_dynamodb_table=data.get("target_dynamodb_table"),
            # Write behavior
            write_mode=data.get("write_mode", "overwrite"),
            internal_record_id=data.get("internal_record_id", False),
            # Columns
            target_select=data.get("target_select"),
            column_aliases=data.get("column_aliases"),
            # Cross-account
            cross_account_role_arn=data.get("cross_account_role_arn"),
        )


@dataclass
class EnrichmentConfig:
    """Configuration for broadcast join enrichment."""

    enabled: bool = False
    enrichments: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> Optional["EnrichmentConfig"]:
        """Create from dictionary, returning None if data is None or not enabled."""
        if not data:
            return None
        return cls(
            enabled=data.get("enabled", False),
            enrichments=data.get("enrichments", []),
        )


@dataclass
class ValidationConfig:
    """Configuration for data validation."""

    enabled: bool = False
    validators: List[Dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> Optional["ValidationConfig"]:
        """Create from dictionary, returning None if data is None or not enabled."""
        if not data:
            return None
        return cls(
            enabled=data.get("enabled", False),
            validators=data.get("validators", []),
        )


@dataclass
class JobConfig:
    """
    Top-level job configuration loaded from DynamoDB.

    Contains the complete definition of an ETL job including
    sources to read, targets to write, and optional enrichment/validation.
    """

    config_key: str = ""
    source_list: Optional[List[Dict[str, Any]]] = None
    target_list: Optional[List[Dict[str, Any]]] = None
    enrichment_config: Optional[EnrichmentConfig] = None
    validation_config: Optional[ValidationConfig] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobConfig":
        """
        Create a JobConfig from a dictionary (DynamoDB item).

        Args:
            data: Dictionary containing the full job configuration

        Returns:
            JobConfig instance
        """
        return cls(
            config_key=data.get("config_key", ""),
            source_list=data.get("source_list"),
            target_list=data.get("target_list"),
            enrichment_config=EnrichmentConfig.from_dict(
                data.get("enrichment_config")
            ),
            validation_config=ValidationConfig.from_dict(
                data.get("validation_config")
            ),
        )


@dataclass
class ProcessingMetrics:
    """Metrics collected during source/target processing."""

    records_read: int = 0
    records_written: int = 0
    records_failed: int = 0
    sources_processed: int = 0
    sources_failed: int = 0
    targets_processed: int = 0
    targets_failed: int = 0
