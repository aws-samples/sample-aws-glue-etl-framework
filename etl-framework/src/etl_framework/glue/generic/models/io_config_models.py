"""
I/O Configuration Models for the ETL Framework.

TypedDict-based models representing the structure of source and target
configurations as used in the job configuration JSON files.
These provide type hints and documentation for the expected config shape.
"""

from typing import Any, Dict, List, Optional, TypedDict


class SourceConfig(TypedDict, total=False):
    """
    TypedDict representing source configuration in JSON job configs.

    This documents the expected shape of entries in source_list.
    All fields are optional (total=False) since different source types
    use different subsets of fields.
    """

    # Identification
    source_key: str
    source_type: str
    create_temp_view: bool

    # S3 fields
    source_s3_bucket: str
    source_s3_path: str
    source_format: str
    source_format_options: Dict[str, Any]

    # Glue Catalog / Iceberg fields
    database_name: str
    table_name: str
    catalog_name: str

    # Redshift fields
    source_redshift_secrets_arn: str
    source_redshift_arn: str
    source_sql: str
    source_connection_name: str

    # DynamoDB fields
    source_dynamodb_table: str

    # API fields
    source_api_url: str
    source_api_method: str
    source_api_resource_path: str
    source_api_secrets_arn: str
    source_api_headers: Dict[str, str]
    source_api_params: Dict[str, str]
    source_api_pagination: Dict[str, Any]

    # Cross-account
    cross_account_role_arn: str

    # Watermark
    watermark_strategy: str
    watermark_column: str
    table_name_watermark: str

    # Filtering
    source_filter: str
    source_select_columns: List[str]


class TargetConfig(TypedDict, total=False):
    """
    TypedDict representing target configuration in JSON job configs.

    This documents the expected shape of entries in target_list.
    All fields are optional since different target types use different
    subsets of fields.
    """

    # Identification
    target_type: str
    source_config_key: str

    # Database / table
    database_name: str
    schema_name: str
    table_name: str
    catalog_name: str

    # S3 fields
    target_s3_path: str
    s3_path: str  # Alternative name for target_s3_path
    target_format: str
    target_format_options: Dict[str, Any]
    partition_by: Any  # str or List[str]
    max_records_per_file: int
    compression: str

    # Redshift fields
    target_redshift_secrets_arn: str
    target_redshift_arn: str
    target_connection_name: str
    tempdir: str
    pre_actions: str
    post_actions: str

    # DynamoDB fields
    target_dynamodb_table: str

    # Write behavior
    write_mode: str
    internal_record_id: bool

    # Column selection / aliasing
    target_select: List[str]
    column_aliases: Dict[str, str]

    # Cross-account
    cross_account_role_arn: str


class EnrichmentConfigDict(TypedDict, total=False):
    """TypedDict for enrichment configuration."""

    enabled: bool
    enrichments: List[Dict[str, Any]]


class ValidationConfigDict(TypedDict, total=False):
    """TypedDict for validation configuration."""

    enabled: bool
    validators: List[Dict[str, Any]]


class JobConfigDict(TypedDict, total=False):
    """
    TypedDict representing the complete job configuration JSON structure
    as stored in DynamoDB.
    """

    config_key: str
    source_list: List[SourceConfig]
    target_list: List[TargetConfig]
    enrichment_config: EnrichmentConfigDict
    validation_config: ValidationConfigDict
