# Configuration Guide

## Overview

The ETL Framework uses two types of configuration:

1. **Infrastructure Config** (YAML) - Defines AWS resources to create
2. **Job Config** (JSON) - Defines what each ETL job does at runtime

## Infrastructure Configuration (YAML)

Located at `infra/config/{domain}/{env}/{region}.yaml`

### Full Reference

```yaml
# Required fields
env: dev                         # Environment name (dev, stg, prd)
region: us-west-2                # AWS region
prefix: mycompany                # Resource naming prefix
account_id: "123456789012"       # AWS account ID

# Stack deployment flags
deploy_generic_etl: true         # Deploy the FrameworkStack

# DynamoDB settings
dynamodb:
  deletion_protection: false     # Enable for production

# Glue job defaults
glue:
  glue_version: "4.0"           # Glue version (3.0, 4.0, 5.0)
  max_retries: 0                # Max retry attempts
  execution_timeout: 90          # Timeout in minutes

# Resource tags (applied to all resources)
tags:
  application: "etl-framework"
  team: "data-engineering"
  cost-center: "1234"

# External S3 buckets for Glue access (optional)
external_s3_buckets:
  - "partner-data-bucket"
  - "shared-analytics-bucket"

# Cross-account IAM roles (optional)
cross_account_role_arns:
  - "arn:aws:iam::999888777666:role/data-access-role"

# Glue connection for VPC access (optional)
create_connection: true
glue_connection:
  type: NETWORK                  # NETWORK or JDBC
  subnet_id: "subnet-0abc123"
  security_group_ids:
    - "sg-0def456"
  availability_zone: "us-west-2a"
  # For JDBC type: provide Secrets Manager ARN (never hardcode credentials)
  secrets_arn: "arn:aws:secretsmanager:us-west-2:123456789012:secret:jdbc-creds"
  jdbc_url: "jdbc:redshift://cluster.region.redshift.amazonaws.com:5439/db"
```

## Job Configuration (JSON)

Located at `infra/config/{domain}/{env}/job_configs/{region}/*.json`

### Source Configuration

```json
{
  "source_key": "unique_source_name",
  "source_type": "S3|GLUE|ICEBERG|REDSHIFT|DYNAMODB|API",
  "create_temp_view": true,

  "_comment_s3": "S3 source fields",
  "source_s3_bucket": "bucket-name",
  "source_s3_path": "path/to/data/",
  "source_format": "parquet|csv|json|orc|avro",
  "source_format_options": {
    "header": "true",
    "inferSchema": "true",
    "delimiter": ","
  },

  "_comment_glue": "Glue/Iceberg source fields",
  "database_name": "my_database",
  "table_name": "my_table",
  "catalog_name": "iceberg",

  "_comment_redshift": "Redshift source fields",
  "source_redshift_secrets_arn": "arn:aws:secretsmanager:...",
  "source_sql": "SELECT * FROM schema.table WHERE date > '2024-01-01'",

  "_comment_dynamodb": "DynamoDB source fields",
  "source_dynamodb_table": "my-table-name",

  "_comment_api": "API source fields",
  "source_api_url": "https://api.example.com",
  "source_api_method": "GET",
  "source_api_resource_path": "/v1/data",
  "source_api_secrets_arn": "arn:aws:secretsmanager:...",
  "source_api_headers": {"Accept": "application/json"},
  "source_api_params": {"limit": "100"},
  "source_api_pagination": {
    "type": "offset|cursor|none",
    "page_size": 100,
    "data_path": "results",
    "max_pages": 50
  },

  "_comment_cross_account": "Cross-account access",
  "cross_account_role_arn": "arn:aws:iam::...:role/...",

  "_comment_watermark": "Incremental load tracking",
  "watermark_strategy": "timestamp|sequence|file_path|none",
  "watermark_column": "updated_at",
  "table_name_watermark": "custom_watermark_key",

  "_comment_filtering": "Data filtering",
  "source_filter": "status = 'active' AND amount > 0",
  "source_select_columns": ["col1", "col2", "col3"]
}
```

### Target Configuration

```json
{
  "target_type": "S3|GLUE|ICEBERG|REDSHIFT|REDSHIFT_SERVERLESS|DYNAMODB",
  "source_config_key": "source_key_to_read_from",

  "_comment_table": "Table identification",
  "database_name": "my_database",
  "schema_name": "my_schema",
  "table_name": "my_table",
  "catalog_name": "iceberg",

  "_comment_s3": "S3 target fields",
  "target_s3_path": "s3://bucket/path/",
  "target_format": "parquet|csv|json|orc|avro",
  "target_format_options": {},
  "partition_by": ["date_col", "region"],
  "max_records_per_file": 1000000,
  "compression": "snappy|gzip|zstd|lz4",

  "_comment_redshift": "Redshift target fields",
  "target_redshift_secrets_arn": "arn:aws:secretsmanager:...",
  "target_redshift_arn": "arn:aws:iam::...:role/redshift-access",
  "tempdir": "s3://bucket/temp/",
  "pre_actions": "TRUNCATE TABLE schema.table;",
  "post_actions": "ANALYZE schema.table;",

  "_comment_dynamodb": "DynamoDB target fields",
  "target_dynamodb_table": "my-output-table",

  "_comment_write": "Write behavior",
  "write_mode": "overwrite|append",
  "internal_record_id": false,

  "_comment_columns": "Column manipulation",
  "target_select": ["col1", "col2", "col3"],
  "column_aliases": {"old_name": "new_name"},

  "_comment_cross_account": "Cross-account access",
  "cross_account_role_arn": "arn:aws:iam::...:role/..."
}
```

### Enrichment Configuration

```json
{
  "enrichment_config": {
    "enabled": true,
    "enrichments": [
      {
        "source_key": "which_source_to_enrich",
        "lookups": [
          {
            "lookup_key": "unique_lookup_name",
            "lookup_s3_path": "s3://bucket/lookups/data.parquet",
            "lookup_format": "parquet|csv|json",
            "join_column": "column_to_join_on",
            "select_columns": ["col_a", "col_b"],
            "join_type": "left|inner|outer"
          }
        ]
      }
    ]
  }
}
```

## Job Definitions (Python)

Located at `infra/config/{domain}/{env}/job_configs/{region}/config.py`

```python
JOBS: Dict[str, JobConfig] = {
    "CONFIG_KEY": JobConfig(
        name="job-name",               # Used in Glue job name
        config_file_name="file.json",  # JSON config file name
        worker_type="G.1X",            # G.1X, G.2X, G.4X, G.8X
        number_of_workers=2,           # Number of DPUs
        description="Description",
        schedule="cron(0 6 * * ? *)",  # AWS cron expression (optional)
        skip_schedule=False,           # Set True for manual-only jobs
        chunk_size=5000,               # Processing batch size
        timeout=90,                    # Timeout in minutes
    ),
}
```
