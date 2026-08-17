# Security

## Reporting a Vulnerability

If you discover a potential security issue in this project, we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## AWS Services Used

This project deploys and orchestrates the following AWS services:

- **AWS Glue** — Configuration-driven ETL jobs (source read, enrichment, target write)
- **Amazon S3** — Glue script/wheel storage, data ingestion landing zone, job logs
- **AWS Glue Data Catalog** — Metadata management for Glue/Iceberg tables
- **Amazon DynamoDB** — Job configuration storage and incremental-load watermark tracking
- **Amazon Redshift** — Optional source/target for JDBC-based reads and writes
- **AWS KMS** — Customer-managed encryption keys for S3 and DynamoDB
- **AWS IAM** — Glue execution roles, Lambda execution roles, cross-account access
- **AWS Secrets Manager** — Storage and retrieval of database/API credentials at runtime
- **AWS Lambda** — Custom resource that loads JSON job configs into DynamoDB at deploy time
- **Amazon CloudWatch** — Job metrics (duration, records processed, status) and logs
- **AWS CDK** — Infrastructure-as-code deployment of all of the above

## Prerequisites

- AWS account with appropriate permissions
- Python 3.10+
- AWS CDK v2 (`npm install -g aws-cdk`)
- Node.js 18+ (required by CDK)
- AWS CLI configured with appropriate credentials

## Known Security Considerations

### IAM Permissions

The Glue execution role (`FrameworkStack._create_glue_role`) is scoped with least privilege in most areas, but has the following broader grants that should be reviewed per deployment:

- `cloudwatch:PutMetricData` uses a `resources: ["*"]` statement. This is constrained by a `cloudwatch:namespace` condition restricting it to the `ETLFramework/Glue` namespace, but CloudWatch does not support resource-level ARNs for this action, so the wildcard is required by the service.
- `secretsmanager:GetSecretValue` / `DescribeSecret` are scoped to `arn:aws:secretsmanager:{region}:{account}:secret:*` — i.e., any secret in the deploying account/region, not just secrets used by this framework. Consider scoping this down to a specific prefix (e.g., `secret:etl-framework/*`) if your account hosts unrelated secrets.
- `external_s3_buckets` and `cross_account_role_arns` (both optional, config-driven) grant read/write S3 access and `sts:AssumeRole` respectively to whatever ARNs are listed in your environment YAML. Review these lists carefully before deploying — they are appended directly to the Glue role's policy.

### Generated Infrastructure Defaults

The following security controls are enabled by default in the CDK stack (`infra/framework_stack.py`):

- All S3 buckets (Glue, Ingestion, Log) use customer-managed **KMS encryption** with automatic key rotation enabled
- All S3 buckets **block all public access** and **enforce SSL/TLS** (`enforce_ssl=True`)
- S3 buckets are **versioned** (Glue and Ingestion buckets)
- DynamoDB tables (config, watermark) use **customer-managed KMS encryption** and **point-in-time recovery**
- DynamoDB `deletion_protection` defaults to `true` in the `prd` environment (`env_name == "prd"`), and is overridable per environment via config
- Glue JDBC connections resolve credentials from **AWS Secrets Manager** (`SECRET_ID` connection property) rather than embedding plaintext credentials in the CloudFormation template

### Input Validation (SQL Injection Prevention)

Because job behavior (table names, column names, filters, and Redshift pre/post-actions) is driven entirely by JSON configuration stored in DynamoDB, the framework treats all such values as **untrusted input**. `etl_framework/glue/generic/utils/sql_validation.py` enforces:

- **Identifier allowlisting** — table/column/temp-view names must match `^[A-Za-z_][A-Za-z0-9_]*$` (optionally dot-qualified for `catalog.database.table`), rejecting spaces, semicolons, dashes, parentheses, and comment sequences
- **Read-only SQL enforcement** — `source_sql` must start with `SELECT`/`WITH` and is scanned (with word-boundary matching to avoid false positives like `updated_at`) for DDL/DML keywords (`DROP`, `CREATE`, `ALTER`, `GRANT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `EXEC`, etc.) and rejects multiple statements separated by `;`
- **Filter expression validation** — `source_filter` blocks `UNION`, `SELECT` subqueries, `INTO`, semicolons, and inline SQL comments (`--`, `/* */`)
- **Pre/post-action allowlisting** — Redshift `pre_actions`/`post_actions` may only use `TRUNCATE`, `DELETE`, `ANALYZE`, `VACUUM`, `BEGIN`, `COMMIT`, `END`; any other keyword (including keywords hidden inside an otherwise-allowed statement) is rejected
- **Defense in depth** — validated identifiers are additionally backtick-escaped (`build_safe_qualified_name`) before interpolation into Spark SQL

### API/HTTP Reader Error Handling

`etl_framework/glue/generic/readers/api_reader.py` wraps outbound HTTP calls in explicit exception handling for `Timeout`, `ConnectionError`, and `RequestException`, and checks `response.ok` instead of relying on `raise_for_status()`. Error messages surfaced to logs/exceptions are sanitized to include only status codes and exception types — request headers, query parameters, and Secrets Manager-derived credentials are never included in raised exceptions or log output (CWE-209 mitigation).

### Credential Handling

- API and JDBC credentials are never stored in job configuration JSON or CDK context; only **Secrets Manager ARNs** are referenced (`source_api_secrets_arn`, `target_redshift_secrets_arn`, `glue_connection.secrets_arn`)
- `etl_framework/glue/generic/utils/aws.py` supports temporary, scoped credentials via `sts:assume_role` for cross-account access instead of long-lived keys
- Sample configuration files under `infra/config/sample/` and `examples/` use only placeholder account IDs (`123456789012`) and ARNs — no real credentials are committed to this repository

## Production Hardening Recommendations

Before deploying to production, consider applying the following on top of the defaults:

1. **Scope Secrets Manager access** — Replace the `secret:*` wildcard in the Glue role with a prefix scoped to this framework's secrets (e.g., `secret:etl-framework/*`).
2. **Review `external_s3_buckets` and `cross_account_role_arns`** — Audit these lists in every environment YAML before each deployment; they directly expand the Glue role's blast radius.
3. **Enable AWS CloudTrail** — Log all API calls against the S3 buckets, DynamoDB tables, and Secrets Manager secrets this framework creates/uses.
4. **Enable VPC connectivity for Glue** — Use `glue_connection` (type `NETWORK` or `JDBC`) with private subnets and restrictive security groups instead of public internet access, especially for Redshift/API sources.
5. **Enable S3 access logging** — Turn on server access logging for the ingestion and Glue script buckets.
6. **Set `dynamodb.deletion_protection: true`** explicitly for all non-dev environments, and confirm S3 bucket `removal_policy` (currently `RETAIN` for Glue/Ingestion buckets, `DESTROY` for the log bucket) matches your data retention requirements.
7. **Enable GuardDuty** — Monitor for anomalous access patterns against S3, IAM, and Secrets Manager.
8. **Review KMS key policies** — The default key grants `Encrypt`/`Decrypt` to `glue.amazonaws.com` and `lambda.amazonaws.com`; restrict further to specific role ARNs if you require tighter key governance.
9. **Rotate Secrets Manager secrets** — Enable automatic rotation for JDBC/API credentials referenced by `*_secrets_arn` fields.
10. **Tag all resources** — The stack already applies `application`, `environment`, and `managed-by` tags by default (via `app.py`); extend `tags` in your environment YAML for cost allocation and security grouping.

## Resource Cleanup

To avoid ongoing charges from deployed infrastructure:

1. Run `cdk destroy --context env=<env> --context domain=<domain> --context account=<account> --context region=<region>` from the `infra/` directory
2. Manually verify deletion/handling of resources with `RemovalPolicy.RETAIN` (these are **not** deleted by `cdk destroy` by design):
   - S3 buckets (Glue, Ingestion) — must be emptied manually before bucket deletion if versioning is enabled
   - DynamoDB tables (config, watermark)
   - KMS key (scheduled for deletion, subject to AWS's waiting period)
3. Confirm removal of:
   - IAM roles and policies created by the stack (Glue role, Config Loader Lambda role)
   - CloudWatch log groups created by Glue job runs and the Config Loader Lambda
   - Any Glue connections (`GlueConnection` / `GlueNetworkConnection`) if `create_connection` was enabled

## Dependencies

| Package | Minimum Version | Purpose |
|---------|-----------------|---------|
| boto3 | >=1.26.0 | AWS SDK for Python (S3, DynamoDB, Secrets Manager, STS, CloudWatch) |
| pyspark | >=3.3.0 | Distributed data processing within Glue jobs |
| requests | >=2.28.0 | HTTP client for the generic API reader |
| aws-cdk-lib | >=2.100.0 | Infrastructure-as-code stack definitions |
| constructs | >=10.0.0 | CDK construct base library |
| pyyaml | >=6.0 | Environment configuration file parsing |

Dependencies are pinned with minimum versions in `etl-framework/requirements.txt` and `infra/requirements.txt`. Run `pip list --outdated` regularly and monitor for CVEs against `boto3`, `pyspark`, and `requests` in particular, since these handle network I/O and credential material.

## Input Validation Summary

| Control | Location | Prevents |
|---------|----------|----------|
| SQL identifier allowlist regex | `sql_validation.validate_identifier` / `validate_qualified_identifier` | SQL injection via table/column/temp-view names |
| Read-only SQL enforcement | `sql_validation.validate_read_only_sql` | DDL/DML injection via `source_sql` |
| Filter expression validation | `sql_validation.validate_source_filter` | UNION-based injection, subquery exfiltration, comment-based bypass |
| Pre/post-action allowlist | `sql_validation.validate_pre_post_actions` | Privilege escalation and destructive operations via Redshift pre/post SQL |
| HTTP error sanitization | `readers.api_reader._fetch_with_pagination` | Credential/header exposure in stack traces (CWE-209) |
| Secrets Manager-only credentials | `framework_stack._create_glue_connection`, reader/writer secret lookups | Hardcoded credentials in CloudFormation templates and job configs (CWE-798) |
