# Architecture Guide

## Overview

The AWS Glue ETL Framework is a configuration-driven platform for building data pipelines on AWS. It separates **infrastructure** (CDK stacks) from **pipeline logic** (Python ETL library), enabling rapid deployment of new ETL jobs via JSON configuration without code changes.

## High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                         CDK Deployment                               │
│  ┌────────────┐  ┌────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │   KMS Key  │  │  S3 Buckets│  │   DynamoDB   │  │ Glue Jobs  │  │
│  │            │  │  (3 total) │  │  (2 tables)  │  │ (per config)│  │
│  └────────────┘  └────────────┘  └──────────────┘  └────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │ Lambda Config Loader (Custom Resource)                          │  │
│  │ Loads JSON configs → DynamoDB at deploy time                    │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│                    Glue Job Runtime                                   │
│                                                                      │
│  ┌──────────┐     ┌─────────────┐     ┌──────────────┐              │
│  │ DynamoDB │────>│JobConfig    │────>│ SourceReader  │              │
│  │ (config) │     │ loader      │     │ (registry)    │              │
│  └──────────┘     └─────────────┘     └──────┬───────┘              │
│                                               │                      │
│                                               ▼                      │
│                                      ┌────────────────┐              │
│                                      │ Temp Views     │              │
│                                      │ (Spark SQL)    │              │
│                                      └───────┬────────┘              │
│                                              │                       │
│                                              ▼                       │
│                                     ┌─────────────────┐              │
│                                     │ BroadcastEnricher│ (optional)  │
│                                     └────────┬────────┘              │
│                                              │                       │
│                                              ▼                       │
│                                     ┌─────────────────┐              │
│                                     │ TargetWriter    │              │
│                                     │ (registry)      │              │
│                                     └────────┬────────┘              │
│                                              │                       │
│                                              ▼                       │
│                                     ┌─────────────────┐              │
│                                     │ Watermark       │              │
│                                     │ Commit          │              │
│                                     └─────────────────┘              │
└──────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Plugin Registry (`registry.py`)

The registry system decouples the orchestrators from specific reader/writer implementations:

```python
ReaderRegistry.register("MY_TYPE", MyReaderClass)
WriterRegistry.register("MY_TYPE", MyWriterClass)
```

**How it works:**
- `SourceReader` looks up `source_def.source_type` in `ReaderRegistry`
- `TargetWriter` looks up `target_def.target_type` in `WriterRegistry`
- If no match is found, an error is logged with available types
- Users extend the framework by registering custom classes

### 2. Job Configuration (DynamoDB)

Jobs are defined as JSON documents stored in DynamoDB:

```json
{
  "config_key": "JOB_NAME",
  "source_list": [...],
  "target_list": [...],
  "enrichment_config": {...}
}
```

The CDK stack deploys a Lambda custom resource that loads JSON files from the `config/` directory into DynamoDB at deploy time.

### 3. Source Readers

Each reader implements `ReaderInterface`:
- Receives a `SourceDef` with connection parameters
- Returns `(has_error: bool, temp_view_name: str | None)`
- Creates a Spark temporary view for downstream processing

**Built-in readers:** S3, Glue Catalog, Iceberg, Redshift, DynamoDB, API

### 4. Target Writers

Each writer implements `WriterInterface`:
- Receives a DataFrame, `TargetDef`, and optional raw config
- Returns `has_error: bool`
- Handles format-specific writing logic

**Built-in writers:** S3, Glue Catalog, Iceberg, Redshift, DynamoDB

### 5. Watermark System

Enables incremental data loading:
1. Source defines `watermark_strategy` and `watermark_column`
2. During read, `SourceReader` tracks the max value of the watermark column
3. After successful target writes, watermarks are committed to DynamoDB
4. On next run, readers can query the watermark table to filter for new data

### 6. Enrichment

Optional broadcast join enrichment:
1. Load small lookup datasets (CSV, Parquet) into memory
2. Broadcast-join with large source DataFrames
3. Replace original temp views with enriched versions

## Data Flow

```
1. CDK deploys infrastructure + loads configs to DynamoDB
2. Glue job starts (scheduled or manual)
3. generic_glue_job.py resolves parameters
4. Reads JobConfig from DynamoDB
5. For each source:
   a. Look up reader class from ReaderRegistry
   b. Instantiate reader with JobContext
   c. Call reader.read(source_def)
   d. Result: named Spark temp view
6. For each source with enrichment:
   a. Load lookup datasets
   b. Broadcast join with source data
   c. Replace temp view with enriched version
7. For each target:
   a. Look up writer class from WriterRegistry
   b. Read DataFrame from temp view
   c. Apply column selection/aliases
   d. Call writer.write(df, target_def)
8. Commit watermarks for sources whose targets all succeeded
9. Publish metrics to CloudWatch
```

## Infrastructure Resources

| Resource | Naming Pattern | Purpose |
|----------|---------------|---------|
| KMS Key | `{prefix}-{env}-encryption` | Encrypt all data at rest |
| S3 (Glue) | `{prefix}-{env}-{account}-{region}-glue` | Scripts, wheels, temp files |
| S3 (Ingestion) | `{prefix}-{env}-{account}-{region}-ingestion` | Data landing zone |
| S3 (Logs) | `{prefix}-{env}-{account}-{region}-glue-logs` | Job execution logs |
| DynamoDB (Config) | `{prefix}-{env}-etl-configs` | Job configurations |
| DynamoDB (Watermark) | `{prefix}-{env}-etl-watermark` | Incremental load tracking |
| IAM Role (Glue) | `{prefix}-{env}-glue-{region}` | Glue job execution |
| Glue Jobs | `{prefix}-{env}-{job_name}` | ETL job definitions |

## Security

- **Encryption**: All S3 buckets and DynamoDB tables use customer-managed KMS keys
- **Least Privilege**: Glue role gets only necessary permissions per resource
- **Secrets**: Credentials stored in Secrets Manager, accessed at runtime
- **Network**: Optional VPC connectivity via Glue network connections
- **Cross-Account**: IAM role assumption for multi-account architectures
