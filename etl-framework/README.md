# ETL Framework

Core Python library for the AWS Glue ETL Framework. This package provides:

- Configuration-driven ETL job orchestration
- Pluggable reader/writer registry for data source extensibility
- Built-in readers for S3, Glue Catalog, Iceberg, Redshift, and DynamoDB
- Built-in writers for S3, Glue Catalog, Iceberg, Redshift, and DynamoDB
- Broadcast join enrichment
- CloudWatch metrics publishing
- DynamoDB-backed watermark tracking for incremental loads
- Data validation framework

## Installation

```bash
pip install -r requirements.txt
python setup.py bdist_wheel
```

## Usage

This package is deployed as a wheel to AWS Glue jobs via the CDK infrastructure.
See the top-level [README](../README.md) for full usage instructions.

## Development

```bash
pip install -e ".[dev]"
pytest tests/
```
