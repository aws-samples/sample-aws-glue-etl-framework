"""
ETL Framework - A configuration-driven, extensible ETL framework built on AWS Glue.

This package provides modular components for building data pipelines:
- Readers: Read from various data sources (S3, Redshift, Glue Catalog, Iceberg, DynamoDB)
- Writers: Write to various data targets (S3, Redshift, Glue Catalog, Iceberg, DynamoDB)
- Registry: Plugin system for extending with custom readers/writers
- Enrichment: Broadcast join enrichment for data enhancement
- Metrics: CloudWatch metrics publishing
- Validators: Data quality validation
"""

__version__ = "0.1.0"
