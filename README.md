# AWS Glue ETL Framework

A configuration-driven, extensible ETL framework built on AWS Glue and AWS CDK. Define data pipelines declaratively using JSON configurations and deploy them with zero custom code.

## Disclaimer

This project is provided as sample/educational code and is NOT intended for
production use without additional security hardening. The generated infrastructure
includes IAM permissions and resource defaults suitable for development environments only.
See [SECURITY.md](SECURITY.md) for production hardening recommendations.

## Overview

This framework provides a configurable, pluggable ETL platform that:

- **Config-driven**: Define ETL jobs via JSON — no code changes needed for new pipelines
- **Extensible**: Plugin registry system allows adding custom readers/writers without modifying core code
- **AWS-native**: Built on AWS Glue, S3, Redshift, DynamoDB, Iceberg, and CloudWatch
- **Observable**: Built-in CloudWatch metrics for job monitoring and alerting
- **Incremental**: DynamoDB-backed watermark tracking for incremental data loads
- **Multi-target**: One source can write to multiple destinations in a single job
- **Enrichable**: Optional broadcast join enrichment for lookup-based data enhancement

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    CI/CD Pipeline                            │
│   (GitHub Actions → OIDC Auth → CDK Deploy)                 │
└────────────────────────────┬────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  FrameworkStack  │ │  Optional Stacks │ │  Config Loader  │
│  (KMS, S3, DDB, │ │  (Raw loaders)  │ │  (Lambda → DDB) │
│  Glue Jobs, IAM)│ │                 │ │                 │
└────────┬────────┘ └─────────────────┘ └─────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│              generic_glue_job.py (Entry Point)               │
│                                                             │
│  DDB Config → SourceReader → [Enrichment] → TargetWriter   │
│                    │                              │          │
│         ┌──────────┴──────────┐        ┌─────────┴───────┐ │
│         │  Reader Registry    │        │ Writer Registry  │ │
│         │  S3, Redshift,      │        │ S3, Redshift,   │ │
│         │  DynamoDB, Iceberg, │        │ Glue Catalog,   │ │
│         │  Glue Catalog,      │        │ DynamoDB,       │ │
│         │  + Custom Readers   │        │ Iceberg,        │ │
│         │                     │        │ + Custom Writers │ │
│         └─────────────────────┘        └─────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.10+
- AWS CDK v2
- AWS CLI configured with appropriate credentials
- Node.js 18+ (for CDK)

### 1. Clone and Install

```bash
git clone https://github.com/aws-samples/aws-glue-etl-framework.git
cd aws-glue-etl-framework

# Build the ETL framework wheel
cd etl-framework
pip install -r requirements.txt
python setup.py bdist_wheel
cd ..
```

### 2. Configure Your Environment

```bash
# Copy sample config and customize
cp infra/config/sample/dev/us-west-2.yaml infra/config/myproject/dev/us-west-2.yaml
# Edit the YAML with your account ID, region, and preferences
```

### 3. Define a Job

Create a JSON job config (see `examples/` directory):

```json
{
  "config_key": "MY_FIRST_JOB",
  "source_list": [
    {
      "source_key": "raw_data",
      "source_type": "S3",
      "source_s3_bucket": "my-data-bucket",
      "source_s3_path": "raw/events/",
      "source_format": "parquet",
      "create_temp_view": true
    }
  ],
  "target_list": [
    {
      "target_type": "REDSHIFT",
      "source_config_key": "raw_data",
      "database_name": "analytics",
      "schema_name": "raw",
      "table_name": "events",
      "write_mode": "append"
    }
  ]
}
```

### 4. Deploy

```bash
cd infra
pip install -r requirements.txt
cdk deploy --context env=dev --context domain=myproject --context region=us-west-2 --context account=123456789012
```

## Repository Structure

```
sample-aws-glue-etl-framework/
├── etl-framework/              # Core Python library (packaged as wheel)
│   ├── src/etl_framework/
│   │   ├── glue/generic/       # Main framework code
│   │   │   ├── constants.py    # Storage type enums
│   │   │   ├── registry.py     # Plugin registry for readers/writers
│   │   │   ├── models/         # Data models (JobContext, SourceDef, TargetDef)
│   │   │   ├── readers/        # Source readers (S3, Redshift, Glue, Iceberg, DynamoDB)
│   │   │   ├── writers/        # Target writers (S3, Redshift, Glue, Iceberg, DynamoDB)
│   │   │   ├── enrichment/     # Broadcast join enrichment
│   │   │   ├── metrics/        # CloudWatch metrics
│   │   │   └── utils/          # Config, dates, logging, AWS utilities
│   │   └── validators/         # Data validation framework
│   └── tests/                  # Unit tests
├── infra/                      # CDK infrastructure
│   ├── app.py                  # CDK app entry point
│   ├── framework_stack.py      # Main ETL infrastructure stack
│   ├── config/                 # Environment configurations
│   └── src/glue/scripts/       # Glue job entry point scripts
├── examples/                   # Example job configurations
└── docs/                       # Documentation
```

## Extending the Framework

### Adding a Custom Reader

```python
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface
from etl_framework.glue.generic.registry import ReaderRegistry

class MyCustomReader(ReaderInterface):
    def read(self, source_def):
        # Your custom read logic here
        df = self.job_context.spark_session.read.format("csv").load("s3://...")
        temp_view_name = f"temp_{source_def.source_key}"
        df.createOrReplaceTempView(temp_view_name)
        return False, temp_view_name  # (has_error, view_name)

# Register your reader
ReaderRegistry.register("MY_CUSTOM_SOURCE", MyCustomReader)
```

See [docs/adding-readers.md](docs/adding-readers.md) for detailed instructions.

## Built-in Storage Types

| Type | Reader | Writer | Description |
|------|--------|--------|-------------|
| S3 | Yes | Yes | Amazon S3 (CSV, JSON, Parquet) |
| GLUE | Yes | Yes | AWS Glue Data Catalog tables |
| ICEBERG | Yes | Yes | Apache Iceberg tables |
| REDSHIFT | Yes | Yes | Amazon Redshift |
| REDSHIFT_SERVERLESS | - | Yes | Amazon Redshift Serverless |
| DYNAMODB | Yes | Yes | Amazon DynamoDB |
| API | Yes | - | Generic REST API connector |

## Documentation

- [Architecture Guide](docs/architecture.md)
- [Adding Custom Readers](docs/adding-readers.md)
- [Adding Custom Writers](docs/adding-writers.md)
- [Configuration Guide](docs/configuration-guide.md)
- [Deployment Guide](docs/deployment-guide.md)

## Security

See [SECURITY.md](SECURITY.md) for vulnerability reporting, AWS services used, known security considerations, and production hardening recommendations.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
