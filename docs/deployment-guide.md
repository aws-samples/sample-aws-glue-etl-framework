# Deployment Guide

## Prerequisites

- **AWS Account** with appropriate permissions
- **Python 3.10+** installed locally
- **Node.js 18+** for AWS CDK
- **AWS CLI** configured with credentials
- **AWS CDK** installed (`npm install -g aws-cdk`)

## Initial Setup

### 1. Clone the Repository

```bash
git clone https://github.com/aws-samples/aws-glue-etl-framework.git
cd aws-glue-etl-framework
```

### 2. Create Your Configuration

```bash
# Create your domain directory
mkdir -p infra/config/myproject/dev/job_configs/us-west-2

# Copy the sample config
cp infra/config/sample/dev/us-west-2.yaml infra/config/myproject/dev/us-west-2.yaml
```

Edit `infra/config/myproject/dev/us-west-2.yaml`:

```yaml
env: dev
region: us-west-2
prefix: mycompany           # Your resource naming prefix
account_id: "123456789012"  # Your AWS account ID

deploy_generic_etl: true

glue:
  glue_version: "4.0"
  max_retries: 0
  execution_timeout: 90

tags:
  application: "etl-framework"
  team: "data-engineering"
  cost-center: "engineering"
```

### 3. Define Your Jobs

Create `infra/config/myproject/dev/job_configs/us-west-2/config.py`:

```python
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class JobConfig:
    name: str
    config_file_name: str
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    description: str = ""
    schedule: Optional[str] = None
    skip_schedule: Optional[bool] = None
    timeout: int = 90

JOBS: Dict[str, JobConfig] = {
    "MY_FIRST_JOB": JobConfig(
        name="my-first-job",
        config_file_name="my-job-config.json",
        worker_type="G.1X",
        number_of_workers=2,
        description="My first ETL job",
    ),
}
```

Create the JSON job config at `infra/config/myproject/dev/job_configs/us-west-2/my-job-config.json`:

```json
{
  "config_key": "MY_FIRST_JOB",
  "source_list": [...],
  "target_list": [...]
}
```

### 4. Build the ETL Framework Wheel

```bash
./build-wheel.sh
```

This produces `infra/assets/wheels/ETLFramework-*.whl`.

## Deployment

### First-Time Bootstrap

If this is a new AWS account/region for CDK:

```bash
cd infra
cdk bootstrap aws://123456789012/us-west-2
```

### Deploy

```bash
cd infra
pip install -r requirements.txt

cdk deploy --all \
  --context env=dev \
  --context domain=myproject \
  --context account=123456789012 \
  --context region=us-west-2
```

### Preview Changes (Diff)

```bash
cdk diff \
  --context env=dev \
  --context domain=myproject \
  --context account=123456789012 \
  --context region=us-west-2
```

## Multi-Environment Setup

### Directory Structure

```
infra/config/myproject/
├── dev/
│   ├── us-west-2.yaml
│   └── job_configs/us-west-2/
│       ├── config.py
│       └── *.json
├── stg/
│   ├── us-west-2.yaml
│   └── job_configs/us-west-2/
└── prd/
    ├── us-west-2.yaml
    └── job_configs/us-west-2/
```

### Deploy to Different Environments

```bash
# Development
cdk deploy --context env=dev --context domain=myproject --context account=111111111111 --context region=us-west-2

# Staging
cdk deploy --context env=stg --context domain=myproject --context account=222222222222 --context region=us-west-2

# Production
cdk deploy --context env=prd --context domain=myproject --context account=333333333333 --context region=us-west-2
```

## CI/CD with GitHub Actions

### Setup OIDC Authentication

1. Create an IAM OIDC provider for GitHub Actions in your AWS account
2. Create a deployment role with CDK deploy permissions
3. Configure GitHub repository environment variables:
   - Set `DEPLOY_ROLE_ARN` as a repository variable per environment

### Automated Deployment

The included `.github/workflows/deploy.yml` provides manual deployment via workflow dispatch with environment, domain, and region selection.

## Running Jobs

After deployment, jobs can be triggered:

### Manual Trigger (Console)

1. Go to AWS Glue Console
2. Navigate to Jobs
3. Find your job (e.g., `mycompany-dev-my-first-job`)
4. Click "Run"

### Manual Trigger (CLI)

```bash
aws glue start-job-run --job-name mycompany-dev-my-first-job
```

### Scheduled (via Glue Trigger)

Add a `schedule` to your JobConfig in `config.py`:

```python
JOBS = {
    "MY_SCHEDULED_JOB": JobConfig(
        name="scheduled-job",
        config_file_name="scheduled-config.json",
        schedule="cron(0 6 * * ? *)",  # Daily at 6 AM UTC
    ),
}
```

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `CDK bootstrap required` | Run `cdk bootstrap aws://ACCOUNT/REGION` |
| `Wheel not found` | Run `./build-wheel.sh` before deploying |
| `Config not found` | Ensure YAML exists at `config/{domain}/{env}/{region}.yaml` |
| `Job fails with ModuleNotFoundError` | Verify wheel is deployed to S3 and `--extra-py-files` is set |
| `DynamoDB config not loaded` | Check Lambda custom resource logs in CloudWatch |
| `Redshift connection fails` | Verify Glue network connection and security groups |

### Viewing Logs

- **Glue Job Logs**: CloudWatch Logs → `/aws-glue/jobs/output/`
- **Config Loader Lambda**: CloudWatch Logs → `/aws/lambda/{prefix}-{env}-config-loader`
- **CDK Deploy Output**: Check terminal output for resource ARNs
