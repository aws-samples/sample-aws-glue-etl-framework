"""
Sample Job Definitions.

Each entry defines a Glue job that will be created by the CDK stack.
The config_file_name points to a JSON file in this same directory
that defines the job's sources and targets.
"""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class JobConfig:
    """Definition of a Glue job to deploy."""
    name: str
    config_file_name: str
    worker_type: str = "G.1X"
    number_of_workers: int = 2
    description: str = ""
    schedule: Optional[str] = None
    skip_schedule: Optional[bool] = None
    chunk_size: Optional[int] = None
    timeout: int = 90


# Define your Glue jobs here
JOBS: Dict[str, JobConfig] = {
    "S3_TO_REDSHIFT_JOB": JobConfig(
        name="s3-to-redshift",
        config_file_name="s3-to-redshift-config.json",
        worker_type="G.1X",
        number_of_workers=2,
        description="Load data from S3 to Redshift",
        schedule="cron(0 6 * * ? *)",  # Daily at 6 AM UTC
    ),
    "S3_TO_ICEBERG_JOB": JobConfig(
        name="s3-to-iceberg",
        config_file_name="s3-to-iceberg-config.json",
        worker_type="G.1X",
        number_of_workers=2,
        description="Load data from S3 to Iceberg table",
        skip_schedule=True,  # Manual trigger only
    ),
    "MULTI_TARGET_JOB": JobConfig(
        name="multi-target",
        config_file_name="multi-target-config.json",
        worker_type="G.2X",
        number_of_workers=4,
        description="Read from S3 and write to multiple targets",
        skip_schedule=True,
    ),
}
