"""
CloudWatch Metrics Publishing.

Provides functions for publishing custom metrics to Amazon CloudWatch
for ETL job monitoring and alerting.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3

logger = logging.getLogger(__name__)

# Default namespace for all ETL Framework metrics
DEFAULT_NAMESPACE = "ETLFramework/Glue"


def get_cloudwatch_client(region_name: str) -> Any:
    """
    Initialize and return a CloudWatch client.

    Args:
        region_name: AWS region for the CloudWatch client

    Returns:
        boto3 CloudWatch client

    Raises:
        Exception: If client initialization fails
    """
    client = boto3.client("cloudwatch", region_name=region_name)
    logger.info(f"CloudWatch client initialized for region: {region_name}")
    return client


def publish_metric(
    cloudwatch_client: Any,
    job_name: str,
    metric_name: str,
    value: float,
    namespace: str = DEFAULT_NAMESPACE,
    dimensions: Optional[List[Dict[str, str]]] = None,
    unit: str = "Count",
    timestamp: Optional[datetime] = None,
) -> bool:
    """
    Publish a single metric to CloudWatch.

    Args:
        cloudwatch_client: boto3 CloudWatch client
        job_name: Name of the ETL job (added as a dimension)
        metric_name: Name of the metric
        value: Metric value
        namespace: CloudWatch namespace (default: ETLFramework/Glue)
        dimensions: Additional dimensions as list of {"Name": ..., "Value": ...}
        unit: CloudWatch unit (Count, Seconds, Bytes, etc.)
        timestamp: Metric timestamp (defaults to now)

    Returns:
        True if metric was published successfully, False otherwise
    """
    if cloudwatch_client is None:
        logger.debug("CloudWatch client is None, skipping metric publication")
        return False

    try:
        # Build dimensions list - always include JobName
        all_dimensions = [{"Name": "JobName", "Value": job_name}]
        if dimensions:
            all_dimensions.extend(dimensions)

        metric_data = {
            "MetricName": metric_name,
            "Dimensions": all_dimensions,
            "Value": value,
            "Unit": unit,
        }

        if timestamp:
            metric_data["Timestamp"] = timestamp

        cloudwatch_client.put_metric_data(
            Namespace=namespace,
            MetricData=[metric_data],
        )

        logger.debug(
            f"Published metric: {namespace}/{metric_name}={value} "
            f"(job={job_name}, dims={dimensions})"
        )
        return True

    except Exception as e:
        logger.warning(f"Failed to publish metric '{metric_name}': {str(e)}")
        return False


def publish_job_duration(
    cloudwatch_client: Any,
    job_name: str,
    duration_seconds: float,
    environment: str,
    namespace: str = DEFAULT_NAMESPACE,
) -> bool:
    """
    Publish job duration metric.

    Args:
        cloudwatch_client: boto3 CloudWatch client
        job_name: Name of the ETL job
        duration_seconds: Job execution duration in seconds
        environment: Environment name (dev, stg, prd)
        namespace: CloudWatch namespace

    Returns:
        True if published successfully
    """
    return publish_metric(
        cloudwatch_client=cloudwatch_client,
        job_name=job_name,
        metric_name="JobDuration",
        value=duration_seconds,
        namespace=namespace,
        dimensions=[{"Name": "Environment", "Value": environment}],
        unit="Seconds",
    )


def publish_records_processed(
    cloudwatch_client: Any,
    job_name: str,
    records_count: int,
    environment: str,
    stage: str = "read",
    namespace: str = DEFAULT_NAMESPACE,
) -> bool:
    """
    Publish records processed metric.

    Args:
        cloudwatch_client: boto3 CloudWatch client
        job_name: Name of the ETL job
        records_count: Number of records processed
        environment: Environment name
        stage: Processing stage ("read" or "write")
        namespace: CloudWatch namespace

    Returns:
        True if published successfully
    """
    return publish_metric(
        cloudwatch_client=cloudwatch_client,
        job_name=job_name,
        metric_name="RecordsProcessed",
        value=records_count,
        namespace=namespace,
        dimensions=[
            {"Name": "Environment", "Value": environment},
            {"Name": "Stage", "Value": stage},
        ],
        unit="Count",
    )


def publish_job_status(
    cloudwatch_client: Any,
    job_name: str,
    status: str,
    environment: str,
    namespace: str = DEFAULT_NAMESPACE,
) -> bool:
    """
    Publish job completion status metric.

    Args:
        cloudwatch_client: boto3 CloudWatch client
        job_name: Name of the ETL job
        status: Job status ("Success" or "Failure")
        environment: Environment name
        namespace: CloudWatch namespace

    Returns:
        True if published successfully
    """
    metric_name = "JobCompleted" if status == "Success" else "JobFailed"
    return publish_metric(
        cloudwatch_client=cloudwatch_client,
        job_name=job_name,
        metric_name=metric_name,
        value=1,
        namespace=namespace,
        dimensions=[
            {"Name": "Status", "Value": status},
            {"Name": "Environment", "Value": environment},
        ],
    )
