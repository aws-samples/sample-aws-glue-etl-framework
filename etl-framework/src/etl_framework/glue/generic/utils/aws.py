"""
AWS Utilities.

Provides helper functions for common AWS operations including
cross-account role assumption and service client creation.
"""

import logging
from typing import Any, Dict, Optional

import boto3
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

logger = logging.getLogger(__name__)


def assume_role(
    role_arn: str,
    session_name: str = "ETLFrameworkSession",
    region_name: Optional[str] = None,
    duration_seconds: int = 3600,
) -> boto3.Session:
    """
    Assume an IAM role and return a boto3 session with temporary credentials.

    Useful for cross-account access patterns where the ETL job needs
    to read/write resources in a different AWS account.

    Args:
        role_arn: ARN of the IAM role to assume
        session_name: Name for the assumed role session
        region_name: AWS region for the session
        duration_seconds: Duration of the temporary credentials (max 3600)

    Returns:
        A boto3.Session configured with the assumed role credentials
    """
    logger.info(f"Assuming role: {role_arn}")

    sts_client = boto3.client("sts", region_name=region_name)
    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
        DurationSeconds=duration_seconds,
    )

    credentials = response["Credentials"]
    session = boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=region_name,
    )

    logger.info(f"Successfully assumed role: {role_arn}")
    return session


def get_client_with_role(
    service_name: str,
    role_arn: str,
    region_name: Optional[str] = None,
) -> Any:
    """
    Get a boto3 client using assumed role credentials.

    Args:
        service_name: AWS service name (e.g., "s3", "dynamodb")
        role_arn: ARN of the IAM role to assume
        region_name: AWS region

    Returns:
        boto3 client configured with assumed role credentials
    """
    session = assume_role(role_arn, region_name=region_name)
    return session.client(service_name, region_name=region_name)


def get_resource_with_role(
    service_name: str,
    role_arn: str,
    region_name: Optional[str] = None,
) -> Any:
    """
    Get a boto3 resource using assumed role credentials.

    Args:
        service_name: AWS service name (e.g., "s3", "dynamodb")
        role_arn: ARN of the IAM role to assume
        region_name: AWS region

    Returns:
        boto3 resource configured with assumed role credentials
    """
    session = assume_role(role_arn, region_name=region_name)
    return session.resource(service_name, region_name=region_name)


def get_account_id(region_name: Optional[str] = None) -> str:
    """
    Get the current AWS account ID.

    Args:
        region_name: AWS region

    Returns:
        The 12-digit AWS account ID
    """
    sts_client = boto3.client("sts", region_name=region_name)
    return sts_client.get_caller_identity()["Account"]
