"""
Configuration Utilities.

Provides functions for reading job configurations from DynamoDB
and resolving configuration parameters.
"""

import json
import logging
from typing import Any, Dict, Optional

import boto3

from etl_framework.glue.generic.models.job_config_models import JobConfig


def read_job_config(
    config_key: str,
    table_name: str,
    region_name: str,
    logger: Optional[logging.Logger] = None,
) -> JobConfig:
    """
    Read a job configuration from DynamoDB by config_key.

    The DynamoDB table stores JSON configurations with a partition key
    of 'config_key'. The full JSON item is deserialized into a JobConfig.

    Args:
        config_key: The partition key value to look up
        table_name: Name of the DynamoDB table storing configs
        region_name: AWS region where the table resides
        logger: Optional logger instance

    Returns:
        JobConfig instance populated from the DynamoDB item

    Raises:
        ValueError: If the config_key is not found in the table
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    logger.info(
        f"Reading job config: key='{config_key}', table='{table_name}', "
        f"region='{region_name}'"
    )

    dynamodb = boto3.resource("dynamodb", region_name=region_name)
    table = dynamodb.Table(table_name)

    response = table.get_item(Key={"config_key": config_key})

    if "Item" not in response:
        raise ValueError(
            f"Job configuration not found for config_key='{config_key}' "
            f"in table '{table_name}'"
        )

    item = response["Item"]
    logger.info(f"Successfully loaded config for '{config_key}'")

    # DynamoDB may store nested structures as strings - parse if needed
    for field in ("source_list", "target_list", "enrichment_config", "validation_config"):
        if field in item and isinstance(item[field], str):
            try:
                item[field] = json.loads(item[field])
            except (json.JSONDecodeError, TypeError):
                pass

    return JobConfig.from_dict(item)


def get_ssm_parameter(
    parameter_name: str,
    region_name: str,
    decrypt: bool = True,
) -> str:
    """
    Retrieve a parameter value from AWS Systems Manager Parameter Store.

    Args:
        parameter_name: The name or ARN of the parameter
        region_name: AWS region
        decrypt: Whether to decrypt SecureString parameters

    Returns:
        The parameter value as a string
    """
    client = boto3.client("ssm", region_name=region_name)
    response = client.get_parameter(
        Name=parameter_name, WithDecryption=decrypt
    )
    return response["Parameter"]["Value"]


def get_secret_value(
    secret_id: str,
    region_name: str,
) -> Dict[str, Any]:
    """
    Retrieve and parse a secret from AWS Secrets Manager.

    Args:
        secret_id: The ARN or name of the secret
        region_name: AWS region

    Returns:
        Parsed JSON dictionary from the secret value
    """
    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_id)
    return json.loads(response["SecretString"])
