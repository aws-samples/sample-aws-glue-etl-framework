"""
Redshift Reader - Reads data from Amazon Redshift clusters.

Supports both provisioned Redshift clusters and Redshift Serverless.
Uses JDBC connectivity with Secrets Manager for credential management.
"""

import json
import traceback
from typing import Optional, Tuple

import boto3

from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_read_only_sql,
    validate_source_filter,
)


class RedshiftReader(ReaderInterface):
    """
    Reads data from Amazon Redshift using JDBC.

    Retrieves connection credentials from AWS Secrets Manager and
    executes SQL queries against Redshift clusters or serverless endpoints.
    """

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]:
        """
        Read data from Redshift and create a temporary view.

        Args:
            source_def: Source definition with Redshift connection details,
                       including secrets ARN and SQL query

        Returns:
            Tuple of (has_error, temp_view_name)
        """
        try:
            # Validate required fields
            if not source_def.source_redshift_secrets_arn:
                self.logger.error(
                    "RedshiftReader: source_redshift_secrets_arn is required"
                )
                return True, None

            if not source_def.source_sql:
                self.logger.error("RedshiftReader: source_sql is required")
                return True, None

            # Validate SQL is read-only to prevent injection
            validate_read_only_sql(source_def.source_sql, "source_sql")

            self.logger.info(
                f"RedshiftReader: Reading with SQL: {source_def.source_sql[:100]}..."
            )

            # Retrieve credentials from Secrets Manager
            credentials = self._get_redshift_credentials(
                source_def.source_redshift_secrets_arn
            )

            if not credentials:
                self.logger.error("RedshiftReader: Failed to retrieve credentials")
                return True, None

            # Build JDBC URL
            host = credentials.get("host")
            port = credentials.get("port", 5439)
            database = credentials.get("dbname") or credentials.get("database", "dev")
            jdbc_url = f"jdbc:redshift://{host}:{port}/{database}"

            self.logger.info(f"RedshiftReader: Connecting to {host}:{port}/{database}")

            # Read from Redshift using Spark JDBC
            df = (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("user", credentials.get("username"))
                .option("password", credentials.get("password"))
                .option("query", source_def.source_sql)
                .option("driver", "com.amazon.redshift.jdbc42.Driver")
                .load()
            )

            # Apply column selection if specified
            if source_def.source_select_columns:
                df = self._validate_and_select_columns(
                    df, source_def.source_select_columns
                )

            # Apply filter if specified
            if source_def.source_filter:
                validate_source_filter(source_def.source_filter, "source_filter")
                df = df.filter(source_def.source_filter)

            # Create temp view
            temp_view_name = self._create_temp_view(df, source_def)
            return False, temp_view_name

        except SQLValidationError as e:
            self.logger.error(f"RedshiftReader: Validation failed: {str(e)}")
            return True, None
        except Exception as e:
            self.logger.error(f"RedshiftReader: Failed to read: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True, None

    def _get_redshift_credentials(self, secrets_arn: str) -> Optional[dict]:
        """
        Retrieve Redshift credentials from AWS Secrets Manager.

        Args:
            secrets_arn: ARN of the Secrets Manager secret

        Returns:
            Dictionary with connection credentials, or None on failure
        """
        try:
            client = boto3.client(
                "secretsmanager", region_name=self.job_context.aws_region
            )
            response = client.get_secret_value(SecretId=secrets_arn)
            return json.loads(response["SecretString"])
        except Exception as e:
            self.logger.error(
                f"RedshiftReader: Failed to get credentials: {str(e)}"
            )
            return None
