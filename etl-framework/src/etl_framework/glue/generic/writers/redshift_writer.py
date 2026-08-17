"""
Redshift Writer - Writes data to Amazon Redshift.

Supports both provisioned Redshift clusters and Redshift Serverless.
Uses JDBC connectivity with Secrets Manager for credentials.
Supports pre/post SQL actions for table management.
"""

import json
import traceback
from typing import Any, Optional

import boto3

from etl_framework.glue.generic.models.job_config_models import JobContext, TargetDef
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_identifier,
    validate_pre_post_actions,
)
from etl_framework.glue.generic.writers.writer_interface import WriterInterface


class RedshiftWriter(WriterInterface):
    """
    Writes DataFrames to Amazon Redshift.

    Supports:
    - JDBC-based writes
    - Pre/post SQL actions (TRUNCATE, DELETE, etc.)
    - Configurable write modes (overwrite, append)
    - Temporary S3 staging for COPY operations
    - Cross-account IAM role for S3 access
    """

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def write(
        self,
        df: Any,
        target_def: TargetDef,
        raw_config: Optional[dict] = None,
    ) -> bool:
        """
        Write DataFrame to Redshift.

        Args:
            df: PySpark DataFrame to write
            target_def: Target definition with Redshift connection details
            raw_config: Optional raw config with additional options

        Returns:
            True if write failed, False on success
        """
        try:
            raw_config = raw_config or {}

            # Validate required fields
            if not target_def.target_redshift_secrets_arn:
                self.logger.error(
                    "RedshiftWriter: target_redshift_secrets_arn is required"
                )
                return True

            # Retrieve credentials
            credentials = self._get_redshift_credentials(
                target_def.target_redshift_secrets_arn
            )
            if not credentials:
                return True

            # Build JDBC URL
            host = credentials.get("host")
            port = credentials.get("port", 5439)
            database = credentials.get("dbname") or credentials.get("database", "dev")
            jdbc_url = f"jdbc:redshift://{host}:{port}/{database}"

            self.logger.info(
                f"RedshiftWriter: Writing to {host}:{port}/{database} "
                f"table={target_def.schema_name}.{target_def.table_name}"
            )

            # Determine write mode
            write_mode = target_def.write_mode or "overwrite"

            # Determine full table name
            full_table_name = target_def.table_name
            if target_def.schema_name:
                # Validate identifiers to prevent SQL injection
                validate_identifier(target_def.schema_name, "schema_name")
                validate_identifier(target_def.table_name, "table_name")
                full_table_name = f"{target_def.schema_name}.{target_def.table_name}"
            else:
                validate_identifier(target_def.table_name, "table_name")

            # Determine temp directory for staging
            tempdir = target_def.tempdir or raw_config.get("tempdir")
            if not tempdir:
                tempdir = (
                    f"s3://{self.job_context.glue_s3_bucket}/temp/redshift/"
                    f"{target_def.table_name}/"
                )

            # Build pre-actions SQL
            pre_actions = target_def.pre_actions or ""
            if write_mode == "overwrite" and not pre_actions:
                pre_actions = f"TRUNCATE TABLE {full_table_name};"

            # Validate pre-actions to prevent dangerous SQL injection
            if pre_actions:
                validate_pre_post_actions(pre_actions, "pre_actions")

            # Build post-actions SQL
            post_actions = target_def.post_actions or ""

            # Validate post-actions to prevent dangerous SQL injection
            if post_actions:
                validate_pre_post_actions(post_actions, "post_actions")

            # Write to Redshift
            writer = (
                df.write.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", full_table_name)
                .option("user", credentials.get("username"))
                .option("password", credentials.get("password"))
                .option("driver", "com.amazon.redshift.jdbc42.Driver")
                .mode("append")  # Use append with pre-actions for better control
            )

            # Add pre/post actions
            if pre_actions:
                writer = writer.option("preactions", pre_actions)
                self.logger.info(f"RedshiftWriter: Pre-actions: {pre_actions}")
            if post_actions:
                writer = writer.option("postactions", post_actions)
                self.logger.info(f"RedshiftWriter: Post-actions: {post_actions}")

            # Add temp directory for S3 staging
            if tempdir:
                writer = writer.option("tempdir", tempdir)

            # Add IAM role for S3 access if specified
            iam_role = raw_config.get("iam_role") or target_def.target_redshift_arn
            if iam_role:
                writer = writer.option("aws_iam_role", iam_role)

            writer.save()
            self.logger.info("RedshiftWriter: Write successful")
            return False

        except SQLValidationError as e:
            self.logger.error(f"RedshiftWriter: Validation failed: {str(e)}")
            return True
        except Exception as e:
            self.logger.error(f"RedshiftWriter: Failed to write: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True

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
                f"RedshiftWriter: Failed to get credentials: {str(e)}"
            )
            return None
