"""
DynamoDB Reader - Reads data from Amazon DynamoDB tables.

Uses the Spark DynamoDB connector to read entire tables or filtered
subsets into DataFrames.
"""

import traceback
from typing import Optional, Tuple

from etl_framework.glue.generic.models.job_config_models import JobContext, SourceDef
from etl_framework.glue.generic.readers.reader_interface import ReaderInterface
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_source_filter,
)


class DynamoDBReader(ReaderInterface):
    """
    Reads data from Amazon DynamoDB tables.

    Uses the DynamoDB Spark connector (or Glue DynamicFrame) to read
    table contents into a DataFrame.
    """

    def __init__(self, job_context: JobContext):
        super().__init__(job_context)

    def read(self, source_def: SourceDef) -> Tuple[bool, Optional[str]]:
        """
        Read data from a DynamoDB table and create a temporary view.

        Args:
            source_def: Source definition with source_dynamodb_table name

        Returns:
            Tuple of (has_error, temp_view_name)
        """
        try:
            table_name = source_def.source_dynamodb_table
            if not table_name:
                self.logger.error(
                    "DynamoDBReader: source_dynamodb_table is required"
                )
                return True, None

            self.logger.info(f"DynamoDBReader: Reading from table '{table_name}'")

            # Determine region for the connection
            region = self.job_context.aws_region

            # Use cross-account role if specified
            role_arn = source_def.cross_account_role_arn

            # Read using Glue DynamicFrame if available, else Spark connector
            if self.job_context.glue_context:
                self.logger.info("DynamoDBReader: Using Glue DynamicFrame")
                from awsglue.dynamicframe import DynamicFrame

                connection_options = {
                    "dynamodb.input.tableName": table_name,
                    "dynamodb.region": region,
                }

                if role_arn:
                    connection_options["dynamodb.sts.roleArn"] = role_arn

                dynamic_frame = self.job_context.glue_context.create_dynamic_frame.from_options(
                    connection_type="dynamodb",
                    connection_options=connection_options,
                )
                df = dynamic_frame.toDF()
            else:
                self.logger.info("DynamoDBReader: Using Spark DynamoDB connector")
                reader = (
                    self.spark.read.format("dynamodb")
                    .option("tableName", table_name)
                    .option("region", region)
                )
                if role_arn:
                    reader = reader.option("roleArn", role_arn)
                df = reader.load()

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
            self.logger.error(f"DynamoDBReader: Validation failed: {str(e)}")
            return True, None
        except Exception as e:
            self.logger.error(f"DynamoDBReader: Failed to read: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True, None
