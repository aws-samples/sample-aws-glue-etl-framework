"""
DynamoDB Writer - Writes data to Amazon DynamoDB tables.

Uses the Spark DynamoDB connector or Glue DynamicFrame for writing
DataFrames to DynamoDB tables with configurable throughput.
"""

import traceback
from typing import Any, Optional

from etl_framework.glue.generic.models.job_config_models import JobContext, TargetDef
from etl_framework.glue.generic.writers.writer_interface import WriterInterface


class DynamoDBWriter(WriterInterface):
    """
    Writes DataFrames to Amazon DynamoDB tables.

    Supports configurable write throughput and both Glue DynamicFrame
    and Spark DataFrame connector approaches.
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
        Write DataFrame to a DynamoDB table.

        Args:
            df: PySpark DataFrame to write
            target_def: Target definition with DynamoDB table name
            raw_config: Optional raw config with additional options

        Returns:
            True if write failed, False on success
        """
        try:
            raw_config = raw_config or {}
            table_name = target_def.target_dynamodb_table or target_def.table_name

            if not table_name:
                self.logger.error(
                    "DynamoDBWriter: target_dynamodb_table or table_name is required"
                )
                return True

            self.logger.info(f"DynamoDBWriter: Writing to table '{table_name}'")

            region = self.job_context.aws_region
            throughput = raw_config.get("dynamodb_write_throughput", "100")

            # Use Glue DynamicFrame if available
            if self.job_context.glue_context:
                self.logger.info("DynamoDBWriter: Using Glue DynamicFrame")
                from awsglue.dynamicframe import DynamicFrame

                dynamic_frame = DynamicFrame.fromDF(
                    df, self.job_context.glue_context, f"df_{table_name}"
                )

                self.job_context.glue_context.write_dynamic_frame.from_options(
                    frame=dynamic_frame,
                    connection_type="dynamodb",
                    connection_options={
                        "dynamodb.output.tableName": table_name,
                        "dynamodb.region": region,
                        "dynamodb.throughput.write.percent": throughput,
                    },
                )
            else:
                self.logger.info("DynamoDBWriter: Using Spark DynamoDB connector")
                (
                    df.write.format("dynamodb")
                    .option("tableName", table_name)
                    .option("region", region)
                    .option("writeThroughput", throughput)
                    .mode("append")
                    .save()
                )

            row_count = df.count()
            self.logger.info(
                f"DynamoDBWriter: Successfully wrote {row_count} records to '{table_name}'"
            )
            return False

        except Exception as e:
            self.logger.error(f"DynamoDBWriter: Failed to write: {str(e)}")
            self.logger.error(traceback.format_exc())
            return True
