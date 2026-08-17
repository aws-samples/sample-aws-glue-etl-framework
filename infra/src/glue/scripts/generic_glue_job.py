"""
Generic Glue Job Entry Point.

This is the main entry point script for all ETL Framework Glue jobs.
It orchestrates the ETL process using the modular components from
the etl_framework package:

1. Initialize job context (Spark, logging, CloudWatch)
2. Load job configuration from DynamoDB
3. Read sources via SourceReader (plugin-based)
4. Optionally enrich data via BroadcastEnricher
5. Write targets via TargetWriter (plugin-based)
6. Commit watermarks for successful targets
7. Publish metrics and handle errors
"""

import sys
import traceback
from datetime import datetime
from typing import Optional

# AWS Glue imports
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ETL Framework imports
from etl_framework.glue.generic import (
    get_cloudwatch_client,
    publish_metric,
    setup_logging,
)
from etl_framework.glue.generic.enrichment.broadcast_enricher import BroadcastEnricher
from etl_framework.glue.generic.metrics.cloudwatch import (
    publish_job_duration,
    publish_job_status,
    publish_records_processed,
)
from etl_framework.glue.generic.models.job_config_models import (
    JobConfig,
    JobContext,
    ProcessingMetrics,
)
from etl_framework.glue.generic.readers.source_reader import SourceReader
from etl_framework.glue.generic.utils.config import read_job_config
from etl_framework.glue.generic.writers.target_writer import TargetWriter


def create_spark_session(glue_s3_bucket: str, region: str, account_id: str) -> SparkSession:
    """
    Create a Spark session configured for Glue with Iceberg support.

    Args:
        glue_s3_bucket: S3 bucket for Iceberg warehouse
        region: AWS region
        account_id: AWS account ID

    Returns:
        Configured SparkSession
    """
    warehouse_path = f"s3://{glue_s3_bucket}/iceberg/"

    spark = (
        SparkSession.builder.appName("ETL Framework")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.warehouse", warehouse_path)
        .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.client.region", region)
        .config("spark.sql.catalog.iceberg.glue.account-id", account_id)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    return spark


def initialize_job(args) -> JobContext:
    """
    Initialize the job context with all required components.

    Args:
        args: Resolved Glue job arguments

    Returns:
        Initialized JobContext
    """
    # Set up logging
    job_name = args["job_name"]
    logger = setup_logging(job_name)

    # Extract parameters
    environment = args["environment"]
    glue_s3_bucket = args["glue_s3_bucket"]
    ingestion_s3_bucket = args["ingestion_s3_bucket"]
    config_key = args["config_key"]
    aws_region = args["aws_region"]
    account_id = args["aws_accountid"]
    chunk_size = int(args.get("chunk_size", "5000"))
    watermark_table = args["etl_watermark_table_name"]

    logger.info("=== Job Parameters ===")
    logger.info(f"Job Name: {job_name}")
    logger.info(f"Environment: {environment}")
    logger.info(f"Config Key: {config_key}")
    logger.info(f"Region: {aws_region}")
    logger.info(f"Chunk Size: {chunk_size}")

    # Initialize Spark session
    sc = SparkContext.getOrCreate()
    spark = create_spark_session(glue_s3_bucket, aws_region, account_id)

    # Initialize CloudWatch client (optional - graceful degradation)
    cloudwatch_client = None
    try:
        cloudwatch_client = get_cloudwatch_client(aws_region)
        logger.info("CloudWatch client initialized successfully")
    except Exception as e:
        logger.warning(f"Failed to initialize CloudWatch client: {str(e)}")
        logger.warning("Metrics will not be published. Continuing with job execution.")

    return JobContext(
        logger=logger,
        job_name=job_name,
        environment=environment,
        glue_s3_bucket=glue_s3_bucket,
        ingestion_s3_bucket=ingestion_s3_bucket,
        config_key=config_key,
        aws_region=aws_region,
        chunk_size=chunk_size,
        table_name_watermark=watermark_table,
        spark_context=sc,
        spark_session=spark,
        job_start_time=datetime.utcnow(),
        cloudwatch_client=cloudwatch_client,
    )


def handle_job_success(job_context: JobContext) -> None:
    """Publish success metrics and log completion."""
    duration = (datetime.utcnow() - job_context.job_start_time).total_seconds()
    job_context.logger.info(f"Job completed successfully in {duration:.1f}s")

    if job_context.cloudwatch_client:
        publish_job_status(
            job_context.cloudwatch_client,
            job_context.job_name,
            "Success",
            job_context.environment,
        )
        publish_job_duration(
            job_context.cloudwatch_client,
            job_context.job_name,
            duration,
            job_context.environment,
        )


def handle_job_failure(job_context: JobContext, error: Exception) -> None:
    """Publish failure metrics and log error details."""
    job_context.logger.error(f"Job failed with error: {str(error)}")
    job_context.logger.error(f"Error details: {traceback.format_exc()}")

    if job_context.cloudwatch_client:
        publish_job_status(
            job_context.cloudwatch_client,
            job_context.job_name,
            "Failure",
            job_context.environment,
        )
        publish_metric(
            job_context.cloudwatch_client,
            job_context.job_name,
            "JobFailed",
            1,
            dimensions=[
                {"Name": "Environment", "Value": job_context.environment},
                {"Name": "ErrorType", "Value": type(error).__name__},
            ],
        )


def main():
    """Main entry point - orchestrates the ETL process."""
    job_context: Optional[JobContext] = None

    try:
        # Resolve Glue job arguments
        args = getResolvedOptions(
            sys.argv,
            [
                "job_name",
                "environment",
                "glue_s3_bucket",
                "ingestion_s3_bucket",
                "config_key",
                "aws_region",
                "aws_accountid",
                "chunk_size",
                "etl_configs_table_name",
                "etl_watermark_table_name",
            ],
        )

        # Step 1: Initialize job components
        job_context = initialize_job(args)
        job_context.logger.info("=== Initialization Complete ===")

        # Step 2: Load job configuration from DynamoDB
        job_config: JobConfig = read_job_config(
            config_key=args["config_key"],
            table_name=args["etl_configs_table_name"],
            region_name=args["aws_region"],
            logger=job_context.logger,
        )
        job_context.logger.info(f"Loaded job config: {job_config.config_key}")

        # Step 3: Read all sources
        source_reader = SourceReader(job_context, job_config)
        has_no_error, source_views = source_reader.read_source_list()

        if not has_no_error:
            job_context.logger.warning(
                "Some sources failed to read, continuing with available data"
            )

        job_context.logger.info(f"Source views available: {list(source_views.keys())}")

        # Step 4: Apply enrichment if configured
        enrichment_config = getattr(job_config, "enrichment_config", None)
        if enrichment_config and enrichment_config.enabled:
            job_context.logger.info("Enrichment configured - applying broadcast joins")
            enricher = BroadcastEnricher(
                job_context, {"enrichments": enrichment_config.enrichments}
            )
            enricher.load_lookups()

            for source_key, temp_view_name in list(source_views.items()):
                if not temp_view_name:
                    continue
                if not enricher.should_enrich(source_key):
                    continue

                job_context.logger.info(f"Enriching source: {source_key}")
                enriched_view = enricher.enrich(source_key, temp_view_name)
                if enriched_view != temp_view_name:
                    source_views[source_key] = enriched_view
                    job_context.logger.info(
                        f"Source '{source_key}' enriched: {temp_view_name} -> {enriched_view}"
                    )
        else:
            job_context.logger.info("No enrichment configured, skipping")

        # Step 5: Write all targets
        target_writer = TargetWriter(job_context, job_config)
        has_error, target_results = target_writer.write_target_list(source_views)

        job_context.logger.info(f"Target write results: {target_results}")

        # Step 6: Commit watermarks for successful targets
        source_reader.commit_pending_watermarks(target_results, job_config)

        # Step 7: Handle completion
        if has_error:
            failed_targets = [k for k, v in target_results.items() if v == "FAILED"]
            raise RuntimeError(
                f"Job completed with errors. Failed targets: {failed_targets}"
            )

        handle_job_success(job_context)

    except Exception as e:
        if job_context:
            handle_job_failure(job_context, e)
        raise


if __name__ == "__main__":
    main()
