"""
Broadcast Enricher - Enriches source data using broadcast join lookups.

Loads small lookup datasets into Spark broadcast variables and joins
them with source DataFrames for data enrichment (e.g., adding product
names, mapping codes to descriptions).
"""

import traceback
from typing import Any, Dict, List, Optional

from etl_framework.glue.generic.models.job_config_models import JobContext
from etl_framework.glue.generic.utils.sql_validation import (
    SQLValidationError,
    validate_identifier,
)


class BroadcastEnricher:
    """
    Enriches source data by broadcast-joining lookup tables.

    Broadcast joins are efficient when the lookup table is small
    (fits in memory on each executor) and the source table is large.

    Enrichment configuration example:
    {
        "enabled": true,
        "enrichments": [
            {
                "source_key": "orders",
                "lookups": [
                    {
                        "lookup_key": "product_mapping",
                        "lookup_s3_path": "s3://bucket/lookups/products.csv",
                        "lookup_format": "csv",
                        "join_column": "product_id",
                        "select_columns": ["product_name", "category"]
                    }
                ]
            }
        ]
    }
    """

    def __init__(self, job_context: JobContext, enrichment_config: Dict[str, Any]):
        """
        Initialize the BroadcastEnricher.

        Args:
            job_context: Job runtime context
            enrichment_config: Enrichment configuration dictionary
        """
        self.job_context = job_context
        self.logger = job_context.logger
        self.spark = job_context.spark_session
        self.config = enrichment_config
        self.enrichments = enrichment_config.get("enrichments", [])
        self.loaded_lookups: Dict[str, Any] = {}

    def load_lookups(self) -> bool:
        """
        Load all lookup datasets into memory.

        Returns:
            True if all lookups loaded successfully, False if any failed
        """
        self.logger.info("BroadcastEnricher: Loading lookup datasets")
        all_success = True

        for enrichment in self.enrichments:
            source_key = enrichment.get("source_key", "unknown")
            lookups = enrichment.get("lookups", [])

            for lookup in lookups:
                lookup_key = lookup.get("lookup_key", "unknown")
                try:
                    lookup_df = self._load_single_lookup(lookup)
                    if lookup_df is not None:
                        self.loaded_lookups[lookup_key] = {
                            "df": lookup_df,
                            "config": lookup,
                            "source_key": source_key,
                        }
                        self.logger.info(
                            f"BroadcastEnricher: Loaded lookup '{lookup_key}' "
                            f"({lookup_df.count()} rows)"
                        )
                    else:
                        self.logger.warning(
                            f"BroadcastEnricher: Failed to load lookup '{lookup_key}'"
                        )
                        all_success = False
                except Exception as e:
                    self.logger.error(
                        f"BroadcastEnricher: Error loading lookup '{lookup_key}': {str(e)}"
                    )
                    self.logger.error(traceback.format_exc())
                    all_success = False

        self.logger.info(
            f"BroadcastEnricher: Loaded {len(self.loaded_lookups)} lookup(s)"
        )
        return all_success

    def _load_single_lookup(self, lookup_config: Dict[str, Any]) -> Optional[Any]:
        """
        Load a single lookup dataset from S3.

        Args:
            lookup_config: Configuration for the lookup including path and format

        Returns:
            Spark DataFrame or None on failure
        """
        s3_path = lookup_config.get("lookup_s3_path")
        if not s3_path:
            # Try constructing from bucket + path
            bucket = lookup_config.get("lookup_bucket", self.job_context.glue_s3_bucket)
            path = lookup_config.get("lookup_path", "")
            s3_path = f"s3://{bucket}/{path}"

        file_format = lookup_config.get("lookup_format", "csv").lower()

        self.logger.info(f"BroadcastEnricher: Loading from {s3_path} ({file_format})")

        reader = self.spark.read.format(file_format)

        if file_format == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")

        df = reader.load(s3_path)

        # Select only needed columns if specified
        select_columns = lookup_config.get("select_columns")
        join_column = lookup_config.get("join_column")
        if select_columns and join_column:
            # Always include join column
            columns_to_select = list(set([join_column] + select_columns))
            df = df.select(*columns_to_select)

        return df

    def should_enrich(self, source_key: str) -> bool:
        """
        Check if a source should be enriched.

        Args:
            source_key: The source key to check

        Returns:
            True if enrichment is configured for this source
        """
        for enrichment in self.enrichments:
            if enrichment.get("source_key") == source_key:
                return True
        return False

    def enrich(self, source_key: str, temp_view_name: str) -> Optional[str]:
        """
        Apply broadcast join enrichment to a source temp view.

        Joins all applicable lookups with the source data and creates
        a new enriched temp view.

        Args:
            source_key: The source key being enriched
            temp_view_name: Name of the temp view to enrich

        Returns:
            Name of the enriched temp view, or original if no enrichment applied
        """
        try:
            # Get enrichment config for this source
            source_enrichments = None
            for enrichment in self.enrichments:
                if enrichment.get("source_key") == source_key:
                    source_enrichments = enrichment
                    break

            if not source_enrichments:
                return temp_view_name

            lookups = source_enrichments.get("lookups", [])
            if not lookups:
                return temp_view_name

            # Start with the source DataFrame
            from pyspark.sql.functions import broadcast

            # Validate temp_view_name before SQL interpolation
            validate_identifier(temp_view_name, "temp_view_name")

            df = self.spark.sql(f"SELECT * FROM {temp_view_name}")

            for lookup in lookups:
                lookup_key = lookup.get("lookup_key")
                if lookup_key not in self.loaded_lookups:
                    self.logger.warning(
                        f"BroadcastEnricher: Lookup '{lookup_key}' not loaded, skipping"
                    )
                    continue

                lookup_data = self.loaded_lookups[lookup_key]
                lookup_df = lookup_data["df"]
                join_column = lookup.get("join_column")
                join_type = lookup.get("join_type", "left")

                if not join_column:
                    self.logger.warning(
                        f"BroadcastEnricher: No join_column for '{lookup_key}', skipping"
                    )
                    continue

                # Validate join_column as a safe identifier
                validate_identifier(join_column, "join_column")

                self.logger.info(
                    f"BroadcastEnricher: Joining '{lookup_key}' on '{join_column}' "
                    f"(type={join_type})"
                )

                # Perform broadcast join
                df = df.join(
                    broadcast(lookup_df),
                    on=join_column,
                    how=join_type,
                )

            # Create enriched temp view
            enriched_view_name = f"enriched_{source_key}"
            # Validate the constructed view name
            validate_identifier(enriched_view_name, "enriched_view_name")
            df.createOrReplaceTempView(enriched_view_name)
            self.logger.info(
                f"BroadcastEnricher: Created enriched view '{enriched_view_name}' "
                f"({df.count()} rows)"
            )
            return enriched_view_name

        except SQLValidationError as e:
            self.logger.error(
                f"BroadcastEnricher: Validation failed for '{source_key}': {str(e)}"
            )
            # Return original view on failure (graceful degradation)
            return temp_view_name
        except Exception as e:
            self.logger.error(
                f"BroadcastEnricher: Enrichment failed for '{source_key}': {str(e)}"
            )
            self.logger.error(traceback.format_exc())
            # Return original view on failure (graceful degradation)
            return temp_view_name
