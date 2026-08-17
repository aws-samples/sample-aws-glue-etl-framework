"""
Data Comparison Validator.

Compares two datasets (source vs. target) to verify data consistency
after ETL processing. Useful for validating that transformations
preserved data integrity.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DataComparisonValidator:
    """
    Validates data consistency between source and target datasets.

    Compares row counts, key coverage, and optionally column-level
    value differences between two DataFrames.

    Configuration example:
    {
        "type": "data_comparison",
        "source_view": "temp_orders",
        "target_view": "temp_orders_enriched",
        "key_columns": ["order_id"],
        "compare_columns": ["amount", "status"],
        "row_count_tolerance_percent": 1.0,
        "fail_on_violation": true
    }
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the DataComparisonValidator.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)

    def validate(
        self,
        source_df: Any,
        target_df: Any,
        key_columns: List[str],
        compare_columns: Optional[List[str]] = None,
        row_count_tolerance_percent: float = 0.0,
        fail_on_violation: bool = True,
    ) -> Dict[str, Any]:
        """
        Compare source and target DataFrames for consistency.

        Args:
            source_df: Source PySpark DataFrame
            target_df: Target PySpark DataFrame
            key_columns: Columns that uniquely identify rows for matching
            compare_columns: Optional columns to compare values (if None, only counts)
            row_count_tolerance_percent: Acceptable row count difference (0-100)
            fail_on_violation: If True, raises ValueError on violation

        Returns:
            Dictionary with comparison results:
            {
                "passed": bool,
                "source_count": int,
                "target_count": int,
                "count_difference": int,
                "count_difference_percent": float,
                "missing_in_target": int,
                "missing_in_source": int,
                "value_mismatches": int
            }

        Raises:
            ValueError: If fail_on_violation is True and validation fails
        """
        self.logger.info("DataComparisonValidator: Starting comparison")

        result = {
            "passed": True,
            "source_count": 0,
            "target_count": 0,
            "count_difference": 0,
            "count_difference_percent": 0.0,
            "missing_in_target": 0,
            "missing_in_source": 0,
            "value_mismatches": 0,
        }

        # Row count comparison
        source_count = source_df.count()
        target_count = target_df.count()
        count_diff = abs(source_count - target_count)
        count_diff_percent = (
            (count_diff / source_count * 100) if source_count > 0 else 0.0
        )

        result["source_count"] = source_count
        result["target_count"] = target_count
        result["count_difference"] = count_diff
        result["count_difference_percent"] = round(count_diff_percent, 2)

        self.logger.info(
            f"DataComparisonValidator: Source={source_count}, Target={target_count}, "
            f"Diff={count_diff} ({count_diff_percent:.2f}%)"
        )

        # Check row count tolerance
        if count_diff_percent > row_count_tolerance_percent:
            result["passed"] = False
            self.logger.warning(
                f"DataComparisonValidator: Row count difference "
                f"({count_diff_percent:.2f}%) exceeds tolerance "
                f"({row_count_tolerance_percent}%)"
            )

        # Key coverage comparison
        if key_columns:
            source_keys = source_df.select(*key_columns).distinct()
            target_keys = target_df.select(*key_columns).distinct()

            # Keys in source but missing in target
            missing_in_target = source_keys.subtract(target_keys).count()
            result["missing_in_target"] = missing_in_target

            # Keys in target but missing in source
            missing_in_source = target_keys.subtract(source_keys).count()
            result["missing_in_source"] = missing_in_source

            if missing_in_target > 0 or missing_in_source > 0:
                result["passed"] = False
                self.logger.warning(
                    f"DataComparisonValidator: Key mismatches - "
                    f"missing_in_target={missing_in_target}, "
                    f"missing_in_source={missing_in_source}"
                )

        # Value-level comparison (optional)
        if compare_columns and key_columns:
            value_mismatches = self._compare_values(
                source_df, target_df, key_columns, compare_columns
            )
            result["value_mismatches"] = value_mismatches
            if value_mismatches > 0:
                result["passed"] = False

        self.logger.info(f"DataComparisonValidator: Result = {result}")

        if not result["passed"] and fail_on_violation:
            raise ValueError(
                f"Data comparison validation failed: "
                f"count_diff={count_diff_percent:.2f}%, "
                f"missing_in_target={result['missing_in_target']}, "
                f"missing_in_source={result['missing_in_source']}, "
                f"value_mismatches={result['value_mismatches']}"
            )

        return result

    def _compare_values(
        self,
        source_df: Any,
        target_df: Any,
        key_columns: List[str],
        compare_columns: List[str],
    ) -> int:
        """
        Compare specific column values between matched rows.

        Returns the number of mismatched values found.
        """
        try:
            # Join on key columns
            joined = source_df.alias("src").join(
                target_df.alias("tgt"),
                on=key_columns,
                how="inner",
            )

            # Count mismatches for each comparison column
            total_mismatches = 0
            for col in compare_columns:
                if col in source_df.columns and col in target_df.columns:
                    mismatch_count = joined.filter(
                        f"src.{col} != tgt.{col} OR "
                        f"(src.{col} IS NULL AND tgt.{col} IS NOT NULL) OR "
                        f"(src.{col} IS NOT NULL AND tgt.{col} IS NULL)"
                    ).count()
                    if mismatch_count > 0:
                        self.logger.warning(
                            f"DataComparisonValidator: Column '{col}' has "
                            f"{mismatch_count} value mismatches"
                        )
                        total_mismatches += mismatch_count

            return total_mismatches

        except Exception as e:
            self.logger.error(
                f"DataComparisonValidator: Value comparison failed: {str(e)}"
            )
            return -1
