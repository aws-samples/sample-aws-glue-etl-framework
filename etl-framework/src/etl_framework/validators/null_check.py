"""
Null Check Validator.

Validates that specified columns in a DataFrame do not contain
null values beyond an acceptable threshold.
"""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class NullCheckValidator:
    """
    Validates DataFrames for null values in critical columns.

    Configuration example:
    {
        "type": "null_check",
        "columns": ["customer_id", "order_date", "amount"],
        "threshold_percent": 5.0,
        "fail_on_violation": true
    }
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the NullCheckValidator.

        Args:
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)

    def validate(
        self,
        df: Any,
        columns: List[str],
        threshold_percent: float = 0.0,
        fail_on_violation: bool = True,
    ) -> Dict[str, Any]:
        """
        Validate that specified columns have null values below the threshold.

        Args:
            df: PySpark DataFrame to validate
            columns: List of column names to check for nulls
            threshold_percent: Maximum acceptable percentage of null values (0-100)
            fail_on_violation: If True, raises ValueError on violation

        Returns:
            Dictionary with validation results:
            {
                "passed": bool,
                "total_rows": int,
                "violations": [
                    {"column": str, "null_count": int, "null_percent": float}
                ]
            }

        Raises:
            ValueError: If fail_on_violation is True and validation fails
        """
        self.logger.info(
            f"NullCheckValidator: Checking columns {columns} "
            f"(threshold={threshold_percent}%)"
        )

        total_rows = df.count()
        if total_rows == 0:
            self.logger.warning("NullCheckValidator: DataFrame is empty")
            return {"passed": True, "total_rows": 0, "violations": []}

        violations = []
        all_passed = True

        for column in columns:
            if column not in df.columns:
                self.logger.warning(
                    f"NullCheckValidator: Column '{column}' not found in DataFrame"
                )
                continue

            null_count = df.filter(df[column].isNull()).count()
            null_percent = (null_count / total_rows) * 100

            if null_percent > threshold_percent:
                violations.append(
                    {
                        "column": column,
                        "null_count": null_count,
                        "null_percent": round(null_percent, 2),
                    }
                )
                all_passed = False
                self.logger.warning(
                    f"NullCheckValidator: Column '{column}' has {null_percent:.2f}% "
                    f"null values ({null_count}/{total_rows})"
                )
            else:
                self.logger.info(
                    f"NullCheckValidator: Column '{column}' OK "
                    f"({null_percent:.2f}% nulls)"
                )

        result = {
            "passed": all_passed,
            "total_rows": total_rows,
            "violations": violations,
        }

        if not all_passed and fail_on_violation:
            violation_msg = "; ".join(
                f"{v['column']}={v['null_percent']}%" for v in violations
            )
            raise ValueError(
                f"Null check validation failed: {violation_msg} "
                f"(threshold={threshold_percent}%)"
            )

        return result
