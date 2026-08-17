"""Tests for data validators."""

import pytest
from unittest.mock import MagicMock, patch

from etl_framework.validators.null_check import NullCheckValidator
from etl_framework.validators.data_comparison import DataComparisonValidator


class TestNullCheckValidator:
    """Tests for NullCheckValidator."""

    def test_passes_with_no_nulls(self):
        """Test validation passes with no null values."""
        validator = NullCheckValidator()

        # Mock DataFrame
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name"]
        mock_df.__getitem__ = MagicMock()
        mock_df.filter.return_value.count.return_value = 0

        result = validator.validate(mock_df, columns=["id", "name"])
        assert result["passed"] is True
        assert result["total_rows"] == 100
        assert result["violations"] == []

    def test_fails_with_nulls_above_threshold(self):
        """Test validation fails when nulls exceed threshold."""
        validator = NullCheckValidator()

        # Mock DataFrame with 10% nulls
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name"]
        mock_df.__getitem__ = MagicMock()
        mock_df.filter.return_value.count.return_value = 10  # 10% null

        with pytest.raises(ValueError, match="Null check validation failed"):
            validator.validate(
                mock_df, columns=["id"], threshold_percent=5.0
            )

    def test_passes_within_threshold(self):
        """Test validation passes when nulls are within threshold."""
        validator = NullCheckValidator()

        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["id"]
        mock_df.__getitem__ = MagicMock()
        mock_df.filter.return_value.count.return_value = 3  # 3% null

        result = validator.validate(
            mock_df, columns=["id"], threshold_percent=5.0
        )
        assert result["passed"] is True

    def test_empty_dataframe_passes(self):
        """Test that empty DataFrame passes validation."""
        validator = NullCheckValidator()

        mock_df = MagicMock()
        mock_df.count.return_value = 0

        result = validator.validate(mock_df, columns=["id"])
        assert result["passed"] is True
        assert result["total_rows"] == 0

    def test_missing_column_skipped(self):
        """Test that missing columns are skipped gracefully."""
        validator = NullCheckValidator()

        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["id"]

        result = validator.validate(
            mock_df, columns=["nonexistent"], fail_on_violation=False
        )
        assert result["passed"] is True


class TestDataComparisonValidator:
    """Tests for DataComparisonValidator."""

    def test_passes_with_matching_counts(self):
        """Test validation passes with matching row counts."""
        validator = DataComparisonValidator()

        source_df = MagicMock()
        source_df.count.return_value = 100
        source_df.select.return_value.distinct.return_value.subtract.return_value.count.return_value = 0

        target_df = MagicMock()
        target_df.count.return_value = 100
        target_df.select.return_value.distinct.return_value.subtract.return_value.count.return_value = 0

        result = validator.validate(
            source_df, target_df, key_columns=["id"], fail_on_violation=False
        )
        assert result["passed"] is True
        assert result["source_count"] == 100
        assert result["target_count"] == 100

    def test_fails_with_count_difference(self):
        """Test validation fails with significant count difference."""
        validator = DataComparisonValidator()

        source_df = MagicMock()
        source_df.count.return_value = 100
        source_df.select.return_value.distinct.return_value.subtract.return_value.count.return_value = 20

        target_df = MagicMock()
        target_df.count.return_value = 80
        target_df.select.return_value.distinct.return_value.subtract.return_value.count.return_value = 0

        with pytest.raises(ValueError, match="Data comparison validation failed"):
            validator.validate(
                source_df,
                target_df,
                key_columns=["id"],
                row_count_tolerance_percent=1.0,
            )
