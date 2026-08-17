"""Tests for constants and enums."""

from etl_framework.glue.generic.constants import (
    STORAGE_TYPE,
    DataFormat,
    WriteMode,
    WatermarkStrategy,
    find_enum_by_attribute,
)


class TestStorageType:
    """Tests for STORAGE_TYPE enum."""

    def test_all_types_defined(self):
        """Test that all expected storage types exist."""
        expected = ["S3", "GLUE", "ICEBERG", "DYNAMODB", "REDSHIFT",
                    "REDSHIFT_SERVERLESS", "API", "UNKNOWN"]
        actual = [member.name for member in STORAGE_TYPE]
        for name in expected:
            assert name in actual

    def test_value_and_description(self):
        """Test that each member has value and description."""
        for member in STORAGE_TYPE:
            assert member.value is not None
            assert member.description is not None
            assert len(member.description) > 0

    def test_s3_value(self):
        """Test S3 enum value."""
        assert STORAGE_TYPE.S3.value == "S3"
        assert "S3" in STORAGE_TYPE.S3.description


class TestDataFormat:
    """Tests for DataFormat enum."""

    def test_formats_defined(self):
        """Test all expected formats exist."""
        assert DataFormat.CSV.value == "csv"
        assert DataFormat.JSON.value == "json"
        assert DataFormat.PARQUET.value == "parquet"
        assert DataFormat.ORC.value == "orc"
        assert DataFormat.AVRO.value == "avro"


class TestWriteMode:
    """Tests for WriteMode enum."""

    def test_modes_defined(self):
        """Test all write modes exist."""
        assert WriteMode.OVERWRITE.value == "overwrite"
        assert WriteMode.APPEND.value == "append"
        assert WriteMode.ERROR_IF_EXISTS.value == "error"
        assert WriteMode.IGNORE.value == "ignore"


class TestWatermarkStrategy:
    """Tests for WatermarkStrategy enum."""

    def test_strategies_defined(self):
        """Test all watermark strategies exist."""
        assert WatermarkStrategy.TIMESTAMP.value == "timestamp"
        assert WatermarkStrategy.SEQUENCE.value == "sequence"
        assert WatermarkStrategy.FILE_PATH.value == "file_path"
        assert WatermarkStrategy.NONE.value == "none"


class TestFindEnumByAttribute:
    """Tests for find_enum_by_attribute utility."""

    def test_find_by_name(self):
        """Test finding enum by name attribute."""
        result = find_enum_by_attribute(STORAGE_TYPE, "name", "S3")
        assert result == STORAGE_TYPE.S3

    def test_find_case_insensitive(self):
        """Test case-insensitive search."""
        result = find_enum_by_attribute(STORAGE_TYPE, "name", "redshift")
        assert result == STORAGE_TYPE.REDSHIFT

    def test_find_nonexistent_returns_none(self):
        """Test that non-existent value returns None."""
        result = find_enum_by_attribute(STORAGE_TYPE, "name", "NONEXISTENT")
        assert result is None

    def test_find_by_value(self):
        """Test finding by value attribute."""
        result = find_enum_by_attribute(STORAGE_TYPE, "value", "S3")
        assert result == STORAGE_TYPE.S3
