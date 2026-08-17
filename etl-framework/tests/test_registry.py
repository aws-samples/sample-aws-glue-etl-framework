"""Tests for the plugin registry system."""

import pytest

from etl_framework.glue.generic.registry import ReaderRegistry, WriterRegistry


class TestReaderRegistry:
    """Tests for ReaderRegistry."""

    def setup_method(self):
        """Clear registry before each test."""
        ReaderRegistry.clear()

    def test_register_and_get(self):
        """Test basic register and get."""

        class MockReader:
            pass

        ReaderRegistry.register("TEST", MockReader)
        assert ReaderRegistry.get("TEST") is MockReader

    def test_get_case_insensitive(self):
        """Test that get is case-insensitive."""

        class MockReader:
            pass

        ReaderRegistry.register("S3", MockReader)
        assert ReaderRegistry.get("s3") is MockReader
        assert ReaderRegistry.get("S3") is MockReader

    def test_get_unregistered_returns_none(self):
        """Test that getting an unregistered type returns None."""
        assert ReaderRegistry.get("NONEXISTENT") is None

    def test_get_or_raise_with_valid_type(self):
        """Test get_or_raise with a registered type."""

        class MockReader:
            pass

        ReaderRegistry.register("S3", MockReader)
        assert ReaderRegistry.get_or_raise("S3") is MockReader

    def test_get_or_raise_with_invalid_type(self):
        """Test get_or_raise raises KeyError for unregistered type."""
        with pytest.raises(KeyError, match="No reader registered"):
            ReaderRegistry.get_or_raise("NONEXISTENT")

    def test_register_empty_type_raises(self):
        """Test that registering an empty type raises ValueError."""

        class MockReader:
            pass

        with pytest.raises(ValueError, match="non-empty string"):
            ReaderRegistry.register("", MockReader)

    def test_register_none_class_raises(self):
        """Test that registering None class raises ValueError."""
        with pytest.raises(ValueError, match="must not be None"):
            ReaderRegistry.register("TEST", None)

    def test_list_registered(self):
        """Test listing all registered readers."""

        class ReaderA:
            pass

        class ReaderB:
            pass

        ReaderRegistry.register("S3", ReaderA)
        ReaderRegistry.register("REDSHIFT", ReaderB)

        registered = ReaderRegistry.list_registered()
        assert registered == {"REDSHIFT": "ReaderB", "S3": "ReaderA"}

    def test_is_registered(self):
        """Test checking if a type is registered."""

        class MockReader:
            pass

        ReaderRegistry.register("S3", MockReader)
        assert ReaderRegistry.is_registered("S3") is True
        assert ReaderRegistry.is_registered("NONEXISTENT") is False

    def test_unregister(self):
        """Test removing a registration."""

        class MockReader:
            pass

        ReaderRegistry.register("S3", MockReader)
        assert ReaderRegistry.unregister("S3") is True
        assert ReaderRegistry.get("S3") is None
        assert ReaderRegistry.unregister("S3") is False

    def test_overwrite_registration(self):
        """Test that re-registering overwrites previous."""

        class ReaderA:
            pass

        class ReaderB:
            pass

        ReaderRegistry.register("S3", ReaderA)
        ReaderRegistry.register("S3", ReaderB)
        assert ReaderRegistry.get("S3") is ReaderB

    def test_clear(self):
        """Test clearing all registrations."""

        class MockReader:
            pass

        ReaderRegistry.register("S3", MockReader)
        ReaderRegistry.register("GLUE", MockReader)
        ReaderRegistry.clear()
        assert ReaderRegistry.list_registered() == {}


class TestWriterRegistry:
    """Tests for WriterRegistry."""

    def setup_method(self):
        """Clear registry before each test."""
        WriterRegistry.clear()

    def test_register_and_get(self):
        """Test basic register and get."""

        class MockWriter:
            pass

        WriterRegistry.register("S3", MockWriter)
        assert WriterRegistry.get("S3") is MockWriter

    def test_get_case_insensitive(self):
        """Test case-insensitive lookup."""

        class MockWriter:
            pass

        WriterRegistry.register("REDSHIFT", MockWriter)
        assert WriterRegistry.get("redshift") is MockWriter

    def test_list_registered(self):
        """Test listing registered writers."""

        class WriterA:
            pass

        class WriterB:
            pass

        WriterRegistry.register("S3", WriterA)
        WriterRegistry.register("ICEBERG", WriterB)

        registered = WriterRegistry.list_registered()
        assert "S3" in registered
        assert "ICEBERG" in registered
