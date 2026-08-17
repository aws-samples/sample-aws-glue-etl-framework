"""Tests for data models."""

from etl_framework.glue.generic.models.job_config_models import (
    EnrichmentConfig,
    JobConfig,
    JobContext,
    ProcessingMetrics,
    SourceDef,
    TargetDef,
    ValidationConfig,
)


class TestSourceDef:
    """Tests for SourceDef dataclass."""

    def test_from_dict_minimal(self):
        """Test creating SourceDef with minimal fields."""
        data = {
            "source_key": "test_source",
            "source_type": "S3",
        }
        source = SourceDef.from_dict(data)
        assert source.source_key == "test_source"
        assert source.source_type == "S3"
        assert source.create_temp_view is True

    def test_from_dict_full_s3(self):
        """Test creating SourceDef with full S3 config."""
        data = {
            "source_key": "my_data",
            "source_type": "S3",
            "source_s3_bucket": "my-bucket",
            "source_s3_path": "raw/data/",
            "source_format": "parquet",
            "source_format_options": {"mergeSchema": "true"},
            "source_filter": "status = 'active'",
            "source_select_columns": ["id", "name", "status"],
            "watermark_strategy": "timestamp",
            "watermark_column": "updated_at",
        }
        source = SourceDef.from_dict(data)
        assert source.source_s3_bucket == "my-bucket"
        assert source.source_s3_path == "raw/data/"
        assert source.source_format == "parquet"
        assert source.source_format_options == {"mergeSchema": "true"}
        assert source.source_filter == "status = 'active'"
        assert source.source_select_columns == ["id", "name", "status"]
        assert source.watermark_strategy == "timestamp"
        assert source.watermark_column == "updated_at"

    def test_from_dict_api_source(self):
        """Test creating SourceDef with API config."""
        data = {
            "source_key": "api_data",
            "source_type": "API",
            "source_api_url": "https://api.example.com",
            "source_api_method": "GET",
            "source_api_resource_path": "/v1/data",
            "source_api_secrets_arn": "arn:aws:secretsmanager:us-west-2:123:secret:key",
            "source_api_pagination": {"type": "offset", "page_size": 100},
        }
        source = SourceDef.from_dict(data)
        assert source.source_api_url == "https://api.example.com"
        assert source.source_api_method == "GET"
        assert source.source_api_pagination == {"type": "offset", "page_size": 100}

    def test_from_dict_missing_key_defaults(self):
        """Test that missing keys get default values."""
        data = {}
        source = SourceDef.from_dict(data)
        assert source.source_key == ""
        assert source.source_type == "UNKNOWN"
        assert source.create_temp_view is True
        assert source.source_s3_bucket is None


class TestTargetDef:
    """Tests for TargetDef dataclass."""

    def test_from_dict_minimal(self):
        """Test creating TargetDef with minimal fields."""
        data = {
            "target_type": "S3",
            "source_config_key": "raw_data",
        }
        target = TargetDef.from_dict(data)
        assert target.target_type == "S3"
        assert target.source_config_key == "raw_data"
        assert target.write_mode == "overwrite"

    def test_from_dict_redshift(self):
        """Test creating TargetDef with Redshift config."""
        data = {
            "target_type": "REDSHIFT",
            "source_config_key": "source_a",
            "database_name": "warehouse",
            "schema_name": "raw",
            "table_name": "orders",
            "target_redshift_secrets_arn": "arn:aws:secretsmanager:...",
            "pre_actions": "TRUNCATE TABLE raw.orders;",
            "write_mode": "append",
        }
        target = TargetDef.from_dict(data)
        assert target.schema_name == "raw"
        assert target.table_name == "orders"
        assert target.pre_actions == "TRUNCATE TABLE raw.orders;"

    def test_from_dict_with_column_aliases(self):
        """Test column aliasing support."""
        data = {
            "target_type": "S3",
            "source_config_key": "raw",
            "target_select": ["col1", "col2"],
            "column_aliases": {"col1": "column_one", "col2": "column_two"},
        }
        target = TargetDef.from_dict(data)
        assert target.target_select == ["col1", "col2"]
        assert target.column_aliases == {"col1": "column_one", "col2": "column_two"}

    def test_from_dict_s3_path_alias(self):
        """Test that 's3_path' is accepted as alternative to 'target_s3_path'."""
        data = {
            "target_type": "S3",
            "s3_path": "s3://bucket/path/",
        }
        target = TargetDef.from_dict(data)
        assert target.target_s3_path == "s3://bucket/path/"


class TestJobConfig:
    """Tests for JobConfig dataclass."""

    def test_from_dict(self):
        """Test creating JobConfig from dict."""
        data = {
            "config_key": "MY_JOB",
            "source_list": [{"source_key": "a", "source_type": "S3"}],
            "target_list": [{"target_type": "S3", "source_config_key": "a"}],
        }
        config = JobConfig.from_dict(data)
        assert config.config_key == "MY_JOB"
        assert len(config.source_list) == 1
        assert len(config.target_list) == 1
        assert config.enrichment_config is None
        assert config.validation_config is None

    def test_from_dict_with_enrichment(self):
        """Test creating JobConfig with enrichment."""
        data = {
            "config_key": "ENRICHED_JOB",
            "source_list": [],
            "target_list": [],
            "enrichment_config": {
                "enabled": True,
                "enrichments": [{"source_key": "a", "lookups": []}],
            },
        }
        config = JobConfig.from_dict(data)
        assert config.enrichment_config is not None
        assert config.enrichment_config.enabled is True
        assert len(config.enrichment_config.enrichments) == 1


class TestEnrichmentConfig:
    """Tests for EnrichmentConfig."""

    def test_from_dict_none_returns_none(self):
        """Test that None input returns None."""
        assert EnrichmentConfig.from_dict(None) is None

    def test_from_dict_empty_returns_none(self):
        """Test that empty dict returns None."""
        assert EnrichmentConfig.from_dict({}) is None

    def test_from_dict_enabled(self):
        """Test creating enabled enrichment config."""
        data = {
            "enabled": True,
            "enrichments": [{"source_key": "s1", "lookups": []}],
        }
        config = EnrichmentConfig.from_dict(data)
        assert config.enabled is True
        assert len(config.enrichments) == 1


class TestProcessingMetrics:
    """Tests for ProcessingMetrics."""

    def test_defaults(self):
        """Test default values."""
        metrics = ProcessingMetrics()
        assert metrics.records_read == 0
        assert metrics.records_written == 0
        assert metrics.records_failed == 0
        assert metrics.sources_processed == 0
        assert metrics.targets_processed == 0
