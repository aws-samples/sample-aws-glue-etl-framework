"""Tests for configuration utilities."""

import json
from unittest.mock import MagicMock, patch

import pytest

from etl_framework.glue.generic.models.job_config_models import JobConfig
from etl_framework.glue.generic.utils.config import read_job_config


class TestReadJobConfig:
    """Tests for read_job_config."""

    @patch("etl_framework.glue.generic.utils.config.boto3")
    def test_reads_config_successfully(self, mock_boto3):
        """Test reading a valid config from DynamoDB."""
        # Mock DynamoDB response
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "config_key": "TEST_JOB",
                "source_list": [
                    {"source_key": "src1", "source_type": "S3"}
                ],
                "target_list": [
                    {"target_type": "S3", "source_config_key": "src1"}
                ],
            }
        }
        mock_boto3.resource.return_value.Table.return_value = mock_table

        result = read_job_config(
            config_key="TEST_JOB",
            table_name="my-configs",
            region_name="us-west-2",
        )

        assert isinstance(result, JobConfig)
        assert result.config_key == "TEST_JOB"
        assert len(result.source_list) == 1
        assert len(result.target_list) == 1

    @patch("etl_framework.glue.generic.utils.config.boto3")
    def test_raises_on_missing_config(self, mock_boto3):
        """Test that missing config raises ValueError."""
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}  # No Item key
        mock_boto3.resource.return_value.Table.return_value = mock_table

        with pytest.raises(ValueError, match="not found"):
            read_job_config(
                config_key="MISSING",
                table_name="my-configs",
                region_name="us-west-2",
            )

    @patch("etl_framework.glue.generic.utils.config.boto3")
    def test_parses_string_fields(self, mock_boto3):
        """Test that JSON string fields are parsed."""
        mock_table = MagicMock()
        mock_table.get_item.return_value = {
            "Item": {
                "config_key": "STR_JOB",
                "source_list": json.dumps([{"source_key": "s", "source_type": "S3"}]),
                "target_list": json.dumps([{"target_type": "S3", "source_config_key": "s"}]),
            }
        }
        mock_boto3.resource.return_value.Table.return_value = mock_table

        result = read_job_config(
            config_key="STR_JOB",
            table_name="configs",
            region_name="us-west-2",
        )

        assert isinstance(result.source_list, list)
        assert result.source_list[0]["source_key"] == "s"
