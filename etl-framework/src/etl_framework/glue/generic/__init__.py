"""
Generic Glue ETL Framework components.

This module provides the core building blocks for config-driven ETL jobs:
- Constants and enums for storage types
- Plugin registry for readers and writers
- Source readers and target writers
- Metrics publishing
- Logging utilities
"""

from etl_framework.glue.generic.metrics.cloudwatch import (
    get_cloudwatch_client,
    publish_metric,
)
from etl_framework.glue.generic.utils.logging import setup_logging

__all__ = [
    "get_cloudwatch_client",
    "publish_metric",
    "setup_logging",
]
