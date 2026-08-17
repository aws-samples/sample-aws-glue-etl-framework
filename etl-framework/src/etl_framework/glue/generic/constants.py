"""
Constants for the AWS Glue ETL Framework.

Defines storage type enums used for routing sources and targets
to their respective reader/writer implementations.
"""

from enum import Enum
from typing import Any, Optional


class STORAGE_TYPE(Enum):
    """
    Enum representing supported storage types for both sources and targets.

    Each member holds a (value, description) tuple.
    Users can register additional custom types via the ReaderRegistry/WriterRegistry
    without modifying this enum.
    """

    S3 = ("S3", "Amazon S3 bucket storage (CSV, JSON, Parquet)")
    GLUE = ("GLUE", "AWS Glue Data Catalog table (non-Iceberg)")
    ICEBERG = ("ICEBERG", "Apache Iceberg table via AWS Glue Data Catalog")
    DYNAMODB = ("DYNAMODB", "Amazon DynamoDB table")
    REDSHIFT = ("REDSHIFT", "Amazon Redshift provisioned cluster")
    REDSHIFT_SERVERLESS = ("REDSHIFT_SERVERLESS", "Amazon Redshift Serverless")
    API = ("API", "Generic REST API connector")
    UNKNOWN = ("UNKNOWN", "Unknown or unsupported storage type")

    def __init__(self, value: str, description: str):
        self._value_ = value
        self.description = description


class DataFormat(Enum):
    """Supported data file formats for S3-based sources and targets."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"


class WriteMode(Enum):
    """Supported write modes for target writers."""

    OVERWRITE = "overwrite"
    APPEND = "append"
    ERROR_IF_EXISTS = "error"
    IGNORE = "ignore"


class WatermarkStrategy(Enum):
    """
    Strategies for incremental data loading via watermarks.

    TIMESTAMP - Uses a timestamp column to track the high-water mark
    SEQUENCE  - Uses a monotonically increasing sequence/offset
    FILE_PATH - Uses S3 file path (last processed file prefix)
    NONE      - No watermark tracking (full reload each run)
    """

    TIMESTAMP = "timestamp"
    SEQUENCE = "sequence"
    FILE_PATH = "file_path"
    NONE = "none"


def find_enum_by_attribute(
    enum_class: type, attribute_name: str, attribute_value: str
) -> Optional[Any]:
    """
    Find an enum member by matching an attribute value (case-insensitive).

    Args:
        enum_class: The Enum class to search
        attribute_name: The attribute name to match against (e.g., 'name', 'value')
        attribute_value: The value to search for

    Returns:
        The matching enum member, or None if not found
    """
    for member in enum_class:
        if (
            hasattr(member, attribute_name)
            and getattr(member, attribute_name).lower() == attribute_value.lower()
        ):
            return member
    return None
