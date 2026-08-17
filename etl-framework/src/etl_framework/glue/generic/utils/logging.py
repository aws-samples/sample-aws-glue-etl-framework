"""
Logging Utilities.

Provides standardized logging setup for ETL Framework Glue jobs.
"""

import logging
import sys
from typing import Optional


def setup_logging(
    job_name: str = "etl-framework",
    log_level: Optional[str] = None,
) -> logging.Logger:
    """
    Set up structured logging for an ETL Framework Glue job.

    Creates a logger with a standardized format including timestamp,
    job name, level, and message. Configures output to stdout for
    CloudWatch Logs integration.

    Args:
        job_name: Name of the job (used in log prefix)
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
                  Defaults to INFO.

    Returns:
        Configured logger instance
    """
    level = getattr(logging, (log_level or "INFO").upper(), logging.INFO)

    # Create logger
    logger = logging.getLogger(job_name)
    logger.setLevel(level)

    # Avoid duplicate handlers if setup is called multiple times
    if logger.handlers:
        return logger

    # Create handler for stdout (CloudWatch Logs captures stdout)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Suppress verbose Spark/Glue logging
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return logger
