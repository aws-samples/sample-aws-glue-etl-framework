"""Tests for logging utilities."""

import logging

from etl_framework.glue.generic.utils.logging import setup_logging


class TestSetupLogging:
    """Tests for setup_logging."""

    def test_returns_logger(self):
        """Test that setup_logging returns a logger."""
        logger = setup_logging("test-job")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test-job"

    def test_default_level_is_info(self):
        """Test default logging level is INFO."""
        logger = setup_logging("test-job-info")
        assert logger.level == logging.INFO

    def test_custom_level(self):
        """Test setting custom log level."""
        logger = setup_logging("test-job-debug", log_level="DEBUG")
        assert logger.level == logging.DEBUG

    def test_idempotent(self):
        """Test calling setup_logging multiple times doesn't add handlers."""
        logger1 = setup_logging("test-idempotent")
        handler_count = len(logger1.handlers)
        logger2 = setup_logging("test-idempotent")
        assert len(logger2.handlers) == handler_count
        assert logger1 is logger2
