"""
Plugin Registry for Readers and Writers.

Provides a registration mechanism that allows users to extend the framework
with custom readers and writers without modifying core orchestrator code.

Usage:
    # Register a built-in reader
    ReaderRegistry.register("S3", S3Reader)

    # Register a custom reader
    from etl_framework.glue.generic.registry import ReaderRegistry
    ReaderRegistry.register("MY_CUSTOM_SOURCE", MyCustomReader)

    # Retrieve a reader class
    reader_class = ReaderRegistry.get("S3")
    reader = reader_class(job_context)

    # Similarly for writers
    WriterRegistry.register("S3", S3Writer)
    writer_class = WriterRegistry.get("S3")
"""

import logging
from typing import Dict, Optional, Type

logger = logging.getLogger(__name__)


class ReaderRegistry:
    """
    Registry for source reader classes.

    Readers are registered by storage type name (string). The source_reader
    orchestrator uses this registry to dynamically resolve which reader class
    handles a given source type, enabling extensibility without code changes
    to the orchestrator.

    All reader classes must implement the ReaderInterface.
    """

    _readers: Dict[str, Type] = {}

    @classmethod
    def register(cls, storage_type: str, reader_class: Type) -> None:
        """
        Register a reader class for a given storage type.

        Args:
            storage_type: The storage type identifier (e.g., "S3", "REDSHIFT", "MY_API")
            reader_class: The reader class that handles this storage type.
                         Must implement ReaderInterface.

        Raises:
            ValueError: If storage_type is empty or reader_class is None
        """
        if not storage_type:
            raise ValueError("storage_type must be a non-empty string")
        if reader_class is None:
            raise ValueError("reader_class must not be None")

        normalized_type = storage_type.upper()
        if normalized_type in cls._readers:
            logger.warning(
                f"Overwriting existing reader registration for type: {normalized_type} "
                f"(previous: {cls._readers[normalized_type].__name__}, "
                f"new: {reader_class.__name__})"
            )
        cls._readers[normalized_type] = reader_class
        logger.info(f"Registered reader '{reader_class.__name__}' for type '{normalized_type}'")

    @classmethod
    def get(cls, storage_type: str) -> Optional[Type]:
        """
        Get the reader class registered for a storage type.

        Args:
            storage_type: The storage type identifier

        Returns:
            The registered reader class, or None if not found
        """
        return cls._readers.get(storage_type.upper())

    @classmethod
    def get_or_raise(cls, storage_type: str) -> Type:
        """
        Get the reader class registered for a storage type, raising if not found.

        Args:
            storage_type: The storage type identifier

        Returns:
            The registered reader class

        Raises:
            KeyError: If no reader is registered for the given type
        """
        normalized_type = storage_type.upper()
        if normalized_type not in cls._readers:
            available = ", ".join(sorted(cls._readers.keys()))
            raise KeyError(
                f"No reader registered for storage type: '{normalized_type}'. "
                f"Available types: [{available}]. "
                f"Register a reader with: ReaderRegistry.register('{normalized_type}', YourReaderClass)"
            )
        return cls._readers[normalized_type]

    @classmethod
    def list_registered(cls) -> Dict[str, str]:
        """
        List all registered readers with their class names.

        Returns:
            Dictionary mapping storage type to reader class name
        """
        return {k: v.__name__ for k, v in sorted(cls._readers.items())}

    @classmethod
    def is_registered(cls, storage_type: str) -> bool:
        """Check if a reader is registered for the given storage type."""
        return storage_type.upper() in cls._readers

    @classmethod
    def unregister(cls, storage_type: str) -> bool:
        """
        Remove a reader registration. Useful for testing.

        Returns:
            True if a registration was removed, False if type was not registered
        """
        normalized_type = storage_type.upper()
        if normalized_type in cls._readers:
            del cls._readers[normalized_type]
            return True
        return False

    @classmethod
    def clear(cls) -> None:
        """Remove all registrations. Primarily useful for testing."""
        cls._readers.clear()


class WriterRegistry:
    """
    Registry for target writer classes.

    Writers are registered by storage type name (string). The target_writer
    orchestrator uses this registry to dynamically resolve which writer class
    handles a given target type, enabling extensibility without code changes
    to the orchestrator.

    All writer classes must implement the WriterInterface.
    """

    _writers: Dict[str, Type] = {}

    @classmethod
    def register(cls, storage_type: str, writer_class: Type) -> None:
        """
        Register a writer class for a given storage type.

        Args:
            storage_type: The storage type identifier (e.g., "S3", "REDSHIFT")
            writer_class: The writer class that handles this storage type.
                         Must implement WriterInterface.

        Raises:
            ValueError: If storage_type is empty or writer_class is None
        """
        if not storage_type:
            raise ValueError("storage_type must be a non-empty string")
        if writer_class is None:
            raise ValueError("writer_class must not be None")

        normalized_type = storage_type.upper()
        if normalized_type in cls._writers:
            logger.warning(
                f"Overwriting existing writer registration for type: {normalized_type} "
                f"(previous: {cls._writers[normalized_type].__name__}, "
                f"new: {writer_class.__name__})"
            )
        cls._writers[normalized_type] = writer_class
        logger.info(f"Registered writer '{writer_class.__name__}' for type '{normalized_type}'")

    @classmethod
    def get(cls, storage_type: str) -> Optional[Type]:
        """
        Get the writer class registered for a storage type.

        Args:
            storage_type: The storage type identifier

        Returns:
            The registered writer class, or None if not found
        """
        return cls._writers.get(storage_type.upper())

    @classmethod
    def get_or_raise(cls, storage_type: str) -> Type:
        """
        Get the writer class registered for a storage type, raising if not found.

        Args:
            storage_type: The storage type identifier

        Returns:
            The registered writer class

        Raises:
            KeyError: If no writer is registered for the given type
        """
        normalized_type = storage_type.upper()
        if normalized_type not in cls._writers:
            available = ", ".join(sorted(cls._writers.keys()))
            raise KeyError(
                f"No writer registered for storage type: '{normalized_type}'. "
                f"Available types: [{available}]. "
                f"Register a writer with: WriterRegistry.register('{normalized_type}', YourWriterClass)"
            )
        return cls._writers[normalized_type]

    @classmethod
    def list_registered(cls) -> Dict[str, str]:
        """
        List all registered writers with their class names.

        Returns:
            Dictionary mapping storage type to writer class name
        """
        return {k: v.__name__ for k, v in sorted(cls._writers.items())}

    @classmethod
    def is_registered(cls, storage_type: str) -> bool:
        """Check if a writer is registered for the given storage type."""
        return storage_type.upper() in cls._writers

    @classmethod
    def unregister(cls, storage_type: str) -> bool:
        """
        Remove a writer registration. Useful for testing.

        Returns:
            True if a registration was removed, False if type was not registered
        """
        normalized_type = storage_type.upper()
        if normalized_type in cls._writers:
            del cls._writers[normalized_type]
            return True
        return False

    @classmethod
    def clear(cls) -> None:
        """Remove all registrations. Primarily useful for testing."""
        cls._writers.clear()
