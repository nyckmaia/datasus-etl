"""Custom exceptions for DataSUS-ETL."""


class DataSUS_ETLError(Exception):
    """Base exception for all DataSUS-ETL errors."""

    pass


class DownloadError(DataSUS_ETLError):
    """Raised when FTP download fails."""

    pass


class ConversionError(DataSUS_ETLError):
    """Raised when file conversion fails (DBC/DBF/CSV)."""

    pass


class TransformError(DataSUS_ETLError):
    """Raised when data transformation fails."""

    pass


class ValidationError(DataSUS_ETLError):
    """Raised when data validation fails."""

    pass


class ConfigurationError(DataSUS_ETLError):
    """Raised when configuration is invalid."""

    pass


class StorageError(DataSUS_ETLError):
    """Raised when storage operation fails."""

    pass


class DatabaseError(DataSUS_ETLError):
    """Raised when database operation fails."""

    pass


class EnrichmentError(DataSUS_ETLError):
    """Raised when data enrichment fails."""

    pass


class PipelineCancelled(DataSUS_ETLError):
    """Raised when pipeline is cancelled by user (Ctrl+C)."""

    pass


# Legacy alias for backward compatibility
PyInmetError = DataSUS_ETLError
