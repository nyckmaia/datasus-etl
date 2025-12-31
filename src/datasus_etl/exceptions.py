"""Custom exceptions for PyDataSUS."""


class PyDataSUSError(Exception):
    """Base exception for all PyDataSUS errors."""

    pass


class DownloadError(PyDataSUSError):
    """Raised when FTP download fails."""

    pass


class ConversionError(PyDataSUSError):
    """Raised when file conversion fails (DBC/DBF/CSV)."""

    pass


class TransformError(PyDataSUSError):
    """Raised when data transformation fails."""

    pass


class ValidationError(PyDataSUSError):
    """Raised when data validation fails."""

    pass


class ConfigurationError(PyDataSUSError):
    """Raised when configuration is invalid."""

    pass


class StorageError(PyDataSUSError):
    """Raised when storage operation fails."""

    pass


class DatabaseError(PyDataSUSError):
    """Raised when database operation fails."""

    pass


class EnrichmentError(PyDataSUSError):
    """Raised when data enrichment fails."""

    pass


# Legacy alias for backward compatibility
PyInmetError = PyDataSUSError
