"""Type definitions for DataSUS-ETL."""

from pathlib import Path
from typing import Any, TypeAlias

# Path types
PathLike: TypeAlias = str | Path

# Data types
JsonDict: TypeAlias = dict[str, Any]

# UF codes
UF_CODE: TypeAlias = str  # Two-letter state code (e.g., "SP", "RJ")
