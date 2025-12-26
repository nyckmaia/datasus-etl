"""Configuration management for PyDataSUS using Pydantic."""

from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from pydatasus.constants import ALL_UFS


class DownloadConfig(BaseModel):
    """Configuration for FTP download from DATASUS."""

    output_dir: Path = Field(
        default=Path("./data/datasus/dbc"),
        description="Directory to save downloaded DBC files",
    )
    start_date: str = Field(default="2000-01-01", description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(default=None, description="End date (YYYY-MM-DD, None=today)")
    uf_list: Optional[list[str]] = Field(
        default=None, description="List of UF codes (None = all states)"
    )
    override: bool = Field(default=False, description="Override existing files")
    timeout: int = Field(default=60, ge=10, description="FTP timeout in seconds")
    max_retries: int = Field(default=3, ge=1, description="Maximum retry attempts")

    @field_validator("output_dir")
    @classmethod
    def validate_output_dir(cls, v: Path) -> Path:
        return v.resolve()

    @field_validator("uf_list")
    @classmethod
    def validate_uf_list(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if v is not None:
            invalid = [uf for uf in v if uf not in ALL_UFS]
            if invalid:
                raise ValueError(f"Invalid UF codes: {invalid}")
        return v


class ConversionConfig(BaseModel):
    """Configuration for file conversions (DBC→DBF)."""

    dbc_dir: Path = Field(description="Directory with DBC files")
    dbf_dir: Path = Field(description="Directory for DBF files")
    csv_dir: Optional[Path] = Field(
        default=None,
        description="DEPRECATED: CSV intermediate format no longer used. Data goes directly DBF→DuckDB→Parquet. This field is ignored.",
    )
    tabwin_dir: Optional[Path] = Field(
        default=None,
        description="DEPRECATED: TABWIN no longer required. DBC decompression now uses datasus-dbc Python library. This field is ignored.",
    )
    override: bool = Field(default=False, description="Override existing files")
    max_workers: Optional[int] = Field(default=None, description="Parallel workers (None=auto)")


class ProcessingConfig(BaseModel):
    """Configuration for data processing."""

    input_dir: Path = Field(description="Directory with raw CSV files")
    output_dir: Path = Field(description="Directory for processed CSV files")
    override: bool = Field(default=False, description="Override existing files")
    max_workers: Optional[int] = Field(default=None, description="Parallel workers")


class StorageConfig(BaseModel):
    """Configuration for Parquet storage."""

    parquet_dir: Path = Field(description="Directory for Parquet files")
    partition_cols: list[str] = Field(
        default=["uf"], description="Partition columns (default: by UF state)"
    )
    compression: Literal["snappy", "gzip", "brotli", "zstd"] = Field(
        default="snappy", description="Compression codec"
    )
    row_group_size: int = Field(
        default=128_000_000,
        ge=1000,
        description="Row group size in bytes (default: 128MB)"
    )
    max_file_size: int = Field(
        default=512_000_000,
        ge=1_000_000,
        description="Maximum file size in bytes (default: 512MB)"
    )
    # CSV export options
    export_raw_csv: bool = Field(
        default=False,
        description="Export raw DBF data to CSV (before transformations)"
    )
    export_cleaned_csv: bool = Field(
        default=False,
        description="Export cleaned data to CSV (after transformations)"
    )
    csv_dir: Optional[Path] = Field(
        default=None,
        description="Directory for CSV exports (defaults to parquet_dir/../csv)"
    )


class DatabaseConfig(BaseModel):
    """Configuration for DuckDB database."""

    db_path: Optional[Path] = Field(default=None, description="DuckDB file (None=memory)")
    read_only: bool = Field(default=False, description="Read-only mode")
    threads: Optional[int] = Field(default=None, description="Number of threads (None=auto)")
    chunk_size: int = Field(default=10000, ge=1000, description="Chunk size for streaming DBF to DuckDB")
    dataframe_threshold_mb: int = Field(
        default=250,
        ge=10,
        le=2000,
        description="File size threshold (MB) for using full DataFrame vs chunked streaming (default: 250MB)"
    )


class PipelineConfig(BaseModel):
    """Complete pipeline configuration."""

    download: DownloadConfig
    conversion: ConversionConfig
    processing: Optional[ProcessingConfig] = Field(
        default=None,
        description="DEPRECATED: Processing config no longer used. Transformations happen in DuckDB SQL. This field is ignored.",
    )
    storage: StorageConfig
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def from_dict(cls, config_dict: dict) -> "PipelineConfig":
        return cls(**config_dict)
