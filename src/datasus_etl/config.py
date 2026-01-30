"""Configuration management for DataSUS-ETL using Pydantic."""

from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator

from datasus_etl.constants import ALL_UFS


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
    incremental_files: Optional[set[str]] = Field(
        default=None,
        description="Set of specific files to download (for incremental updates). "
                    "When set, only files in this set will be downloaded."
    )

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
    """Configuration for file conversions (DBC→DBF or DBC→CSV)."""

    dbc_dir: Path = Field(description="Directory with DBC files or single DBC file")
    dbf_dir: Path = Field(description="Directory for output files (DBF or CSV)")
    output_format: Literal["dbf", "csv"] = Field(
        default="dbf",
        description="Output format: 'dbf' (default) or 'csv'"
    )
    csv_dir: Optional[Path] = Field(
        default=None,
        description="DEPRECATED: CSV intermediate format no longer used. Data goes directly DBF→DuckDB. This field is ignored.",
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
    """Configuration for DuckDB persistent storage."""

    database_dir: Path = Field(description="Directory for DuckDB database files")
    output_format: Literal["duckdb", "parquet"] = Field(
        default="parquet",
        description="Output storage format: 'parquet' (default, Hive-partitioned files) or "
                    "'duckdb' (single database file)"
    )
    parquet_compression: Literal["snappy", "zstd", "gzip"] = Field(
        default="zstd",
        description="Parquet compression algorithm: 'zstd' (default, best ratio), "
                    "'snappy' (fastest), 'gzip' (most compatible)"
    )
    write_mode: Literal["append", "replace"] = Field(
        default="append",
        description="How to handle existing data: 'append' (default) adds new records, "
                    "'replace' truncates table before inserting"
    )
    export_raw_csv: bool = Field(
        default=False,
        description="Export raw staging data to CSV before transformations"
    )
    export_cleaned_csv: bool = Field(
        default=False,
        description="Export cleaned/transformed data to CSV after transformations"
    )
    csv_dir: Optional[Path] = Field(
        default=None,
        description="Directory for CSV exports (default: database_dir/csv_raw or csv_cleaned)"
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
    num_workers: int = Field(
        default=4,
        ge=1,
        le=8,
        description="Number of parallel workers for memory-aware processing (1-8, default: 4). "
                    "Workers use independent DuckDB connections. Set to 1 for serial processing."
    )
    memory_aware_mode: bool = Field(
        default=False,
        description="Enable memory-aware processing mode for large datasets. "
                    "Processes one DBC file at a time with parallel workers, "
                    "exporting immediately to prevent RAM exhaustion."
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
    subsystem: str = Field(
        default="sihsus",
        description="DataSUS subsystem name (sihsus, sim, siasus, etc). Used to organize output directories."
    )
    keep_temp_files: bool = Field(
        default=False,
        description="Keep temporary DBC and DBF files after successful DuckDB export. "
                    "When False (default), temporary files are deleted to save disk space."
    )
    raw_mode: bool = Field(
        default=False,
        description="Export raw data without type conversions or categorical mappings. "
                    "Only applies basic cleaning (remove invisible chars, trim whitespace). "
                    "All columns are kept as VARCHAR. Useful for debugging or custom processing."
    )

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def from_dict(cls, config_dict: dict) -> "PipelineConfig":
        return cls(**config_dict)

    @classmethod
    def create(
        cls,
        base_dir: Path,
        subsystem: str = "sihsus",
        start_date: str = "2023-01-01",
        end_date: Optional[str] = None,
        uf_list: Optional[list[str]] = None,
        override: bool = False,
        chunk_size: int = 10000,
        keep_temp_files: bool = False,
        raw_mode: bool = False,
        num_workers: int = 4,
        memory_aware_mode: bool = False,
        write_mode: Literal["append", "replace"] = "append",
    ) -> "PipelineConfig":
        """Factory method to create PipelineConfig with automatic path configuration.

        Creates a standardized directory structure:
            base_dir/
            ├── {subsystem}.duckdb  (persistent database)
            └── {subsystem}/
                ├── dbc/      (downloaded files - temporary)
                └── dbf/      (converted files - temporary)

        Args:
            base_dir: Base directory for all data (e.g., ./data/datasus)
            subsystem: DataSUS subsystem name (sihsus, sim, siasus, etc)
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (None = today)
            uf_list: List of UF codes (None = all states)
            override: Override existing files
            chunk_size: Rows per chunk for DBF streaming
            keep_temp_files: Keep DBC/DBF files after DuckDB export (default: False)
            raw_mode: Export without type conversions (default: False)
            num_workers: Number of parallel workers (1-8, default: 4)
            memory_aware_mode: Enable memory-aware processing for large datasets
            write_mode: How to handle existing data: 'append' (default) or 'replace'

        Returns:
            Configured PipelineConfig instance
        """
        base_dir = Path(base_dir)
        subsystem_dir = base_dir / subsystem

        return cls(
            download=DownloadConfig(
                output_dir=subsystem_dir / "dbc",
                start_date=start_date,
                end_date=end_date,
                uf_list=uf_list,
                override=override,
            ),
            conversion=ConversionConfig(
                dbc_dir=subsystem_dir / "dbc",
                dbf_dir=subsystem_dir / "dbf",
                override=override,
            ),
            storage=StorageConfig(
                database_dir=base_dir,
                write_mode=write_mode,
            ),
            database=DatabaseConfig(
                chunk_size=chunk_size,
                num_workers=num_workers,
                memory_aware_mode=memory_aware_mode,
            ),
            subsystem=subsystem,
            keep_temp_files=keep_temp_files,
            raw_mode=raw_mode,
        )

    def get_database_path(self) -> Path:
        """Get the path to the DuckDB database file for this subsystem.

        Returns:
            Path to {database_dir}/{subsystem}.duckdb
        """
        return self.storage.database_dir / f"{self.subsystem}.duckdb"

    def get_parquet_dir(self) -> Path:
        """Get the path to the Parquet directory for this subsystem.

        Supports both new 'datasus_db' folder and legacy 'parquet' folder for
        backwards compatibility.

        Returns:
            Path to {database_dir}/datasus_db/{subsystem}/ (or legacy parquet/ if it exists)
        """
        # Check for legacy "parquet" folder first for backwards compatibility
        legacy_path = self.storage.database_dir / "parquet" / self.subsystem
        if legacy_path.exists():
            return legacy_path
        # Default to new "datasus_db" folder
        return self.storage.database_dir / "datasus_db" / self.subsystem

    def is_parquet_mode(self) -> bool:
        """Check if pipeline is configured for Parquet output.

        Returns:
            True if output_format is 'parquet'
        """
        return self.storage.output_format == "parquet"
