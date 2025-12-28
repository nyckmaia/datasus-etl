"""Tests for configuration module."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from pydatasus.config import (
    ConversionConfig,
    DatabaseConfig,
    DownloadConfig,
    PipelineConfig,
    ProcessingConfig,
    StorageConfig,
)


class TestDownloadConfig:
    """Tests for DownloadConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = DownloadConfig()

        # Check that path contains the expected structure (without checking absolute path)
        assert "data" in str(config.output_dir) or "datasus" in str(config.output_dir)
        assert config.start_date == "2000-01-01"
        assert config.end_date is None
        assert config.uf_list is None
        assert config.override is False
        assert config.timeout == 60
        assert config.max_retries == 3
        assert config.incremental_files is None

    def test_custom_config(self, temp_dir: Path):
        """Test custom configuration."""
        config = DownloadConfig(
            output_dir=temp_dir,
            start_date="2020-01-01",
            end_date="2020-12-31",
            uf_list=["SP", "RJ"],
            override=True,
            timeout=120,
            max_retries=5,
        )

        assert config.output_dir == temp_dir.resolve()
        assert config.start_date == "2020-01-01"
        assert config.end_date == "2020-12-31"
        assert config.uf_list == ["SP", "RJ"]
        assert config.override is True
        assert config.timeout == 120
        assert config.max_retries == 5

    def test_invalid_uf_validation(self):
        """Test that invalid UF codes raise validation error."""
        with pytest.raises(ValidationError) as exc_info:
            DownloadConfig(uf_list=["XX", "YY"])

        assert "Invalid UF codes" in str(exc_info.value)

    def test_valid_uf_validation(self):
        """Test that valid UF codes pass validation."""
        config = DownloadConfig(uf_list=["SP", "RJ", "MG"])
        assert config.uf_list == ["SP", "RJ", "MG"]


class TestConversionConfig:
    """Tests for ConversionConfig."""

    def test_conversion_config(self, temp_dir: Path):
        """Test conversion configuration."""
        dbc_dir = temp_dir / "dbc"
        dbf_dir = temp_dir / "dbf"
        csv_dir = temp_dir / "csv"
        tabwin_dir = Path("C:/Program Files/TAB415")

        config = ConversionConfig(
            dbc_dir=dbc_dir,
            dbf_dir=dbf_dir,
            csv_dir=csv_dir,
            tabwin_dir=tabwin_dir,
        )

        assert config.dbc_dir == dbc_dir
        assert config.dbf_dir == dbf_dir
        assert config.csv_dir == csv_dir
        assert config.tabwin_dir == tabwin_dir
        assert config.override is False
        assert config.max_workers is None


class TestProcessingConfig:
    """Tests for ProcessingConfig."""

    def test_processing_config(self, temp_dir: Path):
        """Test processing configuration."""
        input_dir = temp_dir / "input"
        output_dir = temp_dir / "output"

        config = ProcessingConfig(
            input_dir=input_dir,
            output_dir=output_dir,
            override=True,
            max_workers=4,
        )

        assert config.input_dir == input_dir
        assert config.output_dir == output_dir
        assert config.override is True
        assert config.max_workers == 4


class TestStorageConfig:
    """Tests for StorageConfig."""

    def test_default_storage_config(self, temp_dir: Path):
        """Test default storage configuration."""
        config = StorageConfig(parquet_dir=temp_dir)

        assert config.parquet_dir == temp_dir
        assert config.partition_cols == ["uf"]  # Default is now just UF
        assert config.compression == "snappy"
        assert config.row_group_size == 128_000_000  # Default is 128MB

    def test_custom_storage_config(self, temp_dir: Path):
        """Test custom storage configuration."""
        config = StorageConfig(
            parquet_dir=temp_dir,
            partition_cols=["uf", "year"],
            compression="gzip",
            row_group_size=50_000_000,
        )

        assert config.partition_cols == ["uf", "year"]
        assert config.compression == "gzip"
        assert config.row_group_size == 50_000_000


class TestDatabaseConfig:
    """Tests for DatabaseConfig."""

    def test_default_database_config(self):
        """Test default database configuration."""
        config = DatabaseConfig()

        assert config.db_path is None
        assert config.read_only is False
        assert config.threads is None

    def test_custom_database_config(self, temp_dir: Path):
        """Test custom database configuration."""
        db_path = temp_dir / "test.duckdb"

        config = DatabaseConfig(
            db_path=db_path,
            read_only=True,
            threads=4,
        )

        assert config.db_path == db_path
        assert config.read_only is True
        assert config.threads == 4


class TestPipelineConfig:
    """Tests for PipelineConfig."""

    def test_complete_pipeline_config(self, temp_dir: Path):
        """Test complete pipeline configuration."""
        config = PipelineConfig(
            download=DownloadConfig(output_dir=temp_dir / "dbc"),
            conversion=ConversionConfig(
                dbc_dir=temp_dir / "dbc",
                dbf_dir=temp_dir / "dbf",
                csv_dir=temp_dir / "csv",
                tabwin_dir=Path("."),
            ),
            processing=ProcessingConfig(
                input_dir=temp_dir / "csv",
                output_dir=temp_dir / "processed",
            ),
            storage=StorageConfig(parquet_dir=temp_dir / "parquet"),
            database=DatabaseConfig(db_path=temp_dir / "test.duckdb"),
        )

        assert isinstance(config.download, DownloadConfig)
        assert isinstance(config.conversion, ConversionConfig)
        assert isinstance(config.processing, ProcessingConfig)
        assert isinstance(config.storage, StorageConfig)
        assert isinstance(config.database, DatabaseConfig)

    def test_pipeline_config_from_dict(self, temp_dir: Path):
        """Test creating pipeline config from dictionary."""
        config_dict = {
            "download": {
                "output_dir": temp_dir / "dbc",
                "start_date": "2020-01-01",
            },
            "conversion": {
                "dbc_dir": temp_dir / "dbc",
                "dbf_dir": temp_dir / "dbf",
                "csv_dir": temp_dir / "csv",
                "tabwin_dir": Path("."),
            },
            "processing": {
                "input_dir": temp_dir / "csv",
                "output_dir": temp_dir / "processed",
            },
            "storage": {
                "parquet_dir": temp_dir / "parquet",
            },
        }

        config = PipelineConfig.from_dict(config_dict)

        assert isinstance(config, PipelineConfig)
        assert config.download.start_date == "2020-01-01"
