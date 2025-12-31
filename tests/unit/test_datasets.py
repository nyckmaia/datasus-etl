"""Tests for datasets module (modular subsystem configurations)."""

import pytest

from datasus_etl.datasets import DatasetConfig, DatasetRegistry, SIHSUSConfig, SIMConfig


class TestDatasetRegistry:
    """Tests for DatasetRegistry class."""

    def test_list_available_datasets(self):
        """Test listing available datasets."""
        datasets = DatasetRegistry.list_available()

        assert "sihsus" in datasets
        assert "sim" in datasets

    def test_get_sihsus_config(self):
        """Test retrieving SIHSUS configuration."""
        config = DatasetRegistry.get("sihsus")

        assert config is not None
        assert config.NAME == "sihsus"
        assert config.FILE_PREFIX == "RD"

    def test_get_sim_config(self):
        """Test retrieving SIM configuration."""
        config = DatasetRegistry.get("sim")

        assert config is not None
        assert config.NAME == "sim"
        assert config.FILE_PREFIX == "DO"

    def test_get_unknown_returns_none(self):
        """Test that unknown dataset returns None."""
        config = DatasetRegistry.get("unknown")
        assert config is None

    def test_case_insensitive_lookup(self):
        """Test that registry lookup is case-insensitive."""
        assert DatasetRegistry.get("SIHSUS") is not None
        assert DatasetRegistry.get("Sihsus") is not None
        assert DatasetRegistry.get("sihsus") is not None


class TestSIHSUSConfig:
    """Tests for SIHSUS dataset configuration."""

    def test_sihsus_name_and_prefix(self):
        """Test SIHSUS name and file prefix."""
        assert SIHSUSConfig.NAME == "sihsus"
        assert SIHSUSConfig.FILE_PREFIX == "RD"

    def test_sihsus_ftp_dirs(self):
        """Test SIHSUS FTP directory configuration."""
        dirs = SIHSUSConfig.get_ftp_dirs()

        assert len(dirs) == 2
        assert any("199201_200712" in d["path"] for d in dirs)
        assert any("200801_" in d["path"] for d in dirs)

    def test_sihsus_file_pattern(self):
        """Test SIHSUS file pattern."""
        pattern = SIHSUSConfig.get_file_pattern()
        assert pattern == "RD*.dbc"

    def test_sihsus_parse_filename_valid(self):
        """Test parsing valid SIHSUS filename."""
        result = SIHSUSConfig.parse_filename("RDSP2301.dbc")

        assert result is not None
        assert result["uf"] == "SP"
        assert result["year"] == 2023
        assert result["month"] == 1
        assert result["source_file"] == "RDSP2301.dbc"

    def test_sihsus_parse_filename_old_year(self):
        """Test parsing SIHSUS filename from old period (1990s)."""
        result = SIHSUSConfig.parse_filename("RDSP9912.dbc")

        assert result is not None
        assert result["uf"] == "SP"
        assert result["year"] == 1999
        assert result["month"] == 12

    def test_sihsus_parse_filename_invalid(self):
        """Test parsing invalid SIHSUS filename."""
        assert SIHSUSConfig.parse_filename("INVALID.dbc") is None
        assert SIHSUSConfig.parse_filename("RD.dbc") is None
        assert SIHSUSConfig.parse_filename("DOSP2023.dbc") is None  # Wrong prefix

    def test_sihsus_parquet_schema(self):
        """Test SIHSUS Parquet schema."""
        schema = SIHSUSConfig.get_parquet_schema()

        assert isinstance(schema, dict)
        assert len(schema) > 100  # SIHSUS has many columns
        assert "uf" in schema
        assert "source_file" in schema
        assert "dt_inter" in schema
        assert "val_tot" in schema

    def test_sihsus_numeric_columns(self):
        """Test getting numeric columns from SIHSUS schema."""
        numeric = SIHSUSConfig.get_numeric_columns()

        assert "idade" in numeric
        assert "val_tot" in numeric
        assert "dias_perm" in numeric
        assert "sexo" not in numeric  # VARCHAR

    def test_sihsus_date_columns(self):
        """Test getting date columns from SIHSUS schema."""
        dates = SIHSUSConfig.get_date_columns()

        assert "dt_inter" in dates
        assert "dt_saida" in dates
        assert "nasc" in dates


class TestSIMConfig:
    """Tests for SIM (mortality) dataset configuration."""

    def test_sim_name_and_prefix(self):
        """Test SIM name and file prefix."""
        assert SIMConfig.NAME == "sim"
        assert SIMConfig.FILE_PREFIX == "DO"

    def test_sim_ftp_dirs(self):
        """Test SIM FTP directory configuration."""
        dirs = SIMConfig.get_ftp_dirs()

        assert len(dirs) == 2
        assert any("CID9" in d["path"] for d in dirs)
        assert any("CID10" in d["path"] for d in dirs)

    def test_sim_file_pattern(self):
        """Test SIM file pattern."""
        pattern = SIMConfig.get_file_pattern()
        assert pattern == "DO*.dbc"

    def test_sim_parse_filename_valid(self):
        """Test parsing valid SIM filename."""
        result = SIMConfig.parse_filename("DOSP2023.dbc")

        assert result is not None
        assert result["uf"] == "SP"
        assert result["year"] == 2023
        assert result["month"] is None  # SIM is yearly
        assert result["source_file"] == "DOSP2023.dbc"

    def test_sim_parse_filename_old_year(self):
        """Test parsing SIM filename from CID-9 period."""
        result = SIMConfig.parse_filename("DORG1990.dbc")

        assert result is not None
        assert result["uf"] == "RG"  # This is the wrong UF format in old files
        assert result["year"] == 1990

    def test_sim_parse_filename_invalid(self):
        """Test parsing invalid SIM filename."""
        assert SIMConfig.parse_filename("INVALID.dbc") is None
        assert SIMConfig.parse_filename("RDSP2023.dbc") is None  # Wrong prefix

    def test_sim_parquet_schema(self):
        """Test SIM Parquet schema."""
        schema = SIMConfig.get_parquet_schema()

        assert isinstance(schema, dict)
        assert len(schema) > 50  # SIM has many columns
        assert "uf" in schema
        assert "source_file" in schema
        assert "dtobito" in schema
        assert "causabas" in schema
