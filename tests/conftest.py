"""Shared test fixtures for DataSUS-ETL."""

import tempfile
from pathlib import Path
from typing import Generator

import polars as pl
import pytest


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_sihsus_csv(temp_dir: Path) -> Path:
    """Create a sample SIHSUS CSV file for testing."""
    csv_data = """UF_ZI;MUNIC_RES;SEXO;RACA_COR;DT_INTER;DT_SAIDA;IDADE;QT_DIARIAS;VAL_TOT
SP;350600;1;01;20200115;20200120;45;5;1500.50
RJ;330455;3;02;20200210;20200215;32;5;2300.75
MG;310620;1;04;20200305;20200308;28;3;1200.00
"""

    csv_file = temp_dir / "sample_sihsus.csv"
    csv_file.write_text(csv_data, encoding="utf-8")
    return csv_file


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Create a sample Polars DataFrame for testing."""
    return pl.DataFrame(
        {
            "UF_ZI": ["SP", "RJ", "MG"],
            "MUNIC_RES": ["350600", "330455", "310620"],
            "SEXO": ["1", "3", "1"],
            "RACA_COR": ["01", "02", "04"],
            "DT_INTER": ["20200115", "20200210", "20200305"],
            "DT_SAIDA": ["20200120", "20200215", "20200308"],
            "IDADE": [45, 32, 28],
            "QT_DIARIAS": [5, 5, 3],
            "VAL_TOT": [1500.50, 2300.75, 1200.00],
        }
    )


@pytest.fixture
def sample_ibge_data(temp_dir: Path) -> Path:
    """Create a sample IBGE municipality data CSV."""
    ibge_data = """cod_municipio,uf,nome_municipio,regiao
3506003,SP,Município 1 - SP,Sudeste
3304557,RJ,Município 2 - RJ,Sudeste
3106200,MG,Município 3 - MG,Sudeste
"""

    ibge_file = temp_dir / "ibge_municipios.csv"
    ibge_file.write_text(ibge_data, encoding="utf-8")
    return ibge_file


@pytest.fixture
def mock_dbc_file(temp_dir: Path) -> Path:
    """Create a mock DBC file for testing."""
    dbc_file = temp_dir / "RDSP2001.dbc"
    dbc_file.write_bytes(b"MOCK_DBC_DATA")
    return dbc_file


@pytest.fixture
def mock_dbf_file(temp_dir: Path) -> Path:
    """Create a mock DBF file for testing."""
    dbf_file = temp_dir / "RDSP2001.dbf"
    dbf_file.write_bytes(b"MOCK_DBF_DATA")
    return dbf_file
