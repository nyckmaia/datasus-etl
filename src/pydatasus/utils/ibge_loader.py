"""IBGE municipality data loader.

Loads municipality reference data from the bundled IBGE DTB 2024 Excel file.
Used for enriching SIHSUS data with geographic information.
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Optional

import xlrd

logger = logging.getLogger(__name__)


def get_ibge_data_path() -> Path:
    """Get path to the bundled IBGE DTB 2024 Excel file.

    Returns:
        Path to the Excel file

    Raises:
        FileNotFoundError: If the data file is not found
    """
    # Try importlib.resources first (Python 3.9+)
    try:
        from importlib.resources import files
        data_path = files("pydatasus._data.ibge").joinpath(
            "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls"
        )
        # Convert to Path
        path = Path(str(data_path))
        if path.exists():
            return path
    except (ImportError, TypeError, AttributeError):
        pass

    # Fallback: relative to this file
    package_dir = Path(__file__).parent.parent
    fallback_path = package_dir / "_data" / "ibge" / "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls"

    if fallback_path.exists():
        return fallback_path

    raise FileNotFoundError(
        f"IBGE DTB file not found. Expected at: {fallback_path}\n"
        "Please ensure the file is included in the package installation."
    )


@lru_cache(maxsize=1)
def load_ibge_municipalities() -> dict[int, dict[str, str]]:
    """Load IBGE municipality data from bundled Excel file.

    The Excel file has:
    - 6 header/prefix rows to skip
    - Column names on row 7 (0-indexed: row 6)
    - Data starts at row 8 (0-indexed: row 7)
    - Columns: UF, Nome_UF, Regiao Geografica Intermediaria, Nome Regiao...,
               Regiao Geografica Imediata, Nome Regiao..., Municipio,
               Codigo Municipio Completo, Nome_Municipio

    Returns:
        Dict mapping 6-digit municipality code to enrichment data:
        {
            350600: {
                "municipio": "Sao Paulo",
                "uf": "Sao Paulo",
                "rg_imediata": "Sao Paulo",
                "rg_intermediaria": "Sao Paulo"
            },
            ...
        }
    """
    excel_path = get_ibge_data_path()
    logger.info(f"Loading IBGE municipality data from {excel_path}")

    # Load workbook using xlrd (for .xls format)
    workbook = xlrd.open_workbook(str(excel_path))
    sheet = workbook.sheet_by_name("DTB_Municípios")

    municipalities: dict[int, dict[str, str]] = {}

    # Data starts at row 7 (0-indexed), skip header rows
    for row_idx in range(7, sheet.nrows):
        try:
            row = sheet.row_values(row_idx)

            # Column indices (0-based):
            # 0: UF (code)
            # 1: Nome_UF
            # 2: Regiao Geografica Intermediaria (code)
            # 3: Nome Regiao Geografica Intermediaria
            # 4: Regiao Geografica Imediata (code)
            # 5: Nome Regiao Geografica Imediata
            # 6: Municipio (partial code)
            # 7: Codigo Municipio Completo (7 digits)
            # 8: Nome_Municipio

            codigo_completo = row[7]  # 7-digit code
            if not codigo_completo:
                continue

            # Convert to string and extract 6-digit code
            # xlrd may return float for numbers
            if isinstance(codigo_completo, float):
                codigo_str = str(int(codigo_completo))
            else:
                codigo_str = str(codigo_completo).strip()

            if len(codigo_str) < 6:
                continue

            # IBGE 7-digit code format: D_UUMMMMC where:
            # - D = verification digit (first digit, to be removed)
            # - UU = UF code (2 digits)
            # - MMMM = municipality code (4 digits)
            # - C = check digit or part of code
            # SIHSUS munic_res uses first 6 digits after removing verification digit
            # Example: 3550308 -> 550308 (São Paulo capital)
            codigo_6 = int(codigo_str[1:7]) if len(codigo_str) >= 7 else int(codigo_str[-6:])

            municipalities[codigo_6] = {
                "municipio": str(row[8]).strip() if row[8] else None,
                "uf": str(row[1]).strip() if row[1] else None,
                "rg_imediata": str(row[5]).strip() if row[5] else None,
                "rg_intermediaria": str(row[3]).strip() if row[3] else None,
            }

        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"Error parsing row {row_idx}: {e}")
            continue

    logger.info(f"Loaded {len(municipalities)} municipalities from IBGE data")

    return municipalities


def create_ibge_lookup_csv(output_path: Optional[Path] = None) -> Path:
    """Create a CSV file with IBGE lookup data for DuckDB import.

    Args:
        output_path: Path for output CSV. If None, creates temp file.

    Returns:
        Path to the created CSV file
    """
    import csv
    import os
    import tempfile

    if output_path is None:
        fd, temp_path = tempfile.mkstemp(suffix=".csv", prefix="ibge_lookup_")
        os.close(fd)  # Close the file descriptor immediately
        output_path = Path(temp_path)

    municipalities = load_ibge_municipalities()

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["codigo_municipio", "municipio_res", "uf_res", "rg_imediata_res", "rg_intermediaria_res"])

        for codigo, data in municipalities.items():
            writer.writerow([
                codigo,
                data.get("municipio") or "",
                data.get("uf") or "",
                data.get("rg_imediata") or "",
                data.get("rg_intermediaria") or "",
            ])

    logger.info(f"Created IBGE lookup CSV at {output_path}")
    return output_path
