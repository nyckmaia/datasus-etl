"""IBGE municipality data loader.

Loads municipality reference data from the bundled IBGE DTB 2024 Excel file.
Used for enriching SIHSUS data with geographic information.
"""

import logging
import unicodedata
from functools import lru_cache
from pathlib import Path
from typing import Optional

import xlrd

logger = logging.getLogger(__name__)

# Mapping of Brazilian state names to their 2-letter abbreviations
UF_SIGLA_MAP = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}


def normalize_column_name(name: str) -> str:
    """Normalize column name: lowercase, no accents, spaces -> underscores.

    Args:
        name: Original column name (e.g., "Codigo Municipio Completo")

    Returns:
        Normalized name (e.g., "codigo_municipio_completo")
    """
    # NFD normalization decomposes accented chars into base + diacritical mark
    normalized = unicodedata.normalize("NFD", name)
    # Remove combining diacritical marks (unicode category 'Mn')
    ascii_only = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    # Lowercase, strip whitespace, and replace spaces with underscores
    return ascii_only.lower().strip().replace(" ", "_")


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
        data_path = files("datasus_etl._data.ibge").joinpath(
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

            # IBGE 7-digit code format: UUMMMMV where:
            # - UU = UF code (2 digits)
            # - MMMM = municipality code (4 digits)
            # - V = verification digit (LAST digit, to be removed)
            # SIHSUS munic_res uses first 6 digits (removes last verification digit)
            # Example: 3505005 -> 350500 (Barão de Antonina, SP)
            # Example: 3550308 -> 355030 (São Paulo capital)
            codigo_6 = int(codigo_str[:-1]) if len(codigo_str) >= 7 else int(codigo_str[:6])

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


def generate_ibge_parquet(output_path: Path) -> Path:
    """Generate Parquet file with IBGE municipality data.

    Reads the bundled IBGE DTB 2024 Excel file and exports it to Parquet format
    with normalized column names (lowercase, no accents, spaces -> underscores).

    The Excel file structure:
    - 6 header/prefix rows to skip
    - Column names on row 7 (0-indexed: row 6)
    - Data starts at row 8 (0-indexed: row 7)

    Args:
        output_path: Path for output Parquet file

    Returns:
        Path to the generated Parquet file
    """
    import duckdb

    excel_path = get_ibge_data_path()
    logger.info(f"Generating IBGE Parquet from {excel_path}")

    # Load workbook using xlrd (for .xls format)
    workbook = xlrd.open_workbook(str(excel_path))
    sheet = workbook.sheet_by_name("DTB_Municípios")

    # Row 7 (0-indexed: row 6) contains the column names
    header_row = sheet.row_values(6)
    all_columns = [normalize_column_name(str(col)) for col in header_row]

    # Filter out empty column names and track valid column indices
    valid_indices = [i for i, col in enumerate(all_columns) if col]
    columns = [all_columns[i] for i in valid_indices]

    logger.info(f"Normalized column names: {columns}")

    # Data starts at row 8 (0-indexed: row 7)
    data = []
    for row_idx in range(7, sheet.nrows):
        try:
            row = sheet.row_values(row_idx)
            # Convert floats to int where appropriate (numeric codes)
            # Only include values for valid columns
            processed_row = []
            for i in valid_indices:
                val = row[i] if i < len(row) else None
                if isinstance(val, float) and val == int(val):
                    processed_row.append(int(val))
                elif isinstance(val, str):
                    processed_row.append(val.strip())
                else:
                    processed_row.append(val)
            data.append(processed_row)
        except (IndexError, ValueError, TypeError) as e:
            logger.warning(f"Error processing row {row_idx}: {e}")
            continue

    logger.info(f"Loaded {len(data)} rows from Excel")

    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use in-memory DuckDB to create the Parquet file
    conn = duckdb.connect(":memory:")

    # Build column definitions for table creation
    # All columns are VARCHAR except numeric codes
    col_defs = []
    for col in columns:
        if col in ("uf", "regiao_geografica_intermediaria", "regiao_geografica_imediata",
                   "municipio", "codigo_municipio_completo"):
            col_defs.append(f'"{col}" INTEGER')
        else:
            col_defs.append(f'"{col}" VARCHAR')

    # Create table
    create_sql = f"CREATE TABLE ibge_data ({', '.join(col_defs)})"
    conn.execute(create_sql)

    # Insert data in batches
    if data:
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f"INSERT INTO ibge_data VALUES ({placeholders})"

        for row in data:
            try:
                conn.execute(insert_sql, row)
            except Exception as e:
                logger.warning(f"Error inserting row: {e}")
                continue

    # Build CASE statement for sigla_uf mapping
    case_clauses = [f"WHEN nome_uf = '{nome}' THEN '{sigla}'" for nome, sigla in UF_SIGLA_MAP.items()]
    sigla_case = "CASE " + " ".join(case_clauses) + " ELSE NULL END"

    # Export to Parquet with additional derived columns:
    # - sigla_uf: 2-letter state abbreviation
    # - codigo_municipio_6_digitos: 6-digit code for JOIN with SIHSUS/SIM munic_res
    conn.execute(f"""
        COPY (
            SELECT
                *,
                {sigla_case} AS sigla_uf,
                CAST(FLOOR(codigo_municipio_completo / 10) AS INTEGER) AS codigo_municipio_6_digitos
            FROM ibge_data
        )
        TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION 'zstd')
    """)

    row_count = conn.execute("SELECT COUNT(*) FROM ibge_data").fetchone()[0]
    conn.close()

    logger.info(f"Generated IBGE Parquet at {output_path} with {row_count} rows")
    logger.info("Added derived columns: sigla_uf, codigo_municipio_6_digitos")

    return output_path
