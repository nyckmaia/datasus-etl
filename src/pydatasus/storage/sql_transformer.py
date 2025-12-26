"""SQL-based data transformations for SIHSUS data using DuckDB.

This module replaces the Polars-based SihsusProcessor with pure SQL transformations
executed in DuckDB. All data cleaning, validation, type conversions, and enrichment
are performed in a single streaming SQL query for maximum performance.
"""

import logging
from pathlib import Path
from typing import Optional

import duckdb

from pydatasus.constants.sihsus_schema import SIHSUS_PARQUET_SCHEMA
from pydatasus.exceptions import PyInmetError


class SQLTransformer:
    """SQL-based transformations for SIHSUS data.

    Performs all data transformations using DuckDB SQL queries:
    - Data cleaning (trim, uppercase, remove empty rows)
    - Type validation and conversion
    - Date parsing with multiple format fallback
    - Categorical mappings (SEXO, RACA_COR)
    - Computed columns (ANO_INTER, MES_INTER, DIAS_INTERNACAO)
    - IBGE geographic enrichment

    This replaces the Polars-based SihsusProcessor with streaming SQL execution.

    Example:
        >>> conn = duckdb.connect(":memory:")
        >>> transformer = SQLTransformer(conn)
        >>> transformer.transform_sihsus_data(
        ...     source_table="staging_data",
        ...     target_view="sihsus_processed",
        ...     ibge_data_path=Path("ibge.csv")
        ... )
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Initialize SQL transformer.

        Args:
            conn: DuckDB connection to execute queries on
        """
        self.conn = conn
        self.logger = logging.getLogger(__name__)

    def transform_sihsus_data(
        self,
        source_table: str,
        target_view: str,
        ibge_data_path: Optional[Path] = None,
    ) -> None:
        """Apply all SIHSUS transformations in a single SQL query.

        Creates a DuckDB VIEW with all transformations applied. The VIEW is
        lazy and won't materialize data until queried.

        Transformations applied:
        1. Column cleaning:
           - Remove invisible characters (tabs, newlines, carriage returns, null bytes, form feeds)
           - Trim leading/trailing whitespace
           - Convert to uppercase

        2. Type validation:
           - Safe conversion using TRY_CAST (NULL on failure)
           - Integer conversions: IDADE, QT_DIARIAS, DIAS_PERM
           - Float conversions: VAL_SH, VAL_SP, VAL_TOT, VAL_UTI

        3. Date parsing:
           - Multi-format fallback (YYYYMMDD, DDMMYYYY, YYYY-MM-DD)
           - TRY_CAST with COALESCE for safety

        4. Categorical mappings:
           - SEXO: 0→I, 1→M, 3→F
           - RACA_COR: 01→Branca, 02→Preta, 03→Parda, etc.

        5. Computed columns:
           - ANO_INTER, MES_INTER (extracted from DT_INTER)
           - DIAS_INTERNACAO (difference between DT_SAIDA and DT_INTER)

        6. IBGE enrichment (optional):
           - Adds nome_municipio, uf, regiao via LEFT JOIN

        This replaces:
        - SihsusProcessor._clean_dataframe()
        - SihsusProcessor._validate_dataframe()
        - SihsusProcessor._add_computed_columns()
        - IbgeEnricher.enrich()

        Args:
            source_table: Name of source table/view in DuckDB
            target_view: Name for the transformed view to create
            ibge_data_path: Optional path to IBGE CSV file for geographic enrichment

        Raises:
            PyInmetError: If transformation query fails
        """
        try:
            # Get actual columns from source table (columns vary across files)
            actual_columns = self.conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table}' ORDER BY ordinal_position"
            ).fetchall()
            actual_columns = [col[0] for col in actual_columns]
            self.logger.info(f"Source table has {len(actual_columns)} columns")
            # Load IBGE data if provided
            if ibge_data_path and ibge_data_path.exists():
                self.logger.info(f"Loading IBGE data from {ibge_data_path}")
                self.conn.execute(
                    f"""
                    CREATE TEMP VIEW ibge_data AS
                    SELECT * FROM read_csv('{ibge_data_path}', delim=';', header=true)
                """
                )

            # Build complete transformation query
            # Structure: Single query with all transformations (cleaning, parsing, validation, computed columns)
            # Note: Only clean columns that actually exist in the source table (columns vary across files)

            # Build additional SELECT columns (type conversions, categorical, computed, enrichment)
            additional_selects = []

            # Type conversions
            validated_sql = self._get_validated_columns_sql(actual_columns)
            if validated_sql:
                additional_selects.append(validated_sql)

            # Categorical mappings are now applied directly in _apply_schema_types_sql()
            # No need for separate sexo_descr and raca_cor_descr columns

            # Computed date columns
            if 'DT_INTER' in actual_columns:
                additional_selects.append("EXTRACT(YEAR FROM typed.dt_inter) AS ano_inter")

            # IBGE enrichment
            if ibge_data_path and 'MUNIC_RES' in actual_columns:
                additional_selects.append(self._get_ibge_enrichment_sql())

            # Combine all additional selects
            additional_sql = ",\n                ".join(additional_selects) if additional_selects else ""

            transform_sql = f"""
            CREATE OR REPLACE VIEW {target_view} AS
            WITH cleaned AS (
                SELECT
                    -- Original columns (cleaned) - only columns that exist
                    {self._get_cleaned_columns_sql(actual_columns)},

                    -- Parse dates with fallback to multiple formats (if columns exist)
                    {self._get_date_parsing_sql('DT_INTER') if 'DT_INTER' in actual_columns else 'NULL'} AS dt_inter_parsed,
                    {self._get_date_parsing_sql('DT_SAIDA') if 'DT_SAIDA' in actual_columns else 'NULL'} AS dt_saida_parsed,
                    {self._get_date_parsing_sql('NASC') if 'NASC' in actual_columns else 'NULL'} AS nasc_parsed,
                    {self._get_date_parsing_sql('GESTOR_DT') if 'GESTOR_DT' in actual_columns else 'NULL'} AS gestor_dt_parsed
                FROM {source_table}
            ),
            typed AS (
                SELECT
                    -- Apply schema-based type conversions
                    {self._apply_schema_types_sql(actual_columns)}
                FROM cleaned
                WHERE
                    -- Remove completely empty rows (at least one date must be valid)
                    NOT (cleaned.dt_inter_parsed IS NULL AND cleaned.dt_saida_parsed IS NULL)
            )
            SELECT
                typed.*{', ' if additional_sql else ''}
                {additional_sql}

            FROM typed
            {self._get_ibge_join_sql(actual_columns) if ibge_data_path and 'MUNIC_RES' in actual_columns else ''}
            """

            self.logger.info(f"Creating transformed view: {target_view}")
            self.conn.execute(transform_sql)
            self.logger.info(f"View {target_view} created successfully")

        except Exception as e:
            self.logger.error(f"Transformation failed: {e}")
            raise PyInmetError(f"SQL transformation failed: {e}") from e

    def _get_date_parsing_sql(self, column: str) -> str:
        """Generate SQL for parsing dates with multiple format fallback.

        DuckDB's TRY_CAST doesn't fail on errors, it returns NULL instead.
        We use COALESCE to try multiple formats in order.

        Tries formats in order:
        1. YYYYMMDD (e.g., "20200131")
        2. DDMMYYYY (e.g., "31012020")
        3. YYYY-MM-DD (e.g., "2020-01-31")
        4. Direct cast (fallback)

        Args:
            column: Column name to parse

        Returns:
            SQL expression for date parsing

        Example SQL output:
            COALESCE(
                TRY_CAST(STRPTIME(DT_INTER, '%Y%m%d') AS DATE),
                TRY_CAST(STRPTIME(DT_INTER, '%d%m%Y') AS DATE),
                ...
            )
        """
        return f"""
        COALESCE(
            TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y%m%d') AS DATE),
            TRY_CAST(STRPTIME(NULLIF({column}, ''), '%d%m%Y') AS DATE),
            TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y-%m-%d') AS DATE),
            TRY_CAST(NULLIF({column}, '') AS DATE)
        )
        """

    def _build_clean_expression(self, col: str) -> str:
        """Build SQL expression to clean column value.

        Removes invisible characters:
        - Tabs (CHR(9)) → replaced with space
        - Line feeds (CHR(10)) → replaced with space
        - Carriage returns (CHR(13)) → replaced with space
        - Null bytes (CHR(0)) → removed completely
        - Form feeds (CHR(12)) → removed completely
        - Then applies TRIM (remove outer whitespace)

        Args:
            col: Column name to clean

        Returns:
            SQL expression for cleaned column value

        Example:
            >>> self._build_clean_expression("SEXO")
            'TRIM(REPLACE(...CAST(SEXO AS VARCHAR)...)))'
        """
        return f"""TRIM(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(CAST({col} AS VARCHAR), CHR(9), ' '),
                            CHR(10), ' '),
                        CHR(13), ' '),
                    CHR(0), ''),
                CHR(12), '')
        )"""

    def _get_cleaned_columns_sql(self, actual_columns: list[str]) -> str:
        """Generate SQL for column cleaning.

        Applies comprehensive cleaning to all string columns:
        - Removes invisible characters (tabs, newlines, carriage returns, null bytes, form feeds)
        - Trims leading/trailing whitespace
        - Converts column names to lowercase

        Args:
            actual_columns: List of column names that actually exist in the source table

        Returns:
            SQL column list with cleaning transformations

        Note:
            Column list is dynamically generated based on actual_columns since
            different DATASUS files have different column sets.
        """
        # Generate cleaned expression for each column that exists
        cleaned = []
        for col in actual_columns:
            cleaned.append(f"{self._build_clean_expression(col)} AS {col.lower()}")

        return ",\n                ".join(cleaned)

    def _apply_schema_types_sql(self, actual_columns: list[str]) -> str:
        """Apply type conversions based on SIHSUS_PARQUET_SCHEMA.

        Converts cleaned VARCHAR columns to their target types defined in the schema.
        Uses TRY_CAST for safe conversion (NULL on failure).

        Args:
            actual_columns: List of column names that actually exist in the source table

        Returns:
            SQL column list with type conversions applied

        Note:
            Only converts columns that exist in both actual_columns and SIHSUS_PARQUET_SCHEMA.
            Columns not in schema remain as VARCHAR.
            DATE columns use parsed versions from cleaned CTE.
        """
        typed_columns = []

        # Map of date columns to their parsed versions in cleaned CTE
        date_column_mapping = {
            'dt_inter': 'dt_inter_parsed',
            'dt_saida': 'dt_saida_parsed',
            'nasc': 'nasc_parsed',
            'gestor_dt': 'gestor_dt_parsed'
        }

        for col in actual_columns:
            col_lower = col.lower()

            # Check if column is in schema
            if col_lower in SIHSUS_PARQUET_SCHEMA:
                target_type = SIHSUS_PARQUET_SCHEMA[col_lower]

                # Special handling for different types
                if target_type == "DATE":
                    # Use parsed version with original column name (no suffix)
                    parsed_col = date_column_mapping.get(col_lower, f"{col_lower}_parsed")
                    typed_columns.append(f"cleaned.{parsed_col} AS {col_lower}")
                elif target_type == "BOOLEAN":
                    # Convert 0/1 to boolean
                    typed_columns.append(
                        f"CASE WHEN cleaned.{col_lower} IN ('1', 'true', 'TRUE') THEN TRUE "
                        f"WHEN cleaned.{col_lower} IN ('0', 'false', 'FALSE') THEN FALSE "
                        f"ELSE NULL END AS {col_lower}"
                    )
                elif col_lower == "sexo":
                    # Apply categorical mapping for SEXO (0→I, 1→M, 3→F)
                    typed_columns.append(
                        f"CASE cleaned.{col_lower} "
                        f"WHEN '0' THEN 'I' "
                        f"WHEN '1' THEN 'M' "
                        f"WHEN '3' THEN 'F' "
                        f"ELSE NULL END AS {col_lower}"
                    )
                elif col_lower == "raca_cor":
                    # Apply categorical mapping for RACA_COR
                    typed_columns.append(
                        f"CASE cleaned.{col_lower} "
                        f"WHEN '01' THEN 'Branca' "
                        f"WHEN '02' THEN 'Preta' "
                        f"WHEN '03' THEN 'Parda' "
                        f"WHEN '04' THEN 'Amarela' "
                        f"WHEN '05' THEN 'Indígena' "
                        f"ELSE 'Ignorado' END AS {col_lower}"
                    )
                elif target_type == "VARCHAR":
                    # Already VARCHAR, no conversion needed
                    typed_columns.append(f"cleaned.{col_lower}")
                else:
                    # Numeric types: TRY_CAST for safe conversion
                    typed_columns.append(f"TRY_CAST(cleaned.{col_lower} AS {target_type}) AS {col_lower}")
            else:
                # Column not in schema, keep as VARCHAR
                typed_columns.append(f"cleaned.{col_lower}")

        return ",\n                ".join(typed_columns)

    def _get_validated_columns_sql(self, actual_columns: list[str]) -> str:
        """Generate SQL for type validation and conversion.

        Uses TRY_CAST to safely convert string values to numeric types.
        Invalid values become NULL instead of causing errors.

        Args:
            actual_columns: List of column names that actually exist in the source table

        Returns:
            SQL column list with type conversions (only for columns that exist)
        """
        # NOTE: This method is now deprecated since type conversions are handled by _apply_schema_types_sql()
        # Keeping it for backward compatibility but returning empty string
        # All type conversions now use the schema-based approach
        return ""

    def _get_ibge_enrichment_sql(self) -> str:
        """Generate SQL for IBGE data enrichment columns.

        Returns:
            SQL column list for IBGE data (from JOIN)
        """
        return """
            ibge.nome_municipio,
            ibge.uf AS uf_ibge,
            ibge.regiao
        """

    def _get_ibge_join_sql(self, actual_columns: list[str]) -> str:
        """Generate SQL for IBGE LEFT JOIN.

        Joins on municipality code (MUNIC_RES = codigo_municipio).
        Uses LEFT JOIN so rows without IBGE match are kept.

        Args:
            actual_columns: List of column names that actually exist in the source table

        Returns:
            SQL JOIN clause (empty if MUNIC_RES doesn't exist)
        """
        if 'MUNIC_RES' not in actual_columns:
            return ""

        return """
        LEFT JOIN ibge_data AS ibge
            ON CAST(typed.munic_res AS VARCHAR) = CAST(ibge.codigo_municipio AS VARCHAR)
        """
