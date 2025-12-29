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

        5. IBGE enrichment (optional):
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

            # Computed date columns (NOTE: column names are now lowercase due to DBF column cleaning)
            if 'dt_inter' in actual_columns:
                additional_selects.append("EXTRACT(YEAR FROM typed.dt_inter) AS ano_inter")

            # IBGE enrichment
            if ibge_data_path and 'munic_res' in actual_columns:
                additional_selects.append(self._get_ibge_enrichment_sql())

            # Build final SELECT clause
            if additional_selects:
                joined_selects = ",\n    ".join(additional_selects)
                additional_sql = f",\n    {joined_selects}"
            else:
                additional_sql = ""

            # Build IBGE JOIN clause if needed
            ibge_join_clause = ""
            if ibge_data_path and 'munic_res' in actual_columns:
                ibge_join_clause = self._get_ibge_join_sql(actual_columns)

            transform_sql = f"""
CREATE OR REPLACE VIEW {target_view} AS
WITH cleaned AS (
    SELECT
        -- Original columns (cleaned) - only columns that exist
        {self._get_cleaned_columns_sql(actual_columns)},

        -- Parse dates with fallback to multiple formats and future date validation (if columns exist)
        {self._get_date_parsing_sql('dt_inter', allow_future=False) if 'dt_inter' in actual_columns else 'NULL'} AS dt_inter_parsed,
        {self._get_date_parsing_sql('dt_saida', allow_future=False) if 'dt_saida' in actual_columns else 'NULL'} AS dt_saida_parsed,
        {self._get_date_parsing_sql('nasc', allow_future=False) if 'nasc' in actual_columns else 'NULL'} AS nasc_parsed,
        {self._get_date_parsing_sql('gestor_dt', allow_future=False) if 'gestor_dt' in actual_columns else 'NULL'} AS gestor_dt_parsed
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
    typed.*{additional_sql}
FROM typed{ibge_join_clause}
"""

            from tqdm import tqdm
            tqdm.write(f"[TRANSFORM] Applying SQL transformations to create view: {target_view}...")
            self.logger.info(f"Creating transformed view: {target_view}")

            # Debug: Log the SQL query for troubleshooting
            self.logger.debug(f"Transform SQL:\n{transform_sql}")
            self.logger.debug(f"SQL length: {len(transform_sql)} characters")
            self.logger.debug(f"SQL first 200 chars: {transform_sql[:200]}")
            self.logger.debug(f"SQL last 200 chars: {transform_sql[-200:]}")

            try:
                self.conn.execute(transform_sql)
                tqdm.write(f"[TRANSFORM] View {target_view} created successfully\n")
            except Exception as sql_error:
                # Enhanced error logging with SQL details
                self.logger.error(f"Failed SQL query (length={len(transform_sql)}):\n{transform_sql}")
                self.logger.error(f"First 300 chars:\n{transform_sql[:300]}")
                self.logger.error(f"Last 300 chars:\n{transform_sql[-300:]}")
                self.logger.error(f"SQL ends with: {repr(transform_sql[-50:])}")

                # Save SQL to file for debugging
                import tempfile
                sql_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.sql')
                sql_file.write(transform_sql)
                sql_file.close()
                self.logger.error(f"Full SQL saved to: {sql_file.name}")

                # Check for "val_uci ci" pattern
                if 'val_uci ci' in transform_sql:
                    idx = transform_sql.find('val_uci ci')
                    self.logger.error(f"Found 'val_uci ci' at index {idx}")
                    self.logger.error(f"Context: {repr(transform_sql[max(0,idx-50):idx+50])}")

                raise

            self.logger.info(f"View {target_view} created successfully")

        except Exception as e:
            self.logger.error(f"Transformation failed: {e}")
            raise PyInmetError(f"SQL transformation failed: {e}") from e

    def _get_date_parsing_sql(self, column: str, allow_future: bool = False) -> str:
        """Generate SQL for parsing dates with multiple format fallback and future date validation.

        DuckDB's TRY_CAST doesn't fail on errors, it returns NULL instead.
        We use COALESCE to try multiple formats in order.

        Tries formats in order:
        1. YYYYMMDD (e.g., "20200131")
        2. DDMMYYYY (e.g., "31012020")
        3. YYYY-MM-DD (e.g., "2020-01-31")
        4. Direct cast (fallback)

        When allow_future=False, validates that parsed dates are not in the future.
        This prevents ambiguous dates like "20260115" from being parsed as 2026-01-15
        when they might actually be 2015-01-26 (DDMMYYYY format).

        Args:
            column: Column name to parse
            allow_future: Whether to allow dates in the future (default: False)

        Returns:
            SQL expression for date parsing with optional validation

        Example SQL output (allow_future=False):
            COALESCE(
                CASE WHEN TRY_CAST(...) <= CURRENT_DATE THEN TRY_CAST(...) ELSE NULL END,
                ...
            )
        """
        if allow_future:
            # Original logic without future date validation
            return f"""
            COALESCE(
                TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y%m%d') AS DATE),
                TRY_CAST(STRPTIME(NULLIF({column}, ''), '%d%m%Y') AS DATE),
                TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y-%m-%d') AS DATE),
                TRY_CAST(NULLIF({column}, '') AS DATE)
            )
            """
        else:
            # Enhanced logic with future date rejection
            return f"""
            COALESCE(
                -- Try format 1: YYYYMMDD with date validation
                CASE
                    WHEN TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y%m%d') AS DATE) IS NOT NULL
                         AND TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y%m%d') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y%m%d') AS DATE)
                    ELSE NULL
                END,
                -- Try format 2: DDMMYYYY with date validation
                CASE
                    WHEN TRY_CAST(STRPTIME(NULLIF({column}, ''), '%d%m%Y') AS DATE) IS NOT NULL
                         AND TRY_CAST(STRPTIME(NULLIF({column}, ''), '%d%m%Y') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(STRPTIME(NULLIF({column}, ''), '%d%m%Y') AS DATE)
                    ELSE NULL
                END,
                -- Try format 3: YYYY-MM-DD with date validation
                CASE
                    WHEN TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y-%m-%d') AS DATE) IS NOT NULL
                         AND TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y-%m-%d') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(STRPTIME(NULLIF({column}, ''), '%Y-%m-%d') AS DATE)
                    ELSE NULL
                END,
                -- Fallback: direct cast with validation
                CASE
                    WHEN TRY_CAST(NULLIF({column}, '') AS DATE) IS NOT NULL
                         AND TRY_CAST(NULLIF({column}, '') AS DATE) <= CURRENT_DATE
                    THEN TRY_CAST(NULLIF({column}, '') AS DATE)
                    ELSE NULL
                END
            )
            """

    def _build_clean_expression(self, col: str) -> str:
        """Build SQL expression to clean column value.

        Cleaning steps:
        1. CAST to VARCHAR
        2. Remove invisible characters (\0, \t, \n, \r, form feeds)
        3. TRIM whitespace (left and right)
        4. Convert empty strings to NULL using NULLIF

        Removes invisible characters:
        - Tabs (CHR(9)) → removed completely
        - Line feeds (CHR(10)) → removed completely
        - Carriage returns (CHR(13)) → removed completely
        - Null bytes (CHR(0)) → removed completely
        - Form feeds (CHR(12)) → removed completely
        - Then applies TRIM (remove outer whitespace)
        - Finally converts empty strings to NULL

        Args:
            col: Column name to clean

        Returns:
            SQL expression that returns cleaned VARCHAR or NULL

        Example:
            Input: '  \t  \n  '  → Output: NULL
            Input: '  data  '    → Output: 'data'
            Input: ''            → Output: NULL
        """
        return f"""NULLIF(
            TRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(CAST({col} AS VARCHAR), CHR(9), ''),
                                CHR(10), ''),
                            CHR(13), ''),
                        CHR(0), ''),
                    CHR(12), '')
            ),
            ''
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

        return ",\n        ".join(cleaned)

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
                    sql_fragment = f"TRY_CAST(cleaned.{col_lower} AS {target_type}) AS {col_lower}"
                    typed_columns.append(sql_fragment)
                    # Debug specific column
                    if col_lower == 'val_uci':
                        self.logger.debug(f"val_uci SQL fragment: {repr(sql_fragment)}")
            else:
                # Column not in schema, keep as VARCHAR
                typed_columns.append(f"cleaned.{col_lower}")

        return ",\n        ".join(typed_columns)

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
        return "ibge.nome_municipio,\n    ibge.uf AS uf_ibge,\n    ibge.regiao"

    def _get_ibge_join_sql(self, actual_columns: list[str]) -> str:
        """Generate SQL for IBGE LEFT JOIN.

        Joins on municipality code (MUNIC_RES = codigo_municipio).
        Uses LEFT JOIN so rows without IBGE match are kept.

        Args:
            actual_columns: List of column names that actually exist in the source table

        Returns:
            SQL JOIN clause (empty if munic_res doesn't exist)
        """
        if 'munic_res' not in actual_columns:
            return ""

        return "\nLEFT JOIN ibge_data AS ibge\n    ON CAST(typed.munic_res AS VARCHAR) = CAST(ibge.codigo_municipio AS VARCHAR)"

    def _generate_canonical_column_sql(self, actual_columns: list[str]) -> str:
        """Generate SQL SELECT with ALL columns from SIHSUS_PARQUET_SCHEMA.

        This ensures a consistent schema across all Parquet files, regardless of
        which columns exist in the source DBF. Missing columns are filled with
        NULL cast to the appropriate type.

        Args:
            actual_columns: List of column names that actually exist in the source table

        Returns:
            SQL column list with all canonical columns, properly typed

        Example:
            If DBF has columns [uf, n_aih, dt_inter] but schema has 100 columns,
            this generates:
                typed.uf,
                typed.n_aih,
                typed.dt_inter,
                NULL::INTEGER AS uf_zi,  -- missing column with correct type
                NULL::SMALLINT AS ano_cmpt,  -- missing column with correct type
                ...
        """
        actual_columns_lower = {col.lower() for col in actual_columns}
        canonical_columns = []

        # Map of date columns to their parsed versions
        date_column_mapping = {
            'dt_inter': 'dt_inter_parsed',
            'dt_saida': 'dt_saida_parsed',
            'nasc': 'nasc_parsed',
            'gestor_dt': 'gestor_dt_parsed'
        }

        for col_name, col_type in SIHSUS_PARQUET_SCHEMA.items():
            if col_name in actual_columns_lower:
                # Column exists in source - use typed version
                if col_type == "DATE" and col_name in date_column_mapping:
                    # Date columns use parsed version
                    canonical_columns.append(f"typed.{col_name}")
                else:
                    canonical_columns.append(f"typed.{col_name}")
            else:
                # Column doesn't exist - generate NULL with correct type
                canonical_columns.append(f"NULL::{col_type} AS {col_name}")

        return ",\n        ".join(canonical_columns)

    def transform_to_canonical_view(
        self,
        source_table: str,
        target_view: str,
        ibge_data_path: Optional[Path] = None,
    ) -> None:
        """Apply SIHSUS transformations and normalize to canonical schema.

        Similar to transform_sihsus_data() but ensures the output view has
        ALL columns from SIHSUS_PARQUET_SCHEMA, with missing columns as NULL.
        This enables consistent schema across all Parquet files when using
        partitioned writes with APPEND.

        Args:
            source_table: Name of source table/view in DuckDB
            target_view: Name for the transformed view to create
            ibge_data_path: Optional path to IBGE CSV file for geographic enrichment

        Raises:
            PyInmetError: If transformation query fails
        """
        try:
            # Get actual columns from source table
            actual_columns = self.conn.execute(
                f"SELECT column_name FROM information_schema.columns WHERE table_name = '{source_table}' ORDER BY ordinal_position"
            ).fetchall()
            actual_columns = [col[0] for col in actual_columns]
            self.logger.info(f"Source table has {len(actual_columns)} columns, normalizing to {len(SIHSUS_PARQUET_SCHEMA)} canonical columns")

            # Load IBGE data if provided
            if ibge_data_path and ibge_data_path.exists():
                self.logger.info(f"Loading IBGE data from {ibge_data_path}")
                self.conn.execute(
                    f"""
                    CREATE OR REPLACE TEMP VIEW ibge_data AS
                    SELECT * FROM read_csv('{ibge_data_path}', delim=';', header=true)
                """
                )

            # Build additional computed columns
            additional_selects = []
            actual_columns_lower = {col.lower() for col in actual_columns}

            # IBGE enrichment
            if ibge_data_path and 'munic_res' in actual_columns_lower:
                additional_selects.append(self._get_ibge_enrichment_sql())

            # Build final SELECT clause
            additional_sql = ""
            if additional_selects:
                joined_selects = ",\n        ".join(additional_selects)
                additional_sql = f",\n        {joined_selects}"

            # Build IBGE JOIN clause if needed
            ibge_join_clause = ""
            if ibge_data_path and 'munic_res' in actual_columns_lower:
                ibge_join_clause = self._get_ibge_join_sql(actual_columns)

            # Generate canonical schema SELECT
            canonical_select = self._generate_canonical_column_sql(actual_columns)

            transform_sql = f"""
CREATE OR REPLACE VIEW {target_view} AS
WITH cleaned AS (
    SELECT
        -- Original columns (cleaned) - only columns that exist
        {self._get_cleaned_columns_sql(actual_columns)},

        -- Parse dates with fallback to multiple formats and future date validation
        {self._get_date_parsing_sql('dt_inter', allow_future=False) if 'dt_inter' in actual_columns_lower else 'NULL'} AS dt_inter_parsed,
        {self._get_date_parsing_sql('dt_saida', allow_future=False) if 'dt_saida' in actual_columns_lower else 'NULL'} AS dt_saida_parsed,
        {self._get_date_parsing_sql('nasc', allow_future=False) if 'nasc' in actual_columns_lower else 'NULL'} AS nasc_parsed,
        {self._get_date_parsing_sql('gestor_dt', allow_future=False) if 'gestor_dt' in actual_columns_lower else 'NULL'} AS gestor_dt_parsed
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
),
canonical AS (
    SELECT
        -- All columns from canonical schema (missing columns as NULL with correct type)
        {canonical_select}{additional_sql}
    FROM typed{ibge_join_clause}
)
SELECT * FROM canonical
"""

            from tqdm import tqdm
            tqdm.write(f"[TRANSFORM] Applying SQL transformations with canonical schema to create view: {target_view}...")
            self.logger.info(f"Creating canonical transformed view: {target_view}")
            self.logger.debug(f"Transform SQL length: {len(transform_sql)} characters")

            try:
                self.conn.execute(transform_sql)
                tqdm.write(f"[TRANSFORM] Canonical view {target_view} created successfully\n")
            except Exception as sql_error:
                self.logger.error(f"Failed SQL query (length={len(transform_sql)}):\n{transform_sql}")
                import tempfile
                sql_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, suffix='.sql')
                sql_file.write(transform_sql)
                sql_file.close()
                self.logger.error(f"Full SQL saved to: {sql_file.name}")
                raise

            self.logger.info(f"Canonical view {target_view} created successfully")

        except Exception as e:
            self.logger.error(f"Canonical transformation failed: {e}")
            raise PyInmetError(f"SQL canonical transformation failed: {e}") from e
