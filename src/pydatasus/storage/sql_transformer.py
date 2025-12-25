"""SQL-based data transformations for SIHSUS data using DuckDB.

This module replaces the Polars-based SihsusProcessor with pure SQL transformations
executed in DuckDB. All data cleaning, validation, type conversions, and enrichment
are performed in a single streaming SQL query for maximum performance.
"""

import logging
from pathlib import Path
from typing import Optional

import duckdb

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
            transform_sql = f"""
            CREATE OR REPLACE VIEW {target_view} AS
            SELECT
                -- Original columns (cleaned)
                {self._get_cleaned_columns_sql()},

                -- Type conversions (validated)
                {self._get_validated_columns_sql()},

                -- Categorical mappings
                CASE CAST(SEXO AS VARCHAR)
                    WHEN '0' THEN 'I'
                    WHEN '1' THEN 'M'
                    WHEN '3' THEN 'F'
                    ELSE NULL
                END AS SEXO_DESCR,

                CASE CAST(RACA_COR AS VARCHAR)
                    WHEN '01' THEN 'Branca'
                    WHEN '02' THEN 'Preta'
                    WHEN '03' THEN 'Parda'
                    WHEN '04' THEN 'Amarela'
                    WHEN '05' THEN 'Indígena'
                    ELSE 'Ignorado'
                END AS RACA_COR_DESCR,

                -- Computed columns from dates
                EXTRACT(YEAR FROM dt_inter_parsed) AS ANO_INTER,
                EXTRACT(MONTH FROM dt_inter_parsed) AS MES_INTER,
                DATE_DIFF('day', dt_inter_parsed, dt_saida_parsed) AS DIAS_INTERNACAO,

                -- IBGE enrichment (if available)
                {self._get_ibge_enrichment_sql() if ibge_data_path else 'NULL AS municipio_nome, NULL AS uf_ibge, NULL AS regiao'}

            FROM (
                SELECT
                    *,
                    -- Parse dates with fallback to multiple formats
                    {self._get_date_parsing_sql('DT_INTER')} AS dt_inter_parsed,
                    {self._get_date_parsing_sql('DT_SAIDA')} AS dt_saida_parsed
                FROM {source_table}
            ) AS parsed
            {self._get_ibge_join_sql() if ibge_data_path else ''}
            WHERE
                -- Remove completely empty rows (at least one date must be valid)
                NOT (dt_inter_parsed IS NULL AND dt_saida_parsed IS NULL)
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
            TRY_CAST(STRPTIME({column}, '%Y%m%d') AS DATE),
            TRY_CAST(STRPTIME({column}, '%d%m%Y') AS DATE),
            TRY_CAST(STRPTIME({column}, '%Y-%m-%d') AS DATE),
            TRY_CAST({column} AS DATE)
        )
        """

    def _get_cleaned_columns_sql(self) -> str:
        """Generate SQL for column cleaning (trim, uppercase).

        Applies TRIM and UPPER to all string columns. This standardizes
        data and removes leading/trailing whitespace.

        Returns:
            SQL column list with cleaning transformations

        Note:
            Column list is based on SIHSUS RD (Internações) schema.
            Adjust if processing different DATASUS datasets.
        """
        # SIHSUS RD schema columns (internações hospitalares)
        columns = [
            "UF_ZI",
            "ANO_CMPT",
            "MES_CMPT",
            "ESPEC",
            "CGC_HOSP",
            "N_AIH",
            "IDENT",
            "CEP",
            "MUNIC_RES",
            "NASC",
            "SEXO",
            "UTI_MES_IN",
            "UTI_MES_AN",
            "UTI_MES_AL",
            "UTI_MES_TO",
            "MARCA_UTI",
            "UTI_INT_IN",
            "UTI_INT_AN",
            "UTI_INT_AL",
            "UTI_INT_TO",
            "DIAR_ACOM",
            "QT_DIARIAS",
            "PROC_SOLIC",
            "PROC_REA",
            "VAL_SH",
            "VAL_SP",
            "VAL_SADT",
            "VAL_RN",
            "VAL_ACOMP",
            "VAL_ORTP",
            "VAL_SANGUE",
            "VAL_SADTSR",
            "VAL_TRANSP",
            "VAL_OBSANG",
            "VAL_PED1AC",
            "VAL_TOT",
            "VAL_UTI",
            "US_TOT",
            "DT_INTER",
            "DT_SAIDA",
            "DIAG_PRINC",
            "DIAG_SECUN",
            "COBRANCA",
            "NATUREZA",
            "NAT_JUR",
            "GESTAO",
            "RUBRICA",
            "IND_VDRL",
            "MUNIC_MOV",
            "COD_IDADE",
            "IDADE",
            "DIAS_PERM",
            "MORTE",
            "NACIONAL",
            "NUM_PROC",
            "CAR_INT",
            "TOT_PT_SP",
            "CPF_AUT",
            "HOMONIMO",
            "NUM_FILHOS",
            "INSTRU",
            "CID_NOTIF",
            "CONTRACEP1",
            "CONTRACEP2",
            "GESTRISCO",
            "INSC_PN",
            "SEQ_AIH5",
            "CBOR",
            "CNAES",
            "VINCPREV",
            "GESTOR_COD",
            "GESTOR_TP",
            "GESTOR_CPF",
            "GESTOR_DT",
            "CNES",
            "CNPJ_MANT",
            "INFEHOSP",
            "CID_ASSO",
            "CID_MORTE",
            "COMPLEX",
            "FINANC",
            "FAEC_TP",
            "REGCT",
            "RACA_COR",
            "ETNIA",
            "SEQUENCIA",
            "REMESSA",
            "AUD_JUST",
            "SIS_JUST",
            "VAL_SH_FED",
            "VAL_SP_FED",
            "VAL_SH_GES",
            "VAL_SP_GES",
            "VAL_UCI",
            "MARCA_UCI",
            "DIAGSEC1",
            "DIAGSEC2",
            "DIAGSEC3",
            "DIAGSEC4",
            "DIAGSEC5",
            "DIAGSEC6",
            "DIAGSEC7",
            "DIAGSEC8",
            "DIAGSEC9",
        ]

        # Generate TRIM(UPPER(col)) AS col for each
        cleaned = []
        for col in columns:
            cleaned.append(f"TRIM(UPPER(CAST({col} AS VARCHAR))) AS {col}")

        return ",\n                ".join(cleaned)

    def _get_validated_columns_sql(self) -> str:
        """Generate SQL for type validation and conversion.

        Uses TRY_CAST to safely convert string values to numeric types.
        Invalid values become NULL instead of causing errors.

        Returns:
            SQL column list with type conversions
        """
        return """
            TRY_CAST(IDADE AS INTEGER) AS IDADE_INT,
            TRY_CAST(QT_DIARIAS AS INTEGER) AS QT_DIARIAS_INT,
            TRY_CAST(VAL_SH AS DOUBLE) AS VAL_SH_NUM,
            TRY_CAST(VAL_SP AS DOUBLE) AS VAL_SP_NUM,
            TRY_CAST(VAL_TOT AS DOUBLE) AS VAL_TOT_NUM,
            TRY_CAST(VAL_UTI AS DOUBLE) AS VAL_UTI_NUM,
            TRY_CAST(DIAS_PERM AS INTEGER) AS DIAS_PERM_INT
        """

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

    def _get_ibge_join_sql(self) -> str:
        """Generate SQL for IBGE LEFT JOIN.

        Joins on municipality code (MUNIC_RES = codigo_municipio).
        Uses LEFT JOIN so rows without IBGE match are kept.

        Returns:
            SQL JOIN clause
        """
        return """
        LEFT JOIN ibge_data AS ibge
            ON CAST(parsed.MUNIC_RES AS VARCHAR) = CAST(ibge.codigo_municipio AS VARCHAR)
        """
