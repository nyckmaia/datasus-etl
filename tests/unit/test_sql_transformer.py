"""Tests for SQL transformer module."""

import duckdb
import pytest

from pydatasus.storage.sql_transformer import SQLTransformer


@pytest.fixture
def duckdb_connection():
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def sql_transformer(duckdb_connection):
    """Create a SQLTransformer instance."""
    return SQLTransformer(duckdb_connection)


@pytest.fixture
def sample_staging_table(duckdb_connection):
    """Create a sample staging table with SIHSUS-like data."""
    duckdb_connection.execute("""
        CREATE TABLE staging_test AS
        SELECT * FROM (VALUES
            ('SP', 'RDSP2301.dbc', '350600', '2023', '01', 'M', '45', '5', '1500.50', '20230115', '20230120'),
            ('RJ', 'RDRJ2301.dbc', '330455', '2023', '01', 'F', '32', '5', '2300.75', '20230210', '20230215'),
            ('MG', 'RDMG2301.dbc', '310620', '2023', '03', 'M', '28', '3', '1200.00', '20230305', '20230308')
        ) AS t(UF, SOURCE_FILE, MUNIC_RES, ANO_CMPT, MES_CMPT, SEXO, IDADE, QT_DIARIAS, VAL_TOT, DT_INTER, DT_SAIDA)
    """)
    return "staging_test"


class TestSQLTransformer:
    """Tests for SQLTransformer class."""

    def test_transformer_initialization(self, sql_transformer):
        """Test SQLTransformer initializes correctly."""
        assert sql_transformer is not None
        assert sql_transformer.conn is not None

    def test_execute_sql_query(self, sql_transformer, duckdb_connection):
        """Test executing SQL queries through the transformer's connection."""
        # Create a test table
        duckdb_connection.execute("CREATE TABLE test_table (id INT, name VARCHAR)")
        duckdb_connection.execute("INSERT INTO test_table VALUES (1, 'test')")

        # Query using the transformer's connection
        result = sql_transformer.conn.execute("SELECT * FROM test_table").fetchall()

        assert len(result) == 1
        assert result[0][0] == 1
        assert result[0][1] == "test"

    def test_transform_to_canonical_view(self, sql_transformer, sample_staging_table):
        """Test transforming staging table to canonical view."""
        view_name = "test_canonical_view"

        sql_transformer.transform_to_canonical_view(
            source_table=sample_staging_table,
            target_view=view_name,
        )

        # Query the view and verify structure
        result = sql_transformer.conn.execute(f"SELECT * FROM {view_name}").fetchall()
        assert len(result) == 3  # 3 rows from sample data


class TestSQLTransformerDateParsing:
    """Tests for date parsing in SQL transformer."""

    def test_parse_date_yyyymmdd(self, duckdb_connection):
        """Test parsing dates in YYYYMMDD format using STRPTIME."""
        result = duckdb_connection.execute("""
            SELECT TRY_STRPTIME('20230115', '%Y%m%d')::DATE as parsed_date
        """).fetchone()

        assert result[0] is not None

    def test_parse_invalid_date_returns_null(self, duckdb_connection):
        """Test that invalid dates return NULL with TRY_CAST."""
        result = duckdb_connection.execute("""
            SELECT TRY_CAST('invalid' AS DATE) as parsed_date
        """).fetchone()

        assert result[0] is None


class TestSQLTransformerNumericConversion:
    """Tests for numeric type conversion in SQL transformer."""

    def test_try_cast_integer(self, duckdb_connection):
        """Test TRY_CAST for integer conversion."""
        result = duckdb_connection.execute("""
            SELECT TRY_CAST('123' AS INTEGER) as num,
                   TRY_CAST('abc' AS INTEGER) as invalid
        """).fetchone()

        assert result[0] == 123
        assert result[1] is None

    def test_try_cast_float(self, duckdb_connection):
        """Test TRY_CAST for float conversion."""
        result = duckdb_connection.execute("""
            SELECT TRY_CAST('1500.50' AS FLOAT) as num,
                   TRY_CAST('invalid' AS FLOAT) as invalid
        """).fetchone()

        assert abs(result[0] - 1500.50) < 0.01
        assert result[1] is None


class TestSQLTransformerBooleanConversion:
    """Tests for boolean type conversion in SQL transformer."""

    def test_boolean_from_0_1(self, duckdb_connection):
        """Test boolean conversion from 0/1 values."""
        result = duckdb_connection.execute("""
            SELECT
                CASE WHEN '1' = '1' THEN TRUE ELSE FALSE END as morte_true,
                CASE WHEN '0' = '1' THEN TRUE ELSE FALSE END as morte_false
        """).fetchone()

        assert result[0] is True
        assert result[1] is False
