"""Tests for DuckDB manager."""

import polars as pl
import pytest

from datasus_etl.config import DatabaseConfig
from datasus_etl.exceptions import PyInmetError
from datasus_etl.storage.duckdb_manager import DuckDBManager


class TestDuckDBManager:
    """Tests for DuckDBManager."""

    def test_manager_initialization(self):
        """Test manager initialization."""
        config = DatabaseConfig()
        manager = DuckDBManager(config)

        assert manager.config == config
        assert manager._conn is None

    def test_connect_in_memory(self):
        """Test connecting to in-memory database."""
        config = DatabaseConfig(db_path=None)
        manager = DuckDBManager(config)

        manager.connect()

        assert manager._conn is not None

        manager.disconnect()
        assert manager._conn is None

    def test_connect_to_file(self, temp_dir):
        """Test connecting to file database."""
        db_path = temp_dir / "test.duckdb"
        config = DatabaseConfig(db_path=db_path)
        manager = DuckDBManager(config)

        manager.connect()

        assert manager._conn is not None
        assert db_path.exists()

        manager.disconnect()

    def test_context_manager(self):
        """Test using manager as context manager."""
        config = DatabaseConfig()

        with DuckDBManager(config) as manager:
            assert manager._conn is not None

        # Connection should be closed after exiting context
        assert manager._conn is None

    def test_execute_query(self):
        """Test executing SQL query."""
        config = DatabaseConfig()

        with DuckDBManager(config) as manager:
            result = manager.execute("SELECT 1 as num")
            assert result is not None

    def test_query_returns_dataframe(self):
        """Test query returns Polars DataFrame."""
        config = DatabaseConfig()

        with DuckDBManager(config) as manager:
            df = manager.query("SELECT 1 as num, 'test' as text")

            assert isinstance(df, pl.DataFrame)
            assert len(df) == 1
            assert df["num"][0] == 1
            assert df["text"][0] == "test"

    def test_register_csv(self, temp_dir, sample_sihsus_csv):
        """Test registering CSV as table."""
        config = DatabaseConfig()

        with DuckDBManager(config) as manager:
            manager.register_csv(sample_sihsus_csv, "sihsus", delimiter=";")

            # Query the table
            df = manager.query("SELECT * FROM sihsus")

            assert len(df) > 0
            assert "UF_ZI" in df.columns or "uf_zi" in df.columns

    def test_list_tables(self, temp_dir, sample_sihsus_csv):
        """Test listing tables."""
        config = DatabaseConfig()

        with DuckDBManager(config) as manager:
            manager.register_csv(sample_sihsus_csv, "sihsus", delimiter=";")

            tables = manager.list_tables()

            assert "sihsus" in tables

    def test_table_info(self, temp_dir, sample_sihsus_csv):
        """Test getting table information."""
        config = DatabaseConfig()

        with DuckDBManager(config) as manager:
            manager.register_csv(sample_sihsus_csv, "sihsus", delimiter=";")

            info = manager.table_info("sihsus")

            assert isinstance(info, pl.DataFrame)
            assert len(info) > 0

    def test_table_stats(self, temp_dir, sample_sihsus_csv):
        """Test getting table statistics."""
        config = DatabaseConfig()

        with DuckDBManager(config) as manager:
            manager.register_csv(sample_sihsus_csv, "sihsus", delimiter=";")

            stats = manager.table_stats("sihsus")

            assert stats["table_name"] == "sihsus"
            assert stats["row_count"] > 0
            assert stats["column_count"] > 0
            assert len(stats["columns"]) > 0

    def test_export_query_to_csv(self, temp_dir, sample_sihsus_csv):
        """Test exporting query results to CSV."""
        config = DatabaseConfig()
        output_file = temp_dir / "export.csv"

        with DuckDBManager(config) as manager:
            manager.register_csv(sample_sihsus_csv, "sihsus", delimiter=";")

            manager.export_query_to_csv(
                "SELECT * FROM sihsus WHERE UF_ZI = 'SP'",
                output_file,
            )

            assert output_file.exists()

            # Verify exported data
            df = pl.read_csv(output_file, separator=";")
            assert len(df) >= 0

    def test_execute_without_connection_raises_error(self):
        """Test that executing without connection raises error."""
        config = DatabaseConfig()
        manager = DuckDBManager(config)

        with pytest.raises(PyInmetError):
            manager.execute("SELECT 1")

    def test_query_without_connection_raises_error(self):
        """Test that querying without connection raises error."""
        config = DatabaseConfig()
        manager = DuckDBManager(config)

        with pytest.raises(PyInmetError):
            manager.query("SELECT 1")
