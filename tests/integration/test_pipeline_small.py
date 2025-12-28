"""Integration tests for PyDataSUS pipeline with small dataset.

These tests use minimal downloads to verify the complete pipeline works
end-to-end. They require network access to the DATASUS FTP server.

Run with: pytest tests/integration/ -v --slow
"""

import pytest
from pathlib import Path

from pydatasus.config import PipelineConfig
from pydatasus.storage.parquet_query_engine import ParquetQueryEngine


# Mark all tests in this module as slow (require network)
pytestmark = pytest.mark.slow


@pytest.fixture
def small_pipeline_config(temp_dir):
    """Create a small pipeline config for testing (1 month, 1 state)."""
    return PipelineConfig.create(
        base_dir=temp_dir,
        subsystem="sihsus",
        start_date="2024-01-01",
        end_date="2024-01-31",
        uf_list=["AC"],  # Acre has smallest files
        compression="snappy",  # Fast compression
        chunk_size=5000,
    )


class TestParquetQueryEngineIntegration:
    """Integration tests for ParquetQueryEngine with real Parquet data."""

    def test_query_engine_with_existing_data(self, temp_dir):
        """Test ParquetQueryEngine with existing Parquet files.

        This test requires existing Parquet data in ./data/datasus/sihsus/parquet/
        Skip if not available.
        """
        parquet_dir = Path("./data/datasus/sihsus/parquet")

        if not parquet_dir.exists():
            pytest.skip("No existing Parquet data found")

        parquet_files = list(parquet_dir.rglob("*.parquet"))
        if not parquet_files:
            pytest.skip("No Parquet files found")

        # Test query engine
        engine = ParquetQueryEngine(parquet_dir, view_name="sihsus")

        # Test basic methods
        tables = engine.tables()
        assert "sihsus" in tables

        # Test count
        count = engine.count()
        assert count > 0

        # Test schema
        schema = engine.schema()
        assert len(schema) > 0

        # Test sample
        sample = engine.sample(5)
        assert len(sample) <= 5

        # Test get_processed_source_files
        processed = engine.get_processed_source_files()
        if processed:
            assert all(f.endswith(".dbc") for f in processed)

        engine.close()

    def test_query_engine_sql_execution(self):
        """Test SQL execution on existing data."""
        parquet_dir = Path("./data/datasus/sihsus/parquet")

        if not parquet_dir.exists():
            pytest.skip("No existing Parquet data found")

        parquet_files = list(parquet_dir.rglob("*.parquet"))
        if not parquet_files:
            pytest.skip("No Parquet files found")

        engine = ParquetQueryEngine(parquet_dir, view_name="sihsus")

        # Test aggregation query
        result = engine.sql("""
            SELECT
                uf,
                COUNT(*) as total
            FROM sihsus
            GROUP BY uf
            ORDER BY total DESC
            LIMIT 5
        """)

        assert result is not None
        assert len(result) > 0
        assert "uf" in result.columns
        assert "total" in result.columns

        engine.close()


class TestPipelineConfigFactory:
    """Test PipelineConfig.create() factory method."""

    def test_create_config_structure(self, temp_dir):
        """Test that factory creates correct directory structure."""
        config = PipelineConfig.create(
            base_dir=temp_dir,
            subsystem="sihsus",
            start_date="2023-01-01",
            end_date="2023-03-31",
            uf_list=["SP", "RJ"],
        )

        # Verify paths
        assert config.subsystem == "sihsus"
        assert "sihsus" in str(config.download.output_dir)
        assert "sihsus" in str(config.conversion.dbc_dir)
        assert "sihsus" in str(config.storage.parquet_dir)

        # Verify structure
        assert config.download.output_dir.name == "dbc"
        assert config.conversion.dbf_dir.name == "dbf"
        assert config.storage.parquet_dir.name == "parquet"

    def test_create_config_parameters(self, temp_dir):
        """Test that factory passes parameters correctly."""
        config = PipelineConfig.create(
            base_dir=temp_dir,
            subsystem="sim",
            start_date="2020-01-01",
            end_date="2020-12-31",
            uf_list=["MG"],
            compression="zstd",
            override=True,
            chunk_size=20000,
        )

        assert config.subsystem == "sim"
        assert config.download.start_date == "2020-01-01"
        assert config.download.end_date == "2020-12-31"
        assert config.download.uf_list == ["MG"]
        assert config.storage.compression == "zstd"
        assert config.download.override is True
        assert config.database.chunk_size == 20000


class TestIncrementalUpdater:
    """Integration tests for IncrementalUpdater."""

    def test_get_processed_files_no_data(self, temp_dir):
        """Test with no existing Parquet data."""
        from pydatasus.storage.incremental_updater import IncrementalUpdater

        config = PipelineConfig.create(
            base_dir=temp_dir,
            subsystem="sihsus",
            start_date="2023-01-01",
        )

        updater = IncrementalUpdater(config)
        processed = updater.get_processed_files()

        # Should return empty set when no data exists
        assert processed == set()

    def test_get_update_summary_no_data(self, temp_dir):
        """Test update summary with no existing data."""
        from pydatasus.storage.incremental_updater import IncrementalUpdater

        config = PipelineConfig.create(
            base_dir=temp_dir,
            subsystem="sihsus",
            start_date="2023-01-01",
            uf_list=["AC"],  # Small state
        )

        updater = IncrementalUpdater(config)
        summary = updater.get_update_summary()

        assert summary["processed_count"] == 0
        # available_count depends on FTP access
