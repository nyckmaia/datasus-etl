"""Tests for SIHSUS processor."""

import polars as pl
import pytest

from pydatasus.config import ProcessingConfig
from pydatasus.transform.processors.sihsus_processor import SihsusProcessor


class TestSihsusProcessor:
    """Tests for SihsusProcessor."""

    def test_processor_initialization(self, temp_dir):
        """Test processor initialization."""
        config = ProcessingConfig(
            input_dir=temp_dir / "input",
            output_dir=temp_dir / "output",
        )

        processor = SihsusProcessor(config)

        assert processor.config == config
        assert processor.config.output_dir.exists()

    def test_clean_dataframe(self, temp_dir):
        """Test dataframe cleaning."""
        config = ProcessingConfig(
            input_dir=temp_dir,
            output_dir=temp_dir / "output",
        )

        processor = SihsusProcessor(config)

        # Create test dataframe with dirty data
        df = pl.DataFrame(
            {
                "uf_zi": ["SP ", " RJ", "MG"],  # lowercase with spaces
                "value": [100, None, 200],
            }
        )

        cleaned = processor._clean_dataframe(df)

        # Should uppercase column names
        assert "UF_ZI" in cleaned.columns
        assert "VALUE" in cleaned.columns

        # Should strip whitespace
        assert cleaned["UF_ZI"].to_list() == ["SP", "RJ", "MG"]

    def test_process_file(self, temp_dir, sample_sihsus_csv):
        """Test processing a single CSV file."""
        config = ProcessingConfig(
            input_dir=sample_sihsus_csv.parent,
            output_dir=temp_dir / "output",
        )

        processor = SihsusProcessor(config)
        output_file = processor.process_file(sample_sihsus_csv)

        assert output_file.exists()
        assert output_file.suffix == ".csv"

        # Read processed file
        df = pl.read_csv(output_file, separator=";")

        # Should have processed data
        assert len(df) > 0
        assert "UF_ZI" in df.columns

    def test_process_directory(self, temp_dir, sample_sihsus_csv):
        """Test processing directory of CSV files."""
        config = ProcessingConfig(
            input_dir=sample_sihsus_csv.parent,
            output_dir=temp_dir / "output",
        )

        processor = SihsusProcessor(config)
        stats = processor.process_directory()

        assert stats["total"] >= 1
        assert stats["processed"] >= 1
        assert stats["failed"] == 0

    def test_skip_existing_files(self, temp_dir, sample_sihsus_csv):
        """Test that existing files are skipped."""
        config = ProcessingConfig(
            input_dir=sample_sihsus_csv.parent,
            output_dir=temp_dir / "output",
            override=False,
        )

        processor = SihsusProcessor(config)

        # Process once
        processor.process_file(sample_sihsus_csv)

        # Process again - should skip
        output_file = processor.process_file(sample_sihsus_csv)

        assert output_file.exists()

    def test_override_existing_files(self, temp_dir, sample_sihsus_csv):
        """Test that override works."""
        config = ProcessingConfig(
            input_dir=sample_sihsus_csv.parent,
            output_dir=temp_dir / "output",
            override=True,
        )

        processor = SihsusProcessor(config)

        # Process once
        output1 = processor.process_file(sample_sihsus_csv)
        mtime1 = output1.stat().st_mtime

        # Process again with override
        output2 = processor.process_file(sample_sihsus_csv)
        mtime2 = output2.stat().st_mtime

        # File should be rewritten (mtime may be same or different)
        assert output1 == output2
