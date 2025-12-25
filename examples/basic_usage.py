"""Basic usage example: Process 1 month of SIHSUS data from 1 state.

This is the simplest way to use PyDataSUS optimized pipeline.
Perfect for testing and small-scale data processing.
"""

from pathlib import Path
from pydatasus.config import (
    PipelineConfig,
    DownloadConfig,
    ConversionConfig,
    ProcessingConfig,
    StorageConfig,
    DatabaseConfig,
)
from pydatasus.pipeline import SihsusPipeline


def main():
    """Run basic pipeline example."""
    # Define base directory for data
    base_dir = Path("./data/datasus")

    # Configure pipeline with minimal settings
    config = PipelineConfig(
        download=DownloadConfig(
            output_dir=base_dir / "dbc",
            start_date="2023-01-01",  # Process only January 2023
            end_date="2023-01-31",
            uf_list=["SP"],  # Only São Paulo state
            override=False,
        ),
        conversion=ConversionConfig(
            dbc_dir=base_dir / "dbc",
            dbf_dir=base_dir / "dbf",
            csv_dir=base_dir / "csv",  # Not used but required by config
            tabwin_dir=Path("C:/Program Files/TAB415"),  # Adjust path if needed
            override=False,
        ),
        processing=ProcessingConfig(
            input_dir=base_dir / "csv",  # Not used but required
            output_dir=base_dir / "processed",  # Not used but required
            override=False,
        ),
        storage=StorageConfig(
            parquet_dir=base_dir / "parquet",
            compression="zstd",  # Better compression
            partition_cols=["ANO_INTER", "UF_ZI"],
        ),
        database=DatabaseConfig(
            chunk_size=10000  # Rows per chunk (adjust based on available RAM)
        ),
    )

    # Create and run pipeline
    print("="*60)
    print("PyDataSUS - Basic Usage Example")
    print("="*60)
    print(f"Processing: {config.download.start_date} to {config.download.end_date}")
    print(f"States: {config.download.uf_list}")
    print(f"Output: {config.storage.parquet_dir}")
    print("="*60)

    pipeline = SihsusPipeline(config)
    result = pipeline.run()

    # Show results
    print("\n" + "="*60)
    print("Pipeline completed successfully!")
    print("="*60)
    print(f"Total rows exported: {result.get_metadata('total_rows_exported'):,}")
    print(f"Parquet directory: {config.storage.parquet_dir}")
    print("="*60)


if __name__ == "__main__":
    main()
