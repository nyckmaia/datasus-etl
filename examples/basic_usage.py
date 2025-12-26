"""Basic usage example: Process 1 month of SIHSUS data from 1 state.

This is the simplest way to use PyDataSUS optimized pipeline.
Perfect for testing and small-scale data processing.
"""

from pathlib import Path
from pydatasus.config import (
    PipelineConfig,
    DownloadConfig,
    ConversionConfig,
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
            start_date="2023-01-01",  # Process 2 months of data
            end_date="2023-02-28",
            uf_list=["SP", "RJ"],  # Only São Paulo and Rio de Janeiro states
            override=False,
        ),
        conversion=ConversionConfig(
            dbc_dir=base_dir / "dbc",
            dbf_dir=base_dir / "dbf",
            override=False,
        ),
        storage=StorageConfig(
            parquet_dir=base_dir / "parquet",
            compression="zstd",  # Better compression
            # No partitioning - each DBC generates one Parquet file
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

    total_rows = result.get_metadata('total_rows_exported', 0)
    exported_files = result.get('exported_parquet_files', [])

    print(f"Total rows exported: {total_rows:,}")
    print(f"Parquet files generated: {len(exported_files)}")
    print(f"Output directory: {config.storage.parquet_dir}")

    if exported_files:
        print("\nGenerated files:")
        for file_path in exported_files:
            file_name = Path(file_path).name
            print(f"  - {file_name}")

    print("="*60)


if __name__ == "__main__":
    main()
