"""Basic usage example: Process 1 month of SIHSUS data from 1 state.

This is the simplest way to use DataSUS-ETL optimized pipeline.
Perfect for testing and small-scale data processing.
"""

from pathlib import Path
from datasus_etl.config import PipelineConfig
from datasus_etl.pipeline import SihsusPipeline


def main():
    """Run basic pipeline example."""
    # Use the factory method to create config with automatic path configuration
    # This creates the directory structure: ./data/datasus/sihsus/dbc|dbf|parquet/
    config = PipelineConfig.create(
        base_dir=Path("./data/datasus"),
        subsystem="sihsus",  # DataSUS subsystem (sihsus or sim)
        start_date="2023-01-01",
        end_date="2023-03-31",
        uf_list=["SP"],
        compression="zstd",  # Better compression
        chunk_size=10000,  # Rows per chunk (adjust based on available RAM)
    )

    # Create and run pipeline
    print("=" * 60)
    print("DataSUS-ETL - Basic Usage Example")
    print("=" * 60)
    print(f"Subsystem: {config.subsystem.upper()}")
    print(f"Processing: {config.download.start_date} to {config.download.end_date}")
    print(f"States: {config.download.uf_list}")
    print(f"Output: {config.storage.parquet_dir}")
    print("=" * 60)

    pipeline = SihsusPipeline(config)
    result = pipeline.run()

    # Show results
    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)

    total_rows = result.get_metadata("total_rows_exported", 0)
    exported_files = result.get("exported_parquet_files", [])

    print(f"Total rows exported: {total_rows:,}")
    print(f"Parquet files generated: {len(exported_files)}")
    print(f"Output directory: {config.storage.parquet_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
