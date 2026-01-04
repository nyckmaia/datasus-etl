"""CSV export example: Export DataSUS data to CSV format.

This example shows how to export data to CSV instead of Parquet.
CSV is useful for:
- Compatibility with spreadsheet software (Excel, Google Sheets)
- Integration with legacy systems
- Quick data inspection
"""

from pathlib import Path
from datasus_etl.config import PipelineConfig
from datasus_etl.pipeline import SihsusPipeline


def main():
    """Run pipeline with CSV export."""
    # Create config with CSV output format
    config = PipelineConfig.create(
        base_dir=Path("./data/datasus"),
        subsystem="sihsus",
        start_date="2023-01-01",
        end_date="2023-01-31",  # Just 1 month for this example
        uf_list=["SP"],
        compression="zstd",  # Ignored for CSV
        chunk_size=10000,
        output_format="csv",  # Export as CSV instead of Parquet
        csv_delimiter=";",  # Brazilian standard delimiter
    )

    print("=" * 60)
    print("DataSUS-ETL - CSV Export Example")
    print("=" * 60)
    print(f"Output format: {config.output_format.upper()}")
    print(f"CSV delimiter: {repr(config.csv_delimiter)}")
    print(f"Output directory: {config.storage.parquet_dir}")
    print("=" * 60)

    # Run pipeline
    pipeline = SihsusPipeline(config)
    result = pipeline.run()

    # Show results
    print("\n" + "=" * 60)
    print("CSV Export completed!")
    print("=" * 60)

    total_rows = result.get_metadata("total_rows_exported", 0)
    print(f"Total rows exported: {total_rows:,}")
    print(f"Output directory: {config.storage.parquet_dir}")
    print("=" * 60)

    # Show output file structure
    print("\nOutput structure:")
    print("-" * 40)
    print(f"{config.storage.parquet_dir}/")
    print("└── uf=SP/")
    print("    ├── RDSP2301.csv")
    print("    ├── RDSP2302.csv")
    print("    └── ...")
    print()
    print("Note: Each CSV file corresponds to one DBC source file.")
    print("CSV files have Hive-style partition directories (uf=XX).")


if __name__ == "__main__":
    main()
