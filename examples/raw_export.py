"""Raw export example: Export data without type conversions.

This example shows how to export raw data without transformations.
Raw mode is useful for:
- Debugging data issues
- Custom data processing workflows
- Preserving original data values
- Investigating data quality issues
"""

from pathlib import Path
from pydatasus.config import PipelineConfig
from pydatasus.pipeline import SihsusPipeline


def main():
    """Run pipeline with raw export (no type conversions)."""
    # Create config with raw mode enabled
    config = PipelineConfig.create(
        base_dir=Path("./data/datasus"),
        subsystem="sihsus",
        start_date="2023-01-01",
        end_date="2023-01-31",  # Just 1 month
        uf_list=["SP"],
        compression="zstd",
        chunk_size=10000,
        raw_mode=True,  # Export without type conversions
    )

    print("=" * 60)
    print("PyDataSUS - Raw Export Example")
    print("=" * 60)
    print(f"Raw mode: ENABLED")
    print(f"Output: {config.storage.parquet_dir}")
    print("=" * 60)
    print()
    print("In raw mode:")
    print("  - Only basic cleaning is applied (remove invisible chars)")
    print("  - All columns are exported as VARCHAR")
    print("  - No date parsing or type conversions")
    print("  - No categorical mappings (SEXO, RACA_COR)")
    print("  - No IBGE enrichment")
    print()
    print("=" * 60)

    # Run pipeline
    pipeline = SihsusPipeline(config)
    result = pipeline.run()

    # Show results
    print("\n" + "=" * 60)
    print("Raw Export completed!")
    print("=" * 60)

    total_rows = result.get_metadata("total_rows_exported", 0)
    print(f"Total rows exported: {total_rows:,}")
    print(f"Output directory: {config.storage.parquet_dir}")
    print("=" * 60)

    # Show difference between raw and normal mode
    print("\nComparison: Raw vs Normal mode")
    print("-" * 50)
    print()
    print("Column     | Raw Mode       | Normal Mode")
    print("-" * 50)
    print("dt_inter   | '20230115'     | 2023-01-15 (DATE)")
    print("idade      | '045'          | 45 (INTEGER)")
    print("sexo       | '1'            | 'M' (mapped)")
    print("raca_cor   | '02'           | 'Preta' (mapped)")
    print("val_tot    | '1234.56'      | 1234.56 (DOUBLE)")
    print()
    print("Use raw mode when you need the original string values.")


if __name__ == "__main__":
    main()
