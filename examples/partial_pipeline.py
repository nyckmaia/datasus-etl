"""Execute specific pipeline stages independently.

This example shows how to run individual stages of the pipeline,
useful for debugging, testing, or resuming interrupted pipelines.
"""

from pathlib import Path
from datasus_etl.config import (
    PipelineConfig,
    DownloadConfig,
    ConversionConfig,
    StorageConfig,
    DatabaseConfig,
)
from datasus_etl.pipeline.sihsus_pipeline import (
    DownloadStage,
    DbcToDbfStage,
    DbfToDbStage,
    SqlTransformStage,
)
from datasus_etl.core.context import PipelineContext


def main():
    """Run partial pipeline example."""
    base_dir = Path("./data/datasus")

    # Configure pipeline
    config = PipelineConfig(
        download=DownloadConfig(
            output_dir=base_dir / "dbc",
            start_date="2023-01-01",
            end_date="2023-01-31",
            uf_list=["RJ"],  # Rio de Janeiro
            override=False,
        ),
        conversion=ConversionConfig(
            dbc_dir=base_dir / "dbc",
            dbf_dir=base_dir / "dbf",
            override=False,
        ),
        storage=StorageConfig(
            parquet_dir=base_dir / "parquet",
            compression="zstd",
            partition_cols=["ANO_INTER", "UF_ZI"],
        ),
        database=DatabaseConfig(chunk_size=10000),
    )

    print("="*60)
    print("DataSUS-ETL - Partial Pipeline Example")
    print("="*60)

    # Initialize context
    context = PipelineContext()

    # Example 1: Run only download stage
    print("\n" + "="*60)
    print("Stage 1: Download DBC Files")
    print("="*60)

    stage = DownloadStage(config)
    context = stage.execute(context)

    downloaded = context.get_metadata("download_count")
    print(f"✓ Downloaded {downloaded} DBC files")

    # Example 2: Run only DBC→DBF conversion
    print("\n" + "="*60)
    print("Stage 2: Convert DBC to DBF")
    print("="*60)

    stage = DbcToDbfStage(config)
    context = stage.execute(context)

    converted = context.get_metadata("dbc_converted_count")
    print(f"✓ Converted {converted} DBF files")

    # Example 3: Run only DBF→DuckDB streaming
    print("\n" + "="*60)
    print("Stage 3: Stream DBF to DuckDB")
    print("="*60)

    stage = DbfToDbStage(config)
    context = stage.execute(context)

    rows_loaded = context.get_metadata("total_rows_loaded")
    print(f"✓ Loaded {rows_loaded:,} rows into DuckDB")

    # Example 4: Run only SQL transformation and Parquet export
    print("\n" + "="*60)
    print("Stage 4: Transform and Export to Parquet")
    print("="*60)

    stage = SqlTransformStage(config, ibge_data_path=None)
    context = stage.execute(context)

    rows_exported = context.get_metadata("total_rows_exported")
    print(f"✓ Exported {rows_exported:,} rows to Parquet")

    # Summary
    print("\n" + "="*60)
    print("All stages completed successfully!")
    print("="*60)
    print(f"Downloaded: {downloaded} files")
    print(f"Converted: {converted} files")
    print(f"Loaded: {rows_loaded:,} rows")
    print(f"Exported: {rows_exported:,} rows")
    print(f"Output: {config.storage.parquet_dir}")
    print("="*60)


def resume_from_stage_3():
    """Example: Resume pipeline from stage 3 (assuming 1 and 2 already completed)."""
    print("\n" + "="*60)
    print("Resume Example: Starting from Stage 3")
    print("="*60)

    base_dir = Path("./data/datasus")

    config = PipelineConfig(
        download=DownloadConfig(
            output_dir=base_dir / "dbc",
            start_date="2023-01-01",
            end_date="2023-01-31",
            uf_list=["RJ"],
        ),
        conversion=ConversionConfig(
            dbc_dir=base_dir / "dbc",
            dbf_dir=base_dir / "dbf",
        ),
        storage=StorageConfig(
            parquet_dir=base_dir / "parquet",
            partition_cols=["ANO_INTER", "UF_ZI"],
        ),
        database=DatabaseConfig(chunk_size=10000),
    )

    context = PipelineContext()

    # Resume from stage 3 (DBF files already exist)
    print("Running Stage 3: DBF → DuckDB...")
    stage3 = DbfToDbStage(config)
    context = stage3.execute(context)

    print("Running Stage 4: Transform & Export...")
    stage4 = SqlTransformStage(config)
    context = stage4.execute(context)

    print(f"\n✓ Resumed and completed successfully!")
    print(f"  Exported {context.get_metadata('total_rows_exported'):,} rows")


def run_only_transformation():
    """Example: Run only transformation (assuming DBF→DuckDB already done)."""
    print("\n" + "="*60)
    print("Transform-Only Example")
    print("="*60)

    base_dir = Path("./data/datasus")

    config = PipelineConfig(
        download=DownloadConfig(output_dir=base_dir / "dbc"),
        conversion=ConversionConfig(
            dbc_dir=base_dir / "dbc",
            dbf_dir=base_dir / "dbf",
        ),
        storage=StorageConfig(
            parquet_dir=base_dir / "parquet_v2",  # Different output
            compression="zstd",
        ),
        database=DatabaseConfig(chunk_size=10000),
    )

    # Manually create context with staging tables
    # (In real scenario, you'd know the table names from previous run)
    context = PipelineContext()
    # Simulate that DBF→DuckDB was already done
    # context.set("staging_tables", ["staging_RDSP2301", ...])

    stage = SqlTransformStage(config)
    # Note: This would fail if staging tables don't exist in DuckDB
    # Only works if you have a persistent DuckDB file or tables still in memory

    print("This example requires persistent DuckDB or existing staging tables.")
    print("For demonstration purposes only.")


if __name__ == "__main__":
    # Run the main example
    main()

    # Optionally run other examples:
    # resume_from_stage_3()
    # run_only_transformation()
