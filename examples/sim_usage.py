"""SIM (Sistema de Informações sobre Mortalidade) usage example.

This example shows how to process mortality data from DataSUS.
SIM data includes death certificates with ICD-10 cause of death codes.
"""

from pathlib import Path
from pydatasus.config import PipelineConfig
from pydatasus.pipeline import SIMPipeline


def main():
    """Run SIM pipeline example."""
    # Create config for SIM subsystem
    # SIM files are organized by year (not monthly like SIHSUS)
    config = PipelineConfig.create(
        base_dir=Path("./data/datasus"),
        subsystem="sim",  # Mortality Information System
        start_date="2022-01-01",
        end_date="2022-12-31",
        uf_list=["SP"],  # São Paulo only
        compression="zstd",
        chunk_size=10000,
    )

    print("=" * 60)
    print("PyDataSUS - SIM Pipeline Example")
    print("=" * 60)
    print(f"Subsystem: {config.subsystem.upper()}")
    print(f"Processing: {config.download.start_date} to {config.download.end_date}")
    print(f"States: {config.download.uf_list}")
    print(f"Output: {config.storage.parquet_dir}")
    print("=" * 60)

    # Create and run SIM pipeline
    pipeline = SIMPipeline(config)
    result = pipeline.run()

    # Show results
    print("\n" + "=" * 60)
    print("SIM Pipeline completed successfully!")
    print("=" * 60)

    total_rows = result.get_metadata("total_rows_exported", 0)
    exported_files = result.get("exported_parquet_files", [])

    print(f"Total death records exported: {total_rows:,}")
    print(f"Parquet files generated: {len(exported_files)}")
    print(f"Output directory: {config.storage.parquet_dir}")
    print("=" * 60)

    # Example query on the exported data
    print("\nExample: Query the exported data with DuckDB")
    print("-" * 60)
    print("""
import duckdb

# Read all parquet files with Hive partitioning
conn = duckdb.connect()
conn.execute(f\"\"\"
    CREATE VIEW sim AS
    SELECT * FROM read_parquet('{config.storage.parquet_dir}/**/*.parquet', hive_partitioning=true)
\"\"\")

# Count deaths by cause category (ICD-10 chapter)
conn.execute(\"\"\"
    SELECT
        LEFT(causabas, 1) as icd_chapter,
        COUNT(*) as deaths
    FROM sim
    WHERE causabas IS NOT NULL
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
\"\"\").show()
""")


if __name__ == "__main__":
    main()
