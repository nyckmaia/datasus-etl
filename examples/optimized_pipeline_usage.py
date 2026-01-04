"""Example: Using the optimized SIHSUS pipeline with SQL transformations.

This example demonstrates the new streaming-based pipeline that:
- Streams DBF → DuckDB (no intermediate CSV)
- Applies all transformations in SQL
- Exports directly to Parquet
- Queries results using ParquetQueryEngine
"""

from pathlib import Path
from datasus_etl.config import (
    PipelineConfig,
    DownloadConfig,
    ConversionConfig,
    StorageConfig,
    DatabaseConfig,
)
from datasus_etl.pipeline import SihsusPipeline
from datasus_etl.storage import ParquetQueryEngine


def run_optimized_pipeline():
    """Run the optimized pipeline for SIHSUS data."""

    # Configure pipeline
    base_dir = Path("./data/datasus")

    config = PipelineConfig(
        download=DownloadConfig(
            output_dir=base_dir / "dbc",
            start_date="2020-01-01",
            end_date="2020-12-31",
            uf_list=["SP"],  # Just São Paulo for testing
            override=False,
        ),
        conversion=ConversionConfig(
            dbc_dir=base_dir / "dbc",
            dbf_dir=base_dir / "dbf",
            override=False,
        ),
        storage=StorageConfig(
            parquet_dir=base_dir / "parquet",
            partition_cols=["ANO_INTER", "UF_ZI"],  # Partition by year and state
            compression="zstd",  # Better compression than snappy
            row_group_size=100_000,
        ),
        database=DatabaseConfig(
            db_path=None,  # In-memory for pipeline execution
            chunk_size=10000,  # Rows per chunk for DBF streaming
        ),
    )

    # Run pipeline
    print("Starting optimized SIHSUS pipeline...")
    print(f"Processing: {config.download.start_date} to {config.download.end_date}")
    print(f"States: {config.download.uf_list}")

    pipeline = SihsusPipeline(config, ibge_data_path=None)
    result = pipeline.run()

    print("\nPipeline completed successfully!")
    print(f"Total rows exported: {result.get_metadata('total_rows_exported'):,}")

    return config.storage.parquet_dir


def query_results(parquet_dir: Path):
    """Query the Parquet results using ParquetQueryEngine."""

    print(f"\n{'='*60}")
    print("Querying results with ParquetQueryEngine")
    print(f"{'='*60}\n")

    # Initialize query engine
    engine = ParquetQueryEngine(parquet_dir)

    # Example 1: Basic statistics
    print("Example 1: Basic statistics")
    df = engine.sql("""
        SELECT
            COUNT(*) as total_internacoes,
            COUNT(DISTINCT MUNIC_RES) as total_municipios,
            SUM(QT_DIARIAS_INT) as total_diarias,
            AVG(VAL_TOT_NUM) as valor_medio
        FROM sihsus
    """)
    print(df)

    # Example 2: Top 10 procedures
    print("\nExample 2: Top 10 procedures")
    df = engine.sql("""
        SELECT
            PROC_REA as procedimento,
            COUNT(*) as quantidade
        FROM sihsus
        WHERE PROC_REA IS NOT NULL
        GROUP BY PROC_REA
        ORDER BY quantidade DESC
        LIMIT 10
    """)
    print(df)

    # Example 3: Monthly trends
    print("\nExample 3: Monthly trends")
    df = engine.sql("""
        SELECT
            ANO_INTER as ano,
            MES_INTER as mes,
            COUNT(*) as total_internacoes,
            AVG(DIAS_INTERNACAO) as media_dias_internacao,
            SUM(VAL_TOT_NUM) as valor_total
        FROM sihsus
        GROUP BY ANO_INTER, MES_INTER
        ORDER BY ANO_INTER, MES_INTER
    """)
    print(df)

    # Example 4: Demographics
    print("\nExample 4: Demographics (age and gender)")
    df = engine.sql("""
        SELECT
            SEXO_DESCR as sexo,
            CASE
                WHEN IDADE_INT < 18 THEN '0-17'
                WHEN IDADE_INT BETWEEN 18 AND 59 THEN '18-59'
                WHEN IDADE_INT >= 60 THEN '60+'
                ELSE 'Desconhecido'
            END as faixa_etaria,
            COUNT(*) as total,
            AVG(DIAS_INTERNACAO) as media_dias
        FROM sihsus
        WHERE IDADE_INT IS NOT NULL
        GROUP BY sexo, faixa_etaria
        ORDER BY sexo, faixa_etaria
    """)
    print(df)

    # Example 5: Partition pruning (query only 2020 data)
    print("\nExample 5: Partition pruning example")
    df = engine.sql("""
        SELECT
            ANO_INTER,
            UF_ZI,
            COUNT(*) as total
        FROM sihsus
        WHERE ANO_INTER = 2020  -- This will only read 2020 partition!
        GROUP BY ANO_INTER, UF_ZI
    """)
    print(df)
    print("\n(Notice: DuckDB only reads 2020 partition, very fast!)")

    # Show schema
    print("\nTable schema:")
    print(engine.schema())

    # Cleanup
    engine.close()


if __name__ == "__main__":
    # Run the optimized pipeline
    parquet_dir = run_optimized_pipeline()

    # Query the results
    query_results(parquet_dir)

    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)
