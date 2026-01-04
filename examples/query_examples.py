"""Advanced query examples using ParquetQueryEngine.

Demonstrates various SQL query patterns for analyzing SIHSUS data:
- Aggregations and GROUP BY
- Filtering with partition pruning
- Window functions
- Exporting results
"""

from pathlib import Path
from datasus_etl.storage import ParquetQueryEngine


def main():
    """Run query examples."""
    # Initialize query engine (assumes data already processed)
    parquet_dir = Path("./data/datasus/parquet")

    if not parquet_dir.exists():
        print(f"Error: Parquet directory not found: {parquet_dir}")
        print("Run basic_usage.py first to generate data.")
        return

    print("="*60)
    print("DataSUS-ETL - Query Examples")
    print("="*60)

    engine = ParquetQueryEngine(parquet_dir)

    # Example 1: Basic statistics
    print("\n" + "="*60)
    print("Example 1: Basic Statistics")
    print("="*60)

    df = engine.sql("""
        SELECT
            COUNT(*) as total_internacoes,
            COUNT(DISTINCT MUNIC_RES) as total_municipios,
            SUM(QT_DIARIAS) as total_diarias,
            AVG(VAL_TOT_NUM) as valor_medio,
            MAX(VAL_TOT_NUM) as valor_maximo
        FROM sihsus
    """)
    print(df)

    # Example 2: Top 10 municipalities by hospitalizations
    print("\n" + "="*60)
    print("Example 2: Top 10 Municipalities by Hospitalizations")
    print("="*60)

    df = engine.sql("""
        SELECT
            MUNIC_RES as codigo_municipio,
            COUNT(*) as total_internacoes,
            AVG(DIAS_INTERNACAO) as media_dias,
            SUM(VAL_TOT_NUM) as valor_total
        FROM sihsus
        WHERE MUNIC_RES IS NOT NULL
        GROUP BY MUNIC_RES
        ORDER BY total_internacoes DESC
        LIMIT 10
    """)
    print(df)

    # Example 3: Monthly trends
    print("\n" + "="*60)
    print("Example 3: Monthly Trends")
    print("="*60)

    df = engine.sql("""
        SELECT
            ANO_INTER as ano,
            MES_INTER as mes,
            COUNT(*) as total_internacoes,
            AVG(DIAS_INTERNACAO) as media_dias_internacao,
            SUM(VAL_TOT_NUM) as valor_total,
            AVG(VAL_TOT_NUM) as valor_medio
        FROM sihsus
        WHERE ANO_INTER IS NOT NULL AND MES_INTER IS NOT NULL
        GROUP BY ANO_INTER, MES_INTER
        ORDER BY ANO_INTER, MES_INTER
    """)
    print(df)

    # Example 4: Demographics (age and gender)
    print("\n" + "="*60)
    print("Example 4: Demographics Analysis")
    print("="*60)

    df = engine.sql("""
        SELECT
            SEXO_DESCR as sexo,
            CASE
                WHEN IDADE < 18 THEN '0-17'
                WHEN IDADE BETWEEN 18 AND 59 THEN '18-59'
                WHEN IDADE >= 60 THEN '60+'
                ELSE 'Unknown'
            END as faixa_etaria,
            COUNT(*) as total,
            AVG(DIAS_INTERNACAO) as media_dias,
            AVG(VAL_TOT_NUM) as valor_medio
        FROM sihsus
        WHERE IDADE IS NOT NULL
        GROUP BY sexo, faixa_etaria
        ORDER BY sexo, faixa_etaria
    """)
    print(df)

    # Example 5: Top procedures
    print("\n" + "="*60)
    print("Example 5: Top 10 Medical Procedures")
    print("="*60)

    df = engine.sql("""
        SELECT
            PROC_REA as procedimento,
            COUNT(*) as quantidade,
            AVG(VAL_TOT_NUM) as valor_medio,
            SUM(VAL_TOT_NUM) as valor_total
        FROM sihsus
        WHERE PROC_REA IS NOT NULL
        GROUP BY PROC_REA
        ORDER BY quantidade DESC
        LIMIT 10
    """)
    print(df)

    # Example 6: Partition pruning (only reads specific partitions)
    print("\n" + "="*60)
    print("Example 6: Partition Pruning Example")
    print("="*60)

    df = engine.sql("""
        SELECT
            ANO_INTER,
            UF_ZI,
            COUNT(*) as total
        FROM sihsus
        WHERE ANO_INTER = 2023 AND UF_ZI = 'SP'  -- Only reads 2023/SP partition!
        GROUP BY ANO_INTER, UF_ZI
    """)
    print(df)
    print("\nNote: DuckDB only scans the 2023/SP partition - very fast!")

    # Example 7: Window functions - ranking procedures by month
    print("\n" + "="*60)
    print("Example 7: Window Functions - Top 3 Procedures per Month")
    print("="*60)

    df = engine.sql("""
        SELECT
            MES_INTER,
            PROC_REA,
            quantidade,
            rank
        FROM (
            SELECT
                MES_INTER,
                PROC_REA,
                COUNT(*) as quantidade,
                ROW_NUMBER() OVER (
                    PARTITION BY MES_INTER
                    ORDER BY COUNT(*) DESC
                ) as rank
            FROM sihsus
            WHERE ANO_INTER = 2023 AND PROC_REA IS NOT NULL
            GROUP BY MES_INTER, PROC_REA
        ) ranked
        WHERE rank <= 3
        ORDER BY MES_INTER, rank
    """)
    print(df)

    # Example 8: Export results to CSV
    print("\n" + "="*60)
    print("Example 8: Export Results to CSV")
    print("="*60)

    output_dir = Path("./output")
    output_dir.mkdir(exist_ok=True)

    df_export = engine.sql("""
        SELECT
            ANO_INTER,
            MES_INTER,
            UF_ZI,
            COUNT(*) as total_internacoes,
            SUM(VAL_TOT_NUM) as valor_total
        FROM sihsus
        WHERE ANO_INTER = 2023
        GROUP BY ANO_INTER, MES_INTER, UF_ZI
        ORDER BY ANO_INTER, MES_INTER, UF_ZI
    """)

    csv_file = output_dir / "monthly_summary_2023.csv"
    df_export.write_csv(csv_file)
    print(f"✓ Exported to {csv_file}")
    print(f"  Rows: {len(df_export):,}")

    # Example 9: Complex aggregation with multiple conditions
    print("\n" + "="*60)
    print("Example 9: Complex Aggregation - ICU Statistics")
    print("="*60)

    df = engine.sql("""
        SELECT
            SEXO_DESCR,
            CASE
                WHEN MARCA_UTI = 1 THEN 'Com UTI'
                ELSE 'Sem UTI'
            END as utilizou_uti,
            COUNT(*) as total_casos,
            AVG(DIAS_INTERNACAO) as media_dias,
            AVG(VAL_UTI) as valor_medio_uti,
            AVG(VAL_TOT_NUM) as valor_medio_total
        FROM sihsus
        WHERE SEXO_DESCR IS NOT NULL
        GROUP BY SEXO_DESCR, utilizou_uti
        ORDER BY SEXO_DESCR, utilizou_uti
    """)
    print(df)

    # Show table schema
    print("\n" + "="*60)
    print("Table Schema")
    print("="*60)
    schema = engine.schema()
    print(f"Total columns: {len(schema)}")
    print("\nFirst 10 columns:")
    print(schema.head(10))

    # Cleanup
    engine.close()

    print("\n" + "="*60)
    print("All query examples completed successfully!")
    print("="*60)


if __name__ == "__main__":
    main()
