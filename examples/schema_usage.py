"""Example: Using SIHSUS schema for type validation and SQL generation.

This example demonstrates how to use the SIHSUS_PARQUET_SCHEMA constant
to generate SQL transformations and validate data types.
"""

from datasus_etl.constants.sihsus_schema import (
    SIHSUS_PARQUET_SCHEMA,
    generate_column_cleaning_sql,
    generate_type_validation_sql,
    get_columns_by_type,
    get_numeric_columns,
    get_polars_schema,
    get_sql_cast_expression,
)


def example_1_inspect_schema():
    """Example 1: Inspect the schema definition."""
    print("=" * 60)
    print("Example 1: Inspect SIHSUS Schema")
    print("=" * 60)

    # Total columns
    print(f"\nTotal columns defined: {len(SIHSUS_PARQUET_SCHEMA)}")

    # Show some column types
    print("\nSample column types:")
    sample_cols = ["uf_zi", "nasc", "sexo", "idade", "val_tot", "morte", "dt_inter"]
    for col in sample_cols:
        dtype = SIHSUS_PARQUET_SCHEMA.get(col, "NOT FOUND")
        print(f"  {col:20} -> {dtype}")

    # Count types
    type_counts = {}
    for dtype in SIHSUS_PARQUET_SCHEMA.values():
        type_counts[dtype] = type_counts.get(dtype, 0) + 1

    print("\nType distribution:")
    for dtype, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {dtype:12} : {count:3} columns")


def example_2_generate_sql_casts():
    """Example 2: Generate SQL CAST expressions for data validation."""
    print("\n" + "=" * 60)
    print("Example 2: Generate SQL CAST Expressions")
    print("=" * 60)

    # Generate CAST expressions for numeric columns
    numeric_cols = ["idade", "dias_perm", "val_tot", "uf_zi"]

    print("\nSQL CAST expressions for validation:")
    for col in numeric_cols:
        cast_expr = get_sql_cast_expression(col)
        print(f"  {cast_expr}")

    # Generate CAST for date columns
    date_cols = ["nasc", "dt_inter", "dt_saida"]
    print("\nDate validation (uses TRY_CAST for safety):")
    for col in date_cols:
        cast_expr = get_sql_cast_expression(col)
        print(f"  {cast_expr}")

    # Boolean columns
    bool_cols = ["morte", "gestrisco"]
    print("\nBoolean columns:")
    for col in bool_cols:
        cast_expr = get_sql_cast_expression(col)
        print(f"  {cast_expr}")


def example_3_generate_full_sql_query():
    """Example 3: Generate complete SQL transformation query."""
    print("\n" + "=" * 60)
    print("Example 3: Generate SQL Transformation Query")
    print("=" * 60)

    # Build SELECT clause with all columns cast to proper types
    select_clauses = []
    for col in list(SIHSUS_PARQUET_SCHEMA.keys())[:10]:  # First 10 columns only
        cast_expr = get_sql_cast_expression(col)
        select_clauses.append(f"    {cast_expr} AS {col}")

    sql = f"""
CREATE VIEW sihsus_typed AS
SELECT
{',\n'.join(select_clauses)},
    ... -- (more columns)
FROM staging_sihsus
WHERE
    -- Filter out invalid rows
    uf_zi IS NOT NULL
    AND dt_inter IS NOT NULL;
"""

    print("\nGenerated SQL (sample):")
    print(sql)


def example_4_polars_schema_conversion():
    """Example 4: Convert DuckDB schema to Polars for Parquet export."""
    print("\n" + "=" * 60)
    print("Example 4: Convert to Polars Schema")
    print("=" * 60)

    polars_schema = get_polars_schema()

    print(f"\nTotal Polars columns: {len(polars_schema)}")

    # Show some conversions
    print("\nDuckDB -> Polars type mappings (sample):")
    sample_cols = ["uf_zi", "idade", "val_tot", "nasc", "sexo", "morte"]
    for col in sample_cols:
        duckdb_type = SIHSUS_PARQUET_SCHEMA[col]
        polars_type = polars_schema[col]
        print(f"  {col:20} : {duckdb_type:12} -> {polars_type}")

    # Count Polars types
    polars_type_counts = {}
    for ptype in polars_schema.values():
        polars_type_counts[ptype] = polars_type_counts.get(ptype, 0) + 1

    print("\nPolars type distribution:")
    for ptype, count in sorted(polars_type_counts.items(), key=lambda x: -x[1]):
        print(f"  {ptype:12} : {count:3} columns")


def example_5_validate_custom_data():
    """Example 5: Validate if custom DataFrame matches schema."""
    print("\n" + "=" * 60)
    print("Example 5: Validate Custom Data Against Schema")
    print("=" * 60)

    # Simulate a custom DataFrame with some columns
    custom_columns = {
        "uf_zi": "INTEGER",
        "idade": "TINYINT",
        "sexo": "VARCHAR",
        "val_tot": "FLOAT",
        "unknown_col": "VARCHAR",  # Not in schema
    }

    print("\nValidating custom columns:")
    for col, dtype in custom_columns.items():
        if col in SIHSUS_PARQUET_SCHEMA:
            expected = SIHSUS_PARQUET_SCHEMA[col]
            match = "✓" if dtype == expected else "✗"
            print(f"  {match} {col:20} : {dtype:12} (expected: {expected})")
        else:
            print(f"  ? {col:20} : {dtype:12} (NOT IN SCHEMA)")


def example_6_schema_subsets():
    """Example 6: Extract schema subsets (e.g., only UTI columns)."""
    print("\n" + "=" * 60)
    print("Example 6: Extract Schema Subsets")
    print("=" * 60)

    # Extract all UTI-related columns
    uti_columns = {
        col: dtype
        for col, dtype in SIHSUS_PARQUET_SCHEMA.items()
        if "uti" in col.lower()
    }

    print(f"\nUTI-related columns ({len(uti_columns)} total):")
    for col, dtype in uti_columns.items():
        print(f"  {col:20} : {dtype}")

    # Extract all value columns
    value_columns = {
        col: dtype
        for col, dtype in SIHSUS_PARQUET_SCHEMA.items()
        if col.startswith("val_")
    }

    print(f"\nValue columns ({len(value_columns)} total):")
    for col, dtype in value_columns.items():
        print(f"  {col:20} : {dtype}")

    # Extract all diagnosis columns
    diag_columns = {
        col: dtype
        for col, dtype in SIHSUS_PARQUET_SCHEMA.items()
        if "diag" in col.lower() or "cid" in col.lower()
    }

    print(f"\nDiagnosis/CID columns ({len(diag_columns)} total):")
    for col, dtype in diag_columns.items():
        print(f"  {col:20} : {dtype}")


def example_7_auto_generate_sql():
    """Example 7: Auto-generate SQL from schema."""
    print("\n" + "=" * 60)
    print("Example 7: Auto-Generate SQL Transformations")
    print("=" * 60)

    # Generate column cleaning SQL
    print("\n1. Column Cleaning SQL (first 5 columns):")
    cleaning_sql = generate_column_cleaning_sql()
    lines = cleaning_sql.split("\n")[:5]
    for line in lines:
        print(f"  {line}")
    print(f"  ... ({len(SIHSUS_PARQUET_SCHEMA) - 5} more columns)")

    # Generate type validation SQL
    print("\n2. Type Validation SQL (first 5 columns):")
    validation_sql = generate_type_validation_sql(suffix="")
    lines = validation_sql.split("\n")[:5]
    for line in lines:
        print(f"  {line}")
    print(f"  ... ({len(SIHSUS_PARQUET_SCHEMA) - 5} more columns)")

    # Get columns by type
    print("\n3. Columns by Type:")
    float_cols = get_columns_by_type("FLOAT")
    print(f"  FLOAT columns ({len(float_cols)} total):")
    for col in float_cols[:10]:
        print(f"    - {col}")
    if len(float_cols) > 10:
        print(f"    ... ({len(float_cols) - 10} more)")

    date_cols = get_columns_by_type("DATE")
    print(f"\n  DATE columns ({len(date_cols)} total):")
    for col in date_cols:
        print(f"    - {col}")

    # Get numeric columns
    print("\n4. All Numeric Columns:")
    numeric = get_numeric_columns()
    print(f"  Total numeric columns: {len(numeric)}")
    print("  Sample numeric columns:")
    for col in numeric[:15]:
        dtype = SIHSUS_PARQUET_SCHEMA[col]
        print(f"    - {col:20} ({dtype})")
    if len(numeric) > 15:
        print(f"    ... ({len(numeric) - 15} more)")


if __name__ == "__main__":
    # Run all examples
    example_1_inspect_schema()
    example_2_generate_sql_casts()
    example_3_generate_full_sql_query()
    example_4_polars_schema_conversion()
    example_5_validate_custom_data()
    example_6_schema_subsets()
    example_7_auto_generate_sql()

    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)
