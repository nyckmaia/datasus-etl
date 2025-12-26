"""Quick script to check transformation results."""
import duckdb

conn = duckdb.connect('examples/data/datasus/temp/duckdb.db', read_only=True)

# Check staging table row count
staging_count = conn.execute('SELECT COUNT(*) FROM staging_RDSP2301').fetchone()[0]
print(f'Staging table rows: {staging_count:,}')

# Check transformed view row count
try:
    view_count = conn.execute('SELECT COUNT(*) FROM transformed_RDSP2301').fetchone()[0]
    print(f'Transformed view rows: {view_count:,}')
except Exception as e:
    print(f'View error: {e}')

# Show column names from staging
print('\nStaging table columns (first 15):')
columns = conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'staging_RDSP2301' LIMIT 15").fetchall()
for col in columns:
    print(f'  - {col[0]}')

# Check dt_inter column values
print('\nChecking dt_inter column in staging (sample 5 rows):')
dt_sample = conn.execute("SELECT dt_inter FROM staging_RDSP2301 LIMIT 5").fetchall()
for row in dt_sample:
    print(f'  - {row[0]}')

conn.close()
