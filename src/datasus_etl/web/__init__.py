"""Web interface module for DataSUS ETL (Streamlit-based).

Provides a user-friendly web interface for health researchers:
- Downloading and processing DataSUS data with progress tracking
- Querying datasets with SQL templates for common analyses
- Visualizing data statistics with Plotly charts
- Exporting data to CSV/Excel with size estimation

Run with: streamlit run src/datasus_etl/web/app.py
Or:       datasus ui
"""

from datasus_etl.web.app import main
from datasus_etl.web.templates import get_templates
from datasus_etl.web.dictionary import get_column_descriptions

__all__ = ["main", "get_templates", "get_column_descriptions"]
