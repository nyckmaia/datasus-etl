"""Web interface module for PyDataSUS (Streamlit-based).

Provides a user-friendly web interface for:
- Downloading and processing DataSUS data
- Querying existing Parquet datasets
- Exporting data to CSV/Excel formats

Run with: streamlit run src/pydatasus/web/app.py
"""

from pydatasus.web.app import main

__all__ = ["main"]
