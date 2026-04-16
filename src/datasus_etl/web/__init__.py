"""Web interface module for DataSUS ETL.

Exposes a FastAPI app that serves a React SPA plus a JSON API. Launch with
``datasus ui`` or directly via::

    uvicorn datasus_etl.web.server:create_app --factory --port 8787

The legacy Streamlit UI has been removed — its SQL templates and data
dictionary live on in :mod:`datasus_etl.web.templates` and
:mod:`datasus_etl.web.dictionary`, which are consumed by the API routes.
"""

from datasus_etl.web.dictionary import get_column_descriptions
from datasus_etl.web.templates import get_templates

__all__ = ["get_templates", "get_column_descriptions"]
