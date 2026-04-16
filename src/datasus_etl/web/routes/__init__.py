"""HTTP route modules for the DataSUS ETL web UI.

Each module exports a ``router: APIRouter`` which is mounted in
:func:`datasus_etl.web.server.create_app` under ``/api/<name>``.
"""
