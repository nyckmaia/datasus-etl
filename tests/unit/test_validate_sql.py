"""Hardened read-only guard for the SQL editor and the export endpoint.

The validator now (a) strips SQL comments before the regex pass — a comment
must not be able to disguise a forbidden keyword — and (b) extends the
denylist with every DuckDB statement type that mutates state or loads
extensions/files. Both failure modes return HTTP 400 (the FastAPI test
client surfaces this as `HTTPException`).
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from datasus_etl.web.routes.query import _validate_sql


# ── happy paths ───────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "select * from sihsus where uf = 'SP'",
        "WITH cte AS (SELECT 1 AS a) SELECT * FROM cte",
        "  \n select 1\n ",
        "SELECT 1 -- trailing line comment\n",
        "SELECT 1 /* inline block */",
        "SELECT 1; ",  # trailing semicolon is stripped, single statement OK
    ],
)
def test_valid_select_passes(sql: str) -> None:
    _validate_sql(sql)  # no exception


# ── forbidden keywords (existing + new) ───────────────────────────────────

@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO sihsus VALUES (1)",
        "UPDATE sihsus SET val_tot = 0",
        "DELETE FROM sihsus",
        "DROP VIEW sihsus_all",
        "CREATE TABLE x AS SELECT 1",
        "ALTER TABLE x ADD COLUMN y INT",
        "ATTACH 'evil.db'",
        "COPY sihsus TO '/tmp/x.csv'",
        "PRAGMA enable_profiling",
        "EXPORT DATABASE '/tmp/x'",
        "IMPORT DATABASE '/tmp/x'",
        "REPLACE INTO sihsus VALUES (1)",
        # Newly blocked:
        "TRUNCATE sihsus",
        "MERGE INTO sihsus USING src ON …",
        "UPSERT INTO sihsus VALUES (1)",
        "CALL pragma_database_list()",
        "INSTALL httpfs",
        "LOAD httpfs",
        "SET memory_limit = '1GB'",
        "USE main",
        "DETACH ibge_locais",
        "CHECKPOINT",
        "VACUUM",
        "GRANT ALL ON sihsus TO public",
        "REVOKE ALL ON sihsus FROM public",
        "BEGIN TRANSACTION",
        "COMMIT",
        "ROLLBACK",
    ],
)
def test_forbidden_keyword_blocked(sql: str) -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_sql(sql)
    assert exc.value.status_code == 400


# ── comment-based bypass attempts ─────────────────────────────────────────

@pytest.mark.parametrize(
    "sql",
    [
        "/* hi */ DELETE FROM sihsus",
        "-- innocuous\nDROP VIEW sihsus_all",
        "SELECT 1; /* fake */ DELETE FROM sihsus",
        "/* nested /* */ DELETE FROM sihsus",
        "SELECT/*x*/ 1 UNION /*y*/ SELECT 2; DROP TABLE x",
        "WITH x AS (/*ok*/ SELECT 1) /*sneaky*/ INSERT INTO x VALUES (1)",
    ],
)
def test_comment_bypass_blocked(sql: str) -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_sql(sql)
    assert exc.value.status_code == 400


# ── multi-statement blocking is preserved ─────────────────────────────────

def test_multi_statement_blocked() -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_sql("SELECT 1; SELECT 2")
    assert exc.value.status_code == 400


# ── non-SELECT/WITH allowlist is preserved ────────────────────────────────

def test_unknown_leading_keyword_blocked() -> None:
    with pytest.raises(HTTPException) as exc:
        _validate_sql("DESCRIBE sihsus")  # not SELECT/WITH
    assert exc.value.status_code == 400
