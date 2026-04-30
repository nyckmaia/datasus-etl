"""Verify per-UF missing-months computation. A UF that has only some months
between its first and last available periods reports `missing_months` equal
to the count of absent months — gaps INSIDE the local range, not gaps vs
today (those are publication lag, a different concept)."""

from __future__ import annotations

from datasus_etl.storage.parquet_manager import count_missing_months


def test_no_gap() -> None:
    files = ["RDSP1801.dbc", "RDSP1802.dbc", "RDSP1803.dbc"]
    assert count_missing_months(files) == 0


def test_one_month_gap() -> None:
    # 1801, 1802, then 1804 — 1803 is missing.
    files = ["RDSP1801.dbc", "RDSP1802.dbc", "RDSP1804.dbc"]
    assert count_missing_months(files) == 1


def test_year_boundary_gap() -> None:
    # 2012-12 jumps to 2014-01: missing 2013-01..2013-12 (12 months).
    files = ["RDSP1212.dbc", "RDSP1401.dbc"]
    assert count_missing_months(files) == 12


def test_multiple_gaps() -> None:
    # 1801, 1804 (skip 02, 03 = 2), 1806 (skip 05 = 1) → 3 missing.
    files = ["RDSP1801.dbc", "RDSP1804.dbc", "RDSP1806.dbc"]
    assert count_missing_months(files) == 3


def test_empty_returns_zero() -> None:
    assert count_missing_months([]) == 0


def test_unparseable_filenames_skipped() -> None:
    files = ["RDSP1801.dbc", "garbage.dbc", "RDSP1803.dbc"]
    # garbage skipped; remaining is 1801 and 1803 → 1 missing (1802).
    assert count_missing_months(files) == 1


def test_pre_2000_year_disambiguation() -> None:
    # Year 92-99 → 1992-1999, year 00-91 → 2000-2091 (matches the existing
    # _sort_files_by_date logic). 9912 → 1999-12, 0001 → 2000-01: contiguous,
    # no gap.
    files = ["RDSP9912.dbc", "RDSP0001.dbc"]
    assert count_missing_months(files) == 0


def test_unsorted_input_handled() -> None:
    # Should not assume input is sorted — sort internally.
    files = ["RDSP1804.dbc", "RDSP1801.dbc", "RDSP1802.dbc"]
    # 1801, 1802, 1804 → 1 missing (1803).
    assert count_missing_months(files) == 1
