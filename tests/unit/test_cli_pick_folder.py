"""Tests for the hidden `_pick-folder` CLI subcommand.

The subcommand is called as a child process by the web UI's folder-picker
endpoint. These tests exercise the Typer wiring without opening a real Tk
dialog by monkeypatching tkinter.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from datasus_etl.cli import app


# Click 8.1 needs mix_stderr=False to split the streams; Click 8.2+ removed
# the kwarg and always splits them. Instantiate compatibly with both.
try:
    runner = CliRunner(mix_stderr=False)  # type: ignore[call-arg]
except TypeError:
    runner = CliRunner()


def _install_fake_tk(monkeypatch: pytest.MonkeyPatch, chosen: str) -> MagicMock:
    tk_mod = MagicMock()
    root = MagicMock()
    tk_mod.Tk.return_value = root
    filedialog = MagicMock()
    filedialog.askdirectory.return_value = chosen
    tk_mod.filedialog = filedialog
    monkeypatch.setitem(sys.modules, "tkinter", tk_mod)
    monkeypatch.setitem(sys.modules, "tkinter.filedialog", filedialog)
    return tk_mod


def test_pick_folder_prints_selected_path(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_tk(monkeypatch, r"C:\Users\nyck\data")
    result = runner.invoke(app, ["_pick-folder"])
    assert result.exit_code == 0
    assert result.stdout == r"C:\Users\nyck\data"


def test_pick_folder_cancel_emits_empty_string(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fake_tk(monkeypatch, "")
    result = runner.invoke(app, ["_pick-folder"])
    assert result.exit_code == 0
    assert result.stdout == ""


def test_pick_folder_missing_tkinter_exits_2(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate tkinter unavailable by making the import raise ImportError.
    real_import = __builtins__["__import__"] if isinstance(__builtins__, dict) else __builtins__.__import__

    def _raising(name: str, *args: object, **kwargs: object) -> object:
        if name == "tkinter" or name.startswith("tkinter."):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _raising)
    monkeypatch.delitem(sys.modules, "tkinter", raising=False)
    monkeypatch.delitem(sys.modules, "tkinter.filedialog", raising=False)

    result = runner.invoke(app, ["_pick-folder"])
    assert result.exit_code == 2
    assert "TK_MISSING" in result.stderr


def test_pick_folder_is_hidden_in_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "_pick-folder" not in result.stdout
