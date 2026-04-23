"""Hatch build hook that bundles the React SPA into the wheel.

When ``python -m build`` runs, this hook invokes ``bun install`` + ``bun run
build`` inside ``web-ui/``, which writes the compiled assets to
``src/datasus_etl/web/static/``. Hatch then picks them up via the
``artifacts`` glob declared in ``pyproject.toml``.

The hook is a no-op if ``web-ui/`` is absent (source checkouts without the
frontend sources, or editable installs where the developer builds manually).
A build fails only if Bun is available *and* the build itself errors —
missing Bun prints a warning and continues so the Python-only wheel still
works.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class BuildUIHook(BuildHookInterface):
    PLUGIN_NAME = "build_ui"

    def initialize(self, version: str, build_data: dict) -> None:  # noqa: ARG002
        root = Path(self.root)
        self._sync_version_module(root)

        ui_dir = root / "web-ui"
        static_dir = root / "src" / "datasus_etl" / "web" / "static"

        if not ui_dir.is_dir():
            self.app.display_info("web-ui/ not present; skipping SPA bundle.")
            return

        if os.environ.get("DATASUS_SKIP_UI_BUILD"):
            self.app.display_info("DATASUS_SKIP_UI_BUILD set; skipping SPA bundle.")
            return

        bun = shutil.which("bun")
        if bun is None:
            self.app.display_warning(
                "Bun is not installed — the SPA was not rebuilt. "
                "If src/datasus_etl/web/static/ already exists it will be shipped as-is."
            )
            return

        if not (ui_dir / "node_modules").exists():
            self.app.display_info("Running `bun install` in web-ui/ …")
            subprocess.check_call([bun, "install"], cwd=ui_dir)

        self.app.display_info("Running `bun run build` in web-ui/ …")
        subprocess.check_call([bun, "run", "build"], cwd=ui_dir)

        if not (static_dir / "index.html").exists():
            raise RuntimeError(
                f"SPA build did not produce {static_dir / 'index.html'} — "
                "check the Vite config (build.outDir)."
            )

    def _sync_version_module(self, root: Path) -> None:
        version_file = root / "VERSION"
        module_file = root / "src" / "datasus_etl" / "__version__.py"
        if not version_file.is_file():
            self.app.display_warning("VERSION file missing; leaving __version__.py untouched.")
            return

        version = version_file.read_text(encoding="utf-8").strip()
        expected = (
            '"""Version information for DataSUS-ETL.\n\n'
            "This file is regenerated from the repository-root VERSION file by\n"
            "the hatch build hook. Edit VERSION, not this file.\n"
            '"""\n\n'
            f'__version__ = "{version}"\n'
            '__version_info__ = tuple(int(i) for i in __version__.split(".") if i.isdigit())\n'
        )
        current = module_file.read_text(encoding="utf-8") if module_file.exists() else ""
        if current != expected:
            module_file.write_text(expected, encoding="utf-8")
            self.app.display_info(f"Synced __version__.py to VERSION ({version}).")
