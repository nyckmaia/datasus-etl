"""Build a Nuitka standalone distribution of datasus-etl.

Invoked locally and from the GitHub Actions release matrix. Produces:

- Windows: ``dist/datasus-etl.dist/`` (a directory tree) with ``datasus.exe``
  as the console-less entry point. The Inno Setup script packages this dir.
- macOS:   ``dist/datasus-etl.app`` (a ``.app`` bundle). ``create-dmg`` wraps
  it into ``.dmg``. A shell wrapper at ``Contents/MacOS/datasus-ui`` is
  injected post-build so double-clicking the app runs ``datasus ui``.
- Linux:   ``dist/datasus-etl.dist/`` — the same as Windows, but packaged
  later as an AppImage by ``linuxdeploy`` + ``appimagetool``.

The command line is intentionally long — each flag is here because a
concrete dependency failed without it (see the design brief at
C:/Users/nyck/.claude/plans/este-projeto-est-hospedado-parsed-anchor.md).

Usage:
    python scripts/build_nuitka.py [--output-dir dist] [--jobs N]

Environment variables:
    DATASUS_SKIP_UI_BUILD=1   Skip the `bun run build` pre-step (assume the
                              caller already produced src/.../web/static/).
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SRC_PKG = ROOT / "src" / "datasus_etl"
WEB_STATIC = SRC_PKG / "web" / "static"
DATA_DIR = SRC_PKG / "_data"
ICONS_DIR = ROOT / "installer" / "icons"
VERSION_FILE = ROOT / "VERSION"

# Pass the PACKAGE DIRECTORY (not __main__.py). Passing __main__.py directly
# makes Nuitka treat it as a standalone script and produces a circular import
# inside the stdlib (types.py -> pathlib -> fnmatch -> re -> enum -> types).
# Nuitka itself warns about this if you pass __main__.py. See the docs at
# https://nuitka.net/doc/user-manual.html (Tips → "Standalone and packages").
ENTRY_POINT = SRC_PKG


COMMON_FLAGS = [
    "--standalone",
    "--assume-yes-for-downloads",
    "--lto=yes",
    "--python-flag=-O",
    "--remove-output",
    "--enable-plugin=tk-inter",
    # Explicit package pins — Nuitka's autodetection misses some of these
    # (lazy imports, native extensions, template files).
    "--include-package=pyarrow",
    "--include-package-data=pyarrow",
    "--include-package=polars",
    "--include-package-data=polars",
    "--include-package=duckdb",
    "--include-package=datasus_dbc",
    "--include-package=dbfread",
    "--include-package=simpledbf",
    "--include-package=uvicorn",
    "--include-package=httptools",
    "--include-package=websockets",
    "--include-package=watchfiles",
    "--include-package=fastapi",
    "--include-package=starlette",
    "--include-package=sse_starlette",
    "--include-package=openpyxl",
    "--include-package=xlrd",
    "--include-package=pandas",
    "--include-package-data=pandas",
    # Small pure-Python deps that Nuitka's autodetect has been known to miss.
    # tomli_w is used by web/user_config.py to persist the ~/.config/... file;
    # without it the whole web server fails to import at startup.
    "--include-package=tomli_w",
    "--include-package=psutil",
    # rich lazy-loads per-Unicode-version submodules from rich._unicode_data
    # (e.g. unicode17-0-0) via importlib; Nuitka's static analysis cannot see
    # those names, so include the whole package + data explicitly.
    "--include-package=rich",
    "--include-package-data=rich",
    # `datasus ui` calls uvicorn.run("datasus_etl.web.server:create_app", ...)
    # as a STRING — Nuitka can't trace string-based imports, so without this
    # the whole web subpackage (server, routes/*, runtime, user_config) is
    # missing from the bundle and the UI crashes with ModuleNotFoundError on
    # startup.
    "--include-package=datasus_etl.web",
    # Trim the fat.
    "--nofollow-import-to=pandas.tests",
    "--nofollow-import-to=pyarrow.tests",
    "--nofollow-import-to=numpy.tests",
    "--nofollow-import-to=IPython",
    "--nofollow-import-to=jupyter",
    "--nofollow-import-to=matplotlib",
    "--nofollow-import-to=notebook",
]


def _read_version() -> str:
    return VERSION_FILE.read_text(encoding="utf-8").strip()


def _windows_version(version: str) -> str:
    # Nuitka on Windows emits a Win32 VERSIONINFO resource, which requires a
    # 4-part version (major.minor.patch.build). Our VERSION is semver X.Y.Z.
    return f"{version}.0" if version.count(".") == 2 else version


def _platform_flags(version: str) -> list[str]:
    system = platform.system()
    flags: list[str] = []

    if system == "Windows":
        win_version = _windows_version(version)
        flags += [
            "--msvc=latest",
            # attach: use parent console when launched from a shell (so
            # `datasus.exe ui` shows log output), no window when launched
            # from Explorer or a desktop shortcut. Beats "disable" because
            # disable makes stdout/stderr a black hole and startup errors
            # become invisible.
            "--windows-console-mode=attach",
            f"--windows-icon-from-ico={ICONS_DIR / 'icon.ico'}",
            "--company-name=DataSUS ETL",
            "--product-name=DataSUS ETL",
            "--file-description=Pipeline for Brazilian public-health data",
            # Nuitka refuses to emit VERSIONINFO without explicit file/product
            # versions once any of the name fields above is set.
            f"--file-version={win_version}",
            f"--product-version={win_version}",
        ]
    elif system == "Darwin":
        flags += [
            "--macos-create-app-bundle",
            "--macos-app-name=DataSUS ETL",
            f"--macos-app-icon={ICONS_DIR / 'icon.icns'}",
            "--macos-app-mode=gui",
            f"--macos-app-version={version}",
        ]
    # uvloop is POSIX-only (no Windows wheel); include it on Linux/macOS.
    if system in ("Linux", "Darwin"):
        flags += ["--include-package=uvloop"]

    # Data directories (same on all platforms).
    flags += [
        f"--include-data-dir={DATA_DIR}=datasus_etl/_data",
        f"--include-data-dir={WEB_STATIC}=datasus_etl/web/static",
    ]

    return flags


def _output_flags(output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    return [
        f"--output-dir={output_dir}",
        "--output-filename=datasus",
    ]


def _preflight() -> None:
    version_file = ROOT / "VERSION"
    if not version_file.is_file():
        raise SystemExit("VERSION file is missing at repository root.")

    if not (WEB_STATIC / "index.html").is_file():
        raise SystemExit(
            f"SPA bundle missing at {WEB_STATIC / 'index.html'}. "
            "Run `cd web-ui && bun install && bun run build` first "
            "(CI must do this before invoking this script)."
        )

    # Ensure __version__.py matches VERSION so the binary reports the right
    # number. The hatch build hook normally does this, but Nuitka builds
    # skip hatch entirely.
    version = version_file.read_text(encoding="utf-8").strip()
    module_file = SRC_PKG / "__version__.py"
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
        print(f"[build_nuitka] Synced __version__.py to VERSION ({version}).")


def _build(output_dir: Path, jobs: int | None) -> Path:
    version = _read_version()
    cmd: list[str] = [
        sys.executable,
        "-m",
        "nuitka",
        *COMMON_FLAGS,
        *_platform_flags(version),
        *_output_flags(output_dir),
    ]
    if jobs:
        cmd.append(f"--jobs={jobs}")
    cmd.append(str(ENTRY_POINT))

    print("[build_nuitka] " + " ".join(cmd))
    subprocess.check_call(cmd, cwd=ROOT)

    if platform.system() == "Darwin":
        canonical_app = output_dir / "datasus-etl.app"
        if canonical_app.is_dir():
            app_bundle = canonical_app
        else:
            candidates = [p for p in output_dir.glob("*.app")]
            if not candidates:
                raise SystemExit(f"Nuitka did not produce a .app bundle under {output_dir}")
            app_bundle = candidates[0]
            if app_bundle != canonical_app:
                print(f"[build_nuitka] Renaming {app_bundle.name} -> {canonical_app.name}")
                app_bundle.rename(canonical_app)
                app_bundle = canonical_app
        _inject_macos_wrapper(app_bundle)
        return app_bundle

    # Windows/Linux: the dist folder is named after the main module
    # (__main__.dist by default). Rename it deterministically so all
    # downstream steps (smoke test, installer packaging) can point at a
    # single canonical path.
    canonical_dist = output_dir / "datasus-etl.dist"
    candidates = [
        p for p in output_dir.iterdir()
        if p.is_dir() and p.name.endswith(".dist") and p.name != canonical_dist.name
    ]
    if not candidates and canonical_dist.is_dir():
        return canonical_dist
    if not candidates:
        raise SystemExit(f"Nuitka did not produce a .dist folder under {output_dir}")
    produced = candidates[0]
    if canonical_dist.is_dir():
        shutil.rmtree(canonical_dist)
    print(f"[build_nuitka] Renaming {produced.name} -> {canonical_dist.name}")
    produced.rename(canonical_dist)
    return canonical_dist


def _inject_macos_wrapper(app_bundle: Path) -> None:
    # The compiled binary accepts the full Typer CLI; double-clicking an
    # .app runs CFBundleExecutable with no args, so we drop a shell wrapper
    # that forwards to `datasus ui`.
    macos_dir = app_bundle / "Contents" / "MacOS"
    if not macos_dir.is_dir():
        print(f"[build_nuitka] WARNING: {macos_dir} does not exist; skipping wrapper.")
        return

    wrapper = macos_dir / "datasus-ui"
    wrapper.write_text(
        "#!/bin/sh\n"
        'exec "$(dirname "$0")/datasus" ui "$@"\n',
        encoding="utf-8",
    )
    wrapper.chmod(0o755)

    plist = app_bundle / "Contents" / "Info.plist"
    if plist.is_file():
        text = plist.read_text(encoding="utf-8")
        # Nuitka points CFBundleExecutable at the raw binary; redirect it to
        # the wrapper so opening the .app always runs the UI.
        text = text.replace(
            "<key>CFBundleExecutable</key>\n\t<string>datasus</string>",
            "<key>CFBundleExecutable</key>\n\t<string>datasus-ui</string>",
        )
        plist.write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    parser.add_argument("--jobs", type=int, default=None)
    parser.add_argument(
        "--skip-ui-build",
        action="store_true",
        help="Skip the web-ui pre-build (assume web/static/ is already populated).",
    )
    args = parser.parse_args()

    _preflight()

    if not args.skip_ui_build and not os.environ.get("DATASUS_SKIP_UI_BUILD"):
        _build_spa()

    artifact = _build(args.output_dir, args.jobs)
    print(f"[build_nuitka] Done. Artifact at: {artifact}")


def _build_spa() -> None:
    ui_dir = ROOT / "web-ui"
    if not ui_dir.is_dir():
        print("[build_nuitka] web-ui/ not present; skipping SPA build.")
        return
    bun = shutil.which("bun")
    if bun is None:
        raise SystemExit("Bun is required to build the SPA. Install from https://bun.sh")
    if not (ui_dir / "node_modules").is_dir():
        subprocess.check_call([bun, "install"], cwd=ui_dir)
    subprocess.check_call([bun, "run", "build"], cwd=ui_dir)


if __name__ == "__main__":
    main()
