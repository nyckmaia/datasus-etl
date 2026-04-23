"""Generate icon files for the three platform installers.

Outputs (all under installer/icons/):
  - icon.png   — 1024x1024, source of truth for Linux AppImage
  - icon.ico   — Windows multi-resolution icon (Inno Setup)
  - icon.icns  — macOS .app bundle icon (DMG)

Workflow:

  Developer (has cairosvg + Pillow installed):
    Regenerates icon.png and icon.ico from installer/icons/source.svg.

  CI or anyone without cairosvg:
    Uses the committed icon.png and icon.ico as-is. Those are checked into
    git because rasterizing SVG on Windows/macOS CI would require
    installing libcairo, which is heavy for a one-file render.

  macOS (any):
    Always (re)builds icon.icns from icon.png using the system tools
    `sips` and `iconutil`. No Python deps needed.

Usage:
    python scripts/generate_icons.py
"""

from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from pathlib import Path


ICONS_DIR = Path(__file__).resolve().parent.parent / "installer" / "icons"
SOURCE_SVG = ICONS_DIR / "source.svg"
OUT_PNG = ICONS_DIR / "icon.png"
OUT_ICO = ICONS_DIR / "icon.ico"
OUT_ICNS = ICONS_DIR / "icon.icns"


def _have_cairosvg() -> bool:
    try:
        import cairosvg  # type: ignore[import-not-found]  # noqa: F401
        return True
    except (ImportError, OSError):
        # cairosvg imports succeed but libcairo resolution can raise OSError
        # on systems without the native lib (Windows/macOS runners).
        return False


def _have_pillow() -> bool:
    try:
        import PIL  # type: ignore[import-not-found]  # noqa: F401
        return True
    except ImportError:
        return False


def _regen_png_from_svg() -> bool:
    if not _have_cairosvg():
        return False
    import cairosvg  # type: ignore[import-not-found]
    print(f"[generate_icons] Rasterizing {SOURCE_SVG.name} -> icon.png (1024px)")
    cairosvg.svg2png(
        url=str(SOURCE_SVG),
        write_to=str(OUT_PNG),
        output_width=1024,
        output_height=1024,
    )
    return True


def _regen_ico_from_png() -> bool:
    if not _have_pillow():
        return False
    from PIL import Image  # type: ignore[import-not-found]
    print(f"[generate_icons] Building icon.ico (multi-res) from icon.png")
    base = Image.open(OUT_PNG)
    sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    base.save(OUT_ICO, format="ICO", sizes=sizes)
    return True


def _regen_icns_with_iconutil() -> bool:
    """Produce .icns from the committed icon.png using only macOS tools."""
    if platform.system() != "Darwin":
        print("[generate_icons] Skipping .icns (iconutil is macOS-only).")
        return False
    if shutil.which("iconutil") is None or shutil.which("sips") is None:
        print("[generate_icons] iconutil or sips missing; skipping .icns.", file=sys.stderr)
        return False
    if not OUT_PNG.is_file():
        print(f"[generate_icons] {OUT_PNG} missing; cannot build .icns.", file=sys.stderr)
        return False

    iconset = ICONS_DIR / "icon.iconset"
    if iconset.exists():
        shutil.rmtree(iconset)
    iconset.mkdir()

    # macOS icon set convention. 'sips -z H W <src> --out <dst>' resizes.
    targets = [
        (16, "icon_16x16.png"),
        (32, "icon_16x16@2x.png"),
        (32, "icon_32x32.png"),
        (64, "icon_32x32@2x.png"),
        (128, "icon_128x128.png"),
        (256, "icon_128x128@2x.png"),
        (256, "icon_256x256.png"),
        (512, "icon_256x256@2x.png"),
        (512, "icon_512x512.png"),
        (1024, "icon_512x512@2x.png"),
    ]
    for size, name in targets:
        subprocess.check_call(
            ["sips", "-z", str(size), str(size), str(OUT_PNG), "--out", str(iconset / name)],
            stdout=subprocess.DEVNULL,
        )

    print(f"[generate_icons] Building icon.icns from {iconset.name}")
    subprocess.check_call(["iconutil", "-c", "icns", "-o", str(OUT_ICNS), str(iconset)])
    shutil.rmtree(iconset)
    return True


def main() -> None:
    if not SOURCE_SVG.is_file():
        raise SystemExit(f"Source SVG missing: {SOURCE_SVG}")

    # 1. PNG — only regen if the developer has cairosvg. Otherwise use what's committed.
    if _regen_png_from_svg():
        pass
    elif OUT_PNG.is_file():
        print(f"[generate_icons] Using committed {OUT_PNG.name} (no cairosvg).")
    else:
        raise SystemExit(
            "icon.png missing and cairosvg unavailable — "
            "generate icon.png locally first (pip install cairosvg Pillow)."
        )

    # 2. ICO — only regen if Pillow is available. Otherwise use what's committed.
    if _regen_ico_from_png():
        pass
    elif OUT_ICO.is_file():
        print(f"[generate_icons] Using committed {OUT_ICO.name} (no Pillow).")
    else:
        raise SystemExit(
            "icon.ico missing and Pillow unavailable — "
            "generate icon.ico locally first (pip install Pillow)."
        )

    # 3. ICNS — always (re)built on macOS from icon.png via iconutil.
    _regen_icns_with_iconutil()

    print(f"[generate_icons] PNG:  {OUT_PNG}")
    print(f"[generate_icons] ICO:  {OUT_ICO}")
    if OUT_ICNS.is_file():
        print(f"[generate_icons] ICNS: {OUT_ICNS}")


if __name__ == "__main__":
    main()
