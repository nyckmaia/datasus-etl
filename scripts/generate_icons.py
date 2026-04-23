"""Generate icon files from installer/icons/source.svg.

Produces:
  - installer/icons/icon.png   (1024x1024, for Linux AppImage / source of truth)
  - installer/icons/icon.ico   (Windows: 16/32/48/64/128/256 multi-resolution)
  - installer/icons/icon.icns  (macOS: generated via iconutil — macOS-only step)

Usage:
    python scripts/generate_icons.py

Dependencies:
    - cairosvg (for SVG -> PNG)
    - Pillow   (for PNG -> ICO)
    - iconutil (macOS system tool, used automatically when running on macOS)

When cairosvg/Pillow are absent, skips the corresponding outputs with a
warning so the pipeline can still proceed with whatever icons are checked
in. CI installs both as part of the release workflow.
"""

from __future__ import annotations

import platform
import shutil
import struct
import subprocess
import sys
from pathlib import Path


ICONS_DIR = Path(__file__).resolve().parent.parent / "installer" / "icons"
SOURCE_SVG = ICONS_DIR / "source.svg"
OUT_PNG = ICONS_DIR / "icon.png"
OUT_ICO = ICONS_DIR / "icon.ico"
OUT_ICNS = ICONS_DIR / "icon.icns"


def _svg_to_png(output: Path, size: int) -> bool:
    try:
        import cairosvg  # type: ignore[import-not-found]
    except ImportError:
        print("[generate_icons] cairosvg not installed; cannot rasterize SVG.", file=sys.stderr)
        return False
    cairosvg.svg2png(
        url=str(SOURCE_SVG),
        write_to=str(output),
        output_width=size,
        output_height=size,
    )
    return True


def _build_ico() -> bool:
    try:
        from PIL import Image  # type: ignore[import-not-found]
    except ImportError:
        print("[generate_icons] Pillow not installed; skipping .ico generation.", file=sys.stderr)
        return False
    base = Image.open(OUT_PNG)
    sizes = [(16, 16), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]
    base.save(OUT_ICO, format="ICO", sizes=sizes)
    return True


def _build_icns() -> bool:
    if platform.system() != "Darwin":
        print("[generate_icons] Skipping .icns (only built on macOS).")
        return False
    if shutil.which("iconutil") is None:
        print("[generate_icons] iconutil not found; skipping .icns.", file=sys.stderr)
        return False
    iconset = ICONS_DIR / "icon.iconset"
    if iconset.exists():
        shutil.rmtree(iconset)
    iconset.mkdir()

    # macOS icon set convention.
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
        if not _svg_to_png(iconset / name, size):
            return False
    subprocess.check_call(["iconutil", "-c", "icns", "-o", str(OUT_ICNS), str(iconset)])
    shutil.rmtree(iconset)
    return True


def _fallback_png() -> None:
    # Very last-ditch fallback: a 1x1 transparent PNG, just so the build
    # doesn't fail when cairosvg+Pillow are both missing. The real icon is
    # meant to be produced by CI which installs both.
    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = b"IHDR" + struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)
    ihdr_crc = struct.pack(">I", _png_crc(ihdr))
    idat_payload = b"\x78\x9c\x62\x00\x01\x00\x00\x05\x00\x01\x0d\x0a\x2d\xb4"
    idat = b"IDAT" + idat_payload
    idat_crc = struct.pack(">I", _png_crc(idat))
    iend = b"IEND"
    iend_crc = struct.pack(">I", _png_crc(iend))
    OUT_PNG.write_bytes(
        sig
        + struct.pack(">I", len(ihdr) - 4) + ihdr + ihdr_crc
        + struct.pack(">I", len(idat) - 4) + idat + idat_crc
        + struct.pack(">I", 0) + iend + iend_crc
    )


def _png_crc(data: bytes) -> int:
    import binascii
    return binascii.crc32(data) & 0xFFFFFFFF


def main() -> None:
    if not SOURCE_SVG.is_file():
        raise SystemExit(f"Source SVG missing: {SOURCE_SVG}")

    if not _svg_to_png(OUT_PNG, 1024):
        print("[generate_icons] Writing 1x1 placeholder PNG (install cairosvg for real icon).")
        _fallback_png()

    if not _build_ico():
        # Copy the PNG as a fallback for .ico so Inno Setup at least has a file.
        OUT_ICO.write_bytes(OUT_PNG.read_bytes())

    _build_icns()

    print(f"[generate_icons] PNG:  {OUT_PNG}")
    print(f"[generate_icons] ICO:  {OUT_ICO}")
    if OUT_ICNS.is_file():
        print(f"[generate_icons] ICNS: {OUT_ICNS}")


if __name__ == "__main__":
    main()
