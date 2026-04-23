"""Generate a changelog section for a release from Conventional Commits.

Reads ``git log <last-tag>..HEAD`` (or the whole history if there's no prior
tag), parses commit subjects matching ``<type>(<scope>)?: <subject>``, and
writes a Markdown section under ``## [X.Y.Z] - YYYY-MM-DD`` grouped by type.

Conventional Commit types are mapped to sections:

    feat      -> ### Added
    fix       -> ### Fixed
    refactor  -> ### Changed
    perf      -> ### Performance
    docs      -> ### Documentation
    *         -> ### Other   (chore, revert, style, and un-prefixed commits)

Ignored entirely (noise, not user-facing):
    ci, test, build

Usage:
    python scripts/generate_changelog.py --version 0.2.0 --output out.md

If --version is omitted, reads the root VERSION file.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import subprocess
import sys
from collections import OrderedDict
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERSION_FILE = ROOT / "VERSION"

COMMIT_PATTERN = re.compile(
    r"^(?P<type>\w+)(?:\((?P<scope>[^)]+)\))?(?P<bang>!)?:\s*(?P<subject>.+)$"
)

SECTION_ORDER: "OrderedDict[str, str]" = OrderedDict(
    (
        ("feat", "### Added"),
        ("fix", "### Fixed"),
        ("perf", "### Performance"),
        ("refactor", "### Changed"),
        ("docs", "### Documentation"),
        ("other", "### Other"),
    )
)

IGNORED_TYPES = {"ci", "test", "build", "style"}


def _last_tag() -> str | None:
    try:
        out = subprocess.check_output(
            ["git", "describe", "--tags", "--abbrev=0", "--match=v*"],
            cwd=ROOT,
            text=True,
        ).strip()
        return out or None
    except subprocess.CalledProcessError:
        return None


def _git_log(since: str | None) -> list[tuple[str, str]]:
    rev_range = f"{since}..HEAD" if since else "HEAD"
    # %H<TAB>%s — commit hash + subject
    out = subprocess.check_output(
        ["git", "log", rev_range, "--no-merges", "--format=%H%x09%s"],
        cwd=ROOT,
        text=True,
    )
    entries: list[tuple[str, str]] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        sha, _, subject = line.partition("\t")
        entries.append((sha.strip(), subject.strip()))
    return entries


def _classify(subject: str) -> tuple[str, str, bool]:
    # Returns (section-key, cleaned-subject, is-breaking)
    m = COMMIT_PATTERN.match(subject)
    if not m:
        return "other", subject, False
    raw_type = m.group("type").lower()
    scope = m.group("scope")
    bang = bool(m.group("bang"))
    rest = m.group("subject").strip()
    if raw_type in IGNORED_TYPES and not bang:
        return "", "", False  # skipped
    section = raw_type if raw_type in SECTION_ORDER else "other"
    # Preserve the scope in the rendered bullet for context, e.g.
    # "feat(web-ui): add toggle" -> "**web-ui:** add toggle"
    if scope:
        cleaned = f"**{scope}:** {rest}"
    else:
        cleaned = rest
    return section, cleaned, bang


def render(version: str, entries: list[tuple[str, str]], date: dt.date) -> str:
    buckets: "OrderedDict[str, list[str]]" = OrderedDict(
        (key, []) for key in SECTION_ORDER
    )
    breaking: list[str] = []

    for _sha, subject in entries:
        section, cleaned, is_breaking = _classify(subject)
        if not section:
            continue
        buckets[section].append(cleaned)
        if is_breaking:
            breaking.append(cleaned)

    lines: list[str] = [f"## [{version}] - {date.isoformat()}", ""]
    if breaking:
        lines.append("### ⚠ Breaking changes")
        for item in breaking:
            lines.append(f"- {item}")
        lines.append("")

    has_any = breaking or any(buckets.values())
    for key, items in buckets.items():
        if not items:
            continue
        heading = SECTION_ORDER[key]
        lines.append(heading)
        for item in items:
            lines.append(f"- {item}")
        lines.append("")

    if not has_any:
        lines.append("_No user-facing changes._")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", help="Release version (defaults to VERSION file)")
    parser.add_argument(
        "--output",
        type=Path,
        help="Write to this file. If omitted, prints to stdout.",
    )
    parser.add_argument(
        "--since",
        help="Git rev to diff from (defaults to the latest v* tag, or full history).",
    )
    args = parser.parse_args()

    version = args.version or VERSION_FILE.read_text(encoding="utf-8").strip()
    if not re.match(r"^\d+\.\d+\.\d+$", version):
        print(f"Invalid version: {version!r}", file=sys.stderr)
        sys.exit(1)

    since = args.since or _last_tag()
    entries = _git_log(since)
    today = dt.date.today()
    section = render(version, entries, today)

    if args.output:
        args.output.write_text(section, encoding="utf-8")
    else:
        sys.stdout.write(section)


if __name__ == "__main__":
    main()
