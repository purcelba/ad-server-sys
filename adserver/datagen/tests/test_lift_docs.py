"""AC3: the planted-effects table in the README(s) must match lifts.py
exactly, so the doc can never silently drift from the generator."""

import re
from pathlib import Path

from adserver.datagen.lifts import LIFT_TABLE

REPO_ROOT = Path(__file__).resolve().parents[3]

ROW_RE = re.compile(
    r"^\|\s*(\w+)\s*\|\s*([\w ]+?)\s*\|\s*([\w :().–\-]+?)\s*\|\s*([\d.]+)x"
)


def _parse_table_rows(markdown_path: Path) -> list[tuple[str, str, str, float]]:
    text = markdown_path.read_text()
    rows = []
    for line in text.splitlines():
        m = ROW_RE.match(line.strip())
        if not m:
            continue
        segment, category_raw, condition_raw, lift = m.groups()
        category = "*" if category_raw.strip() == "all categories" else category_raw.strip()
        condition = "any" if condition_raw.strip() == "any" else condition_raw.strip().split()[0]
        rows.append((segment, category, condition, float(lift)))
    return rows


def _expected() -> set[tuple[str, str, str, float]]:
    return set(LIFT_TABLE)


def test_datagen_readme_table_matches_lifts_py():
    rows = _parse_table_rows(REPO_ROOT / "adserver" / "datagen" / "README.md")
    assert set(rows) == _expected()


def test_repo_readme_table_matches_lifts_py():
    rows = _parse_table_rows(REPO_ROOT / "README.md")
    assert set(rows) == _expected()
