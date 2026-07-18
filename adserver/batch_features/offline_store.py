"""DuckDB query layer over the date-partitioned offline Parquet store
(`data/features/entity=<user|ad>/asof=<date>/features.parquet`), supporting
point-in-time reads: "give me feature values as of date D."

Each partition is already a full snapshot computed using only data through
its own `as_of` date (enforced by the jobs themselves, via
`jobs/_shared.py`'s trailing-window filtering) — so a point-in-time read
for date D is simply "find the most recent partition whose asof <= D,
return it whole." No cross-partition merging is needed or correct: an
older partition is a complete, self-consistent snapshot on its own.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import polars as pl

from adserver.batch_features.runner import DEFAULT_OUTPUT_DIR


def available_partitions(entity: str, output_dir: Path = DEFAULT_OUTPUT_DIR) -> list[dt.date]:
    """Every asof date materialized for `entity`, via DuckDB hive-partition
    discovery over the directory structure (not a Python glob/os.listdir —
    this is the "queried via DuckDB" mechanism the offline store uses)."""
    entity_dir = output_dir / f"entity={entity}"
    if not entity_dir.exists():
        return []

    con = duckdb.connect()
    partitions_glob = str(entity_dir / "asof=*" / "features.parquet")
    # "asof" is quoted below because it collides with DuckDB's ASOF JOIN keyword.
    dates = con.execute(
        'SELECT DISTINCT "asof" FROM read_parquet(?, hive_partitioning = true)',
        [partitions_glob],
    ).fetchall()
    return sorted(dt.date.fromisoformat(str(row[0])) for row in dates)


def query_as_of(
    entity: str, as_of: dt.date, output_dir: Path = DEFAULT_OUTPUT_DIR
) -> pl.DataFrame:
    """The most recent materialized snapshot for `entity` with asof <= `as_of`.

    Returns an empty frame if no partition qualifies (nothing materialized
    yet, or every partition is after `as_of`).
    """
    eligible = [d for d in available_partitions(entity, output_dir) if d <= as_of]
    if not eligible:
        return pl.DataFrame()

    latest = max(eligible)
    partition_path = output_dir / f"entity={entity}" / f"asof={latest.isoformat()}" / "features.parquet"

    con = duckdb.connect()
    return con.execute("SELECT * FROM read_parquet(?)", [str(partition_path)]).pl()
