"""Decision log = system of record: one JSON line per request, appended
to a `.jsonl` file — loadable into DuckDB directly (`read_json_auto`),
same "plain files, queryable via a real SQL engine" philosophy as the
offline Parquet store. Every field a request's outcome depends on is
captured here so it can be reconstructed after the fact: what was
eligible, what was excluded and why, what was scored, what the external
bidder did, who won, and which degradation rung (if any) fired.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

DEFAULT_LOG_PATH = Path("data/decision_log.jsonl")

_write_lock = threading.Lock()


def log_decision(entry: dict[str, Any], path: Path = DEFAULT_LOG_PATH) -> None:
    """Appends one JSON line. A single process-wide lock serializes writes
    — simple and sufficient at this project's request volume; a real
    system would use a dedicated log shipper instead of appending
    in-process."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, default=str) + "\n"
    with _write_lock:
        with path.open("a") as f:
            f.write(line)


def read_decisions(path: Path = DEFAULT_LOG_PATH) -> list[dict[str, Any]]:
    """Reads every logged decision back — for tests and ad-hoc DuckDB-style
    querying without needing DuckDB itself for small local runs."""
    if not path.exists():
        return []
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]
