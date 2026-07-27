"""Plain, `st.*`-free functions the Rider/Ops tabs render around.
Streamlit scripts are hard to unit test directly - keeping anything worth
testing (decision lookup, rate math, display labels) here instead means
`adserver/ui/tests/test_logic.py` can cover it without driving Streamlit.
"""

from __future__ import annotations

from typing import Any


def find_decision(decisions: list[dict[str, Any]], request_id: str) -> dict[str, Any] | None:
    """Most decision-log entries are found by the `request_id` a `/serve`
    call just returned - `read_decisions()` returns the whole file in
    append order, so this is a linear scan from the end (most recent
    match first, in case `request_id`s were ever reused, which they
    shouldn't be, but the debug UI shouldn't silently show a stale one)."""
    for decision in reversed(decisions):
        if decision.get("request_id") == request_id:
            return decision
    return None


def compute_rate(prev_count: int, curr_count: int, elapsed_s: float) -> float:
    """Requests/sec between two `/metrics` polls. `/metrics` only reports
    cumulative counts, never a live rate, so the Ops tab has to diff two
    snapshots itself."""
    if elapsed_s <= 0:
        return 0.0
    return max(0, curr_count - prev_count) / elapsed_s


def campaign_label(campaign_id: str | None, campaigns_by_id: dict[str, dict[str, Any]]) -> str:
    """Human-readable label for a candidate_set/scores/winner entry -
    e.g. "c_0012 (retail, ExampleCo)". Falls back to the bare id for
    house_ad/unknown ids rather than raising, since a debug panel
    shouldn't crash on an id it can't look up."""
    if campaign_id is None:
        return "(none)"
    row = campaigns_by_id.get(campaign_id)
    if row is None:
        return campaign_id
    return f"{campaign_id} ({row['category']}, {row['advertiser_name']})"
