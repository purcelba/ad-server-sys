"""Simulated click outcomes for decision-log rows.

No click-tracking exists anywhere in this project — `/serve` only logs a
served impression, never a subsequent click. Both retraining
(`ranking/retrain.py`) and the experiment readout (`ops/readout.py`) need
a label/CTR signal, so both retroactively simulate one using
`datagen.lifts.click_probability` — the exact generative model that
produced Phase 0's original synthetic `events.parquet` history. This is
documented, simulated feedback standing in for real user behavior, not a
new signal; reusing one shared function here means the two consumers can
never silently diverge on what counts as a "click".
"""

from __future__ import annotations

import datetime as dt
import random
from typing import Any

from adserver.datagen.lifts import click_probability


def _hour_of_day(ts: Any) -> int:
    """`ts` is a real `datetime` when a decision comes straight from
    `service.py`, but a `"YYYY-MM-DD HH:MM:SS[.ffffff]"` string once
    round-tripped through `decision_log.jsonl` (json.dumps(default=str))
    — both are real call sites, so handle both."""
    if isinstance(ts, dt.datetime):
        return ts.hour
    return dt.datetime.fromisoformat(ts).hour


def simulate_click(
    decision: dict[str, Any],
    users_by_id: dict[str, dict[str, Any]],
    campaigns_by_id: dict[str, dict[str, Any]],
    rng: random.Random,
) -> bool:
    """Simulates whether the winning impression in `decision` was
    clicked. A decision with no winner (`winner is None`) or whose winner
    is the house ad was never really "shown" to a catalog slot in the
    modeled sense, so it's never a click."""
    winner = decision["winner"]
    if winner is None or winner == "house_ad":
        return False

    campaign = campaigns_by_id.get(winner)
    user = users_by_id.get(decision["user_id"])
    if campaign is None or user is None:
        return False

    hour_of_day = _hour_of_day(decision["ts"])
    p = click_probability(user["segment"], campaign["category"], hour_of_day)
    return rng.random() < p
