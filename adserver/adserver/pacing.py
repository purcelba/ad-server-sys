"""Pacing: best-effort (no transactions), Redis-backed remaining-capacity
counters for both demand types, plus the linear pacing schedule that
decides whether a guaranteed campaign wins its slot outright or lets
auction demand (internal or external) compete.

**Best-effort is a locked project decision, not a bug.** Counters are a
plain `GET` then `SET`, never `DECRBY` inside a transaction or Lua script.
Two concurrent requests can both read "1 remaining," both decide they're
eligible, and both decrement — overshooting the budget/goal by one unit.
`tests/test_pacing_overshoot.py` demonstrates this deliberately (AC4); the
README explains what a production system does about it (a Lua script or
`DECRBY`/`INCRBY` at minimum, ideally a reservation with a rollback path)
— this project intentionally doesn't build that, to make the tradeoff
visible rather than silently correct.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

import redis

from adserver.adserver.scoring import ScoredCandidate

_BUDGET_KEY = "pacing:budget_remaining:{campaign_id}"
_GOAL_KEY = "pacing:goal_remaining:{campaign_id}"
_DELIVERED_KEY = "pacing:delivered:{campaign_id}"


def _capacity_key(campaign_row: dict[str, Any]) -> str:
    if campaign_row["demand_type"] == "guaranteed":
        return _GOAL_KEY.format(campaign_id=campaign_row["campaign_id"])
    return _BUDGET_KEY.format(campaign_id=campaign_row["campaign_id"])


def _initial_capacity(campaign_row: dict[str, Any]) -> float:
    if campaign_row["demand_type"] == "guaranteed":
        return float(campaign_row["impression_goal"])
    return float(campaign_row["budget"])


def get_remaining_capacity(redis_client: redis.Redis, campaign_row: dict[str, Any]) -> float:
    """Best-effort read: `GET`, initializing from the campaign's catalog
    value (budget or impression_goal) on first touch. The init-check
    itself isn't atomic, but a benign race there only affects the very
    first read for a given campaign — not the overshoot behavior AC4 is
    actually about."""
    key = _capacity_key(campaign_row)
    raw = redis_client.get(key)
    if raw is None:
        initial = _initial_capacity(campaign_row)
        redis_client.set(key, initial)
        return initial
    return float(raw)


def decrement_capacity(redis_client: redis.Redis, campaign_row: dict[str, Any], amount: float = 1.0) -> None:
    """The deliberate race: `GET` current value, subtract, `SET` — not an
    atomic decrement. Two concurrent calls can both read the same
    almost-exhausted value and both subtract from it, overshooting by one
    unit combined. See the module docstring."""
    current = get_remaining_capacity(redis_client, campaign_row)
    redis_client.set(_capacity_key(campaign_row), current - amount)


def record_delivery(redis_client: redis.Redis, campaign_row: dict[str, Any]) -> int:
    """Lifetime delivered-impression count (guaranteed campaigns only need
    this, to compare against the pacing schedule) — same best-effort
    GET+SET pattern as capacity."""
    key = _DELIVERED_KEY.format(campaign_id=campaign_row["campaign_id"])
    current = int(redis_client.get(key) or 0) + 1
    redis_client.set(key, current)
    return current


def elapsed_flight_fraction(now: dt.date, flight_start: dt.date, flight_end: dt.date) -> float:
    total_days = (flight_end - flight_start).days
    if total_days <= 0:
        return 1.0
    elapsed_days = (now - flight_start).days
    return max(0.0, min(1.0, elapsed_days / total_days))


def is_behind_schedule(campaign_row: dict[str, Any], now: dt.date, delivered: int) -> bool:
    """Simple linear pacing: expected_by_now = impression_goal *
    elapsed_flight_fraction(now). Behind schedule if actual delivery is
    under that."""
    fraction = elapsed_flight_fraction(now, campaign_row["flight_start"], campaign_row["flight_end"])
    expected_by_now = campaign_row["impression_goal"] * fraction
    return delivered < expected_by_now


@dataclass(frozen=True)
class ArbitrationResult:
    winner_campaign_id: str | None
    rung: str  # "guaranteed" | "auction" | "external" | "none"
    price: float | None


def arbitrate(
    guaranteed_candidates: list[dict[str, Any]],
    scored_auction: list[ScoredCandidate],
    external_bid: float | None,
    now: dt.date,
    delivered_by_campaign: dict[str, int],
) -> ArbitrationResult:
    """Behind-schedule guaranteed campaigns win the slot outright — no
    auction needed. Otherwise the slot goes to whichever of (best internal
    eCPM, external bid) is higher; internal demand only if the external
    bidder timed out or failed (external_bid=None)."""
    behind = [
        c
        for c in guaranteed_candidates
        if is_behind_schedule(c, now, delivered_by_campaign.get(c["campaign_id"], 0))
    ]
    if behind:
        def urgency(c: dict[str, Any]) -> float:
            fraction = elapsed_flight_fraction(now, c["flight_start"], c["flight_end"])
            expected = c["impression_goal"] * fraction
            delivered = delivered_by_campaign.get(c["campaign_id"], 0)
            return (delivered / expected) if expected > 0 else 0.0

        winner = min(behind, key=urgency)
        return ArbitrationResult(winner_campaign_id=winner["campaign_id"], rung="guaranteed", price=None)

    best_internal = max(scored_auction, key=lambda c: c.ecpm or 0.0, default=None)
    best_internal_ecpm = best_internal.ecpm if best_internal else 0.0

    if external_bid is not None and external_bid > (best_internal_ecpm or 0.0):
        return ArbitrationResult(winner_campaign_id=None, rung="external", price=external_bid)

    if best_internal is not None:
        return ArbitrationResult(winner_campaign_id=best_internal.campaign_id, rung="auction", price=best_internal.ecpm)

    return ArbitrationResult(winner_campaign_id=None, rung="none", price=None)
