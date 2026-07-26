"""Reconciliation: compares what pacing's Redis counters *think* each
campaign delivered against what the decision log says actually happened,
per campaign. A non-zero discrepancy is expected, not a bug in this
report — `pacing.decrement_capacity()` is a deliberate best-effort
GET-then-SET (see `adserver/adserver/pacing.py`), and Phase 5's AC4
demonstrated the exact concurrent-request race that causes overshoot.
This is the "accept and reconcile" leg of that documented tradeoff: a
production system fixes the counter (atomic decrement, or a
reservation+rollback); this project instead makes the drift visible.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
import redis
import typer

from adserver.adserver import pacing
from adserver.adserver.decision_log import DEFAULT_LOG_PATH, read_decisions

DEFAULT_DATA_DIR = Path("data")

app = typer.Typer(add_completion=False)


def reconcile(
    campaigns: pl.DataFrame,
    decisions: list[dict[str, Any]],
    redis_client: redis.Redis,
) -> list[dict[str, Any]]:
    """Returns one row per active-or-referenced campaign: initial
    capacity, remaining capacity per Redis, expected_consumed (derived
    from the two), actual_served (a plain count of decision-log wins),
    and the discrepancy between them."""
    served_counts: dict[str, int] = {}
    for decision in decisions:
        winner = decision["winner"]
        if winner is not None:
            served_counts[winner] = served_counts.get(winner, 0) + 1

    rows: list[dict[str, Any]] = []
    for row in campaigns.to_dicts():
        campaign_id = row["campaign_id"]
        actual_served = served_counts.get(campaign_id, 0)
        if actual_served == 0 and redis_client.get(_capacity_key_if_touched(row, redis_client)) is None:
            # Never touched by pacing and never won a request - not
            # interesting to reconcile (most of the catalog, most runs).
            continue

        initial = row["impression_goal"] if row["demand_type"] == "guaranteed" else row["budget"]
        remaining = pacing.get_remaining_capacity(redis_client, row)
        expected_consumed = initial - remaining
        rows.append(
            {
                "campaign_id": campaign_id,
                "demand_type": row["demand_type"],
                "initial_capacity": initial,
                "remaining_capacity": remaining,
                "expected_consumed": expected_consumed,
                "actual_served": actual_served,
                "discrepancy": expected_consumed - actual_served,
            }
        )
    return rows


def _capacity_key_if_touched(campaign_row: dict[str, Any], redis_client: redis.Redis) -> str:
    """`get_remaining_capacity` initializes the key on first read (a
    side effect we don't want just to check "was this campaign ever
    touched by pacing"), so this mirrors its key-naming without calling
    it."""
    if campaign_row["demand_type"] == "guaranteed":
        return f"pacing:goal_remaining:{campaign_row['campaign_id']}"
    return f"pacing:budget_remaining:{campaign_row['campaign_id']}"


def _print_report(rows: list[dict[str, Any]]) -> None:
    if not rows:
        typer.echo("No campaigns with pacing activity found.")
        return
    header = f"{'campaign_id':<12} {'type':<11} {'initial':>9} {'remaining':>10} {'expected':>9} {'actual':>7} {'discrepancy':>12}"
    typer.echo(header)
    typer.echo("-" * len(header))
    for r in sorted(rows, key=lambda r: abs(r["discrepancy"]), reverse=True):
        typer.echo(
            f"{r['campaign_id']:<12} {r['demand_type']:<11} {r['initial_capacity']:>9.1f} "
            f"{r['remaining_capacity']:>10.1f} {r['expected_consumed']:>9.1f} {r['actual_served']:>7d} "
            f"{r['discrepancy']:>+12.1f}"
        )
    total_discrepancy = sum(r["discrepancy"] for r in rows)
    typer.echo("-" * len(header))
    typer.echo(f"{len(rows)} campaign(s) reconciled, total discrepancy: {total_discrepancy:+.1f}")


@app.command()
def main(
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR, help="Directory containing campaigns.parquet"),
    decision_log_path: Path = typer.Option(DEFAULT_LOG_PATH, help="Path to decision_log.jsonl"),
    redis_host: str = typer.Option("localhost"),
    redis_port: int = typer.Option(6379),
) -> None:
    campaigns = pl.read_parquet(data_dir / "campaigns.parquet")
    decisions = read_decisions(decision_log_path)
    redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    rows = reconcile(campaigns, decisions, redis_client)
    _print_report(rows)


if __name__ == "__main__":
    app()
