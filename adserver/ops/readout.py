"""Experiment readout: per-arm simulated CTR + a confidence interval,
read straight from the decision log.

**Why observational arm comparison is trustworthy here.** Every request's
arm is assigned by `experiment.assign_arm()` — a deterministic hash of
`(salt, user_id)` (Phase 5) — independent of anything about the request,
the candidate set, or the user's behavior. That's what makes a simple
per-arm CTR comparison valid: randomization (not a matched cohort, not a
regression adjustment) is what lets the difference in outcomes be
attributed to the difference in model version. What would break this
trust: assignment drift (the salt or the arm-split ratio changing
mid-experiment, so the two arms stop being comparable populations),
logging loss that's correlated with arm (e.g. one version's slower
scoring path timing out and dropping decisions more often), or a change
to the serving pipeline that isn't itself gated by arm. None of those
apply to this project's fixed-salt, single-pipeline setup, but they're
exactly what a real experimentation platform's automated checks (sample
ratio mismatch, pre-period A/A tests) exist to catch.

**Where this stops working: incrementality for brand campaigns.** A CTR
lift tells you which ranking produces more clicks among people who were
already going to see *an* ad — it says nothing about whether the ad
caused any of those clicks to happen at all, which is the actual question
for a brand campaign optimizing for incremental awareness/conversion
rather than direct response. Answering that needs a holdout (a group who
would have been eligible but never sees the campaign at all) compared
against exposed users — a different, harder randomization than "which
model scored you" - explicitly out of this project's build scope.

**Impression population.** Only decisions whose winner is a real,
scored campaign count as an impression here — matching
`ranking/retrain.py`'s definition. A no-fill decision (`winner is None`)
never showed anything; a house-ad decision is a fallback-rung impression
that was never actually eligible to be won by either model version, so
including it would dilute both arms' CTR without saying anything about
the models being compared.
"""

from __future__ import annotations

import argparse
import math
import random
from pathlib import Path
from typing import Any

import polars as pl

from adserver.adserver.decision_log import DEFAULT_LOG_PATH, read_decisions
from adserver.ops.outcomes import simulate_click

DEFAULT_DATA_DIR = Path("data")
Z_95 = 1.959963985  # two-sided 95% normal critical value


def wilson_interval(clicks: int, impressions: int, z: float = Z_95) -> tuple[float, float]:
    """Wilson score interval for a binomial proportion — well-behaved
    (never goes negative or above 1, unlike the plain Wald interval) even
    at the small-n, low-CTR end this project's simulated traffic sits in."""
    if impressions == 0:
        return (0.0, 0.0)
    p = clicks / impressions
    denom = 1 + z**2 / impressions
    center = p + z**2 / (2 * impressions)
    margin = z * math.sqrt(p * (1 - p) / impressions + z**2 / (4 * impressions**2))
    return ((center - margin) / denom, (center + margin) / denom)


def per_arm_ctr(
    decisions: list[dict[str, Any]],
    users_by_id: dict[str, dict[str, Any]],
    campaigns_by_id: dict[str, dict[str, Any]],
    rng: random.Random,
) -> dict[str, dict[str, Any]]:
    """One entry per `experiment_arm` seen: impressions, clicks, ctr, and
    the Wilson 95% CI on that ctr."""
    counts: dict[str, dict[str, int]] = {}
    for decision in decisions:
        if decision["winner"] is None or decision["winner"] == "house_ad":
            continue
        arm = decision["experiment_arm"]
        counts.setdefault(arm, {"impressions": 0, "clicks": 0})
        counts[arm]["impressions"] += 1
        if simulate_click(decision, users_by_id, campaigns_by_id, rng):
            counts[arm]["clicks"] += 1

    report: dict[str, dict[str, Any]] = {}
    for arm, c in counts.items():
        ctr = c["clicks"] / c["impressions"] if c["impressions"] else 0.0
        lo, hi = wilson_interval(c["clicks"], c["impressions"])
        report[arm] = {
            "impressions": c["impressions"],
            "clicks": c["clicks"],
            "ctr": ctr,
            "ci_95": (lo, hi),
        }
    return report


def cis_overlap(report: dict[str, dict[str, Any]], arm_a: str, arm_b: str) -> bool:
    """Non-overlapping 95% CIs on the two arms' CTR - a simple, visible
    significance check (not a formal two-proportion z-test, but the same
    "does the data support a difference" question), used both by the
    printed report and directly by AC3's planted-difference test."""
    lo_a, hi_a = report[arm_a]["ci_95"]
    lo_b, hi_b = report[arm_b]["ci_95"]
    return lo_a <= hi_b and lo_b <= hi_a


def _print_report(report: dict[str, dict[str, Any]]) -> None:
    if not report:
        print("No arm-attributed impressions found in the decision log.")
        return
    header = f"{'arm':<12} {'impressions':>12} {'clicks':>8} {'ctr':>8} {'95% ci':>18}"
    print(header)
    print("-" * len(header))
    for arm in sorted(report):
        r = report[arm]
        ci = f"[{r['ci_95'][0]:.4f}, {r['ci_95'][1]:.4f}]"
        print(f"{arm:<12} {r['impressions']:>12d} {r['clicks']:>8d} {r['ctr']:>8.4f} {ci:>18}")

    arms = sorted(report)
    if len(arms) == 2:
        a, b = arms
        verdict = (
            "CIs overlap - no significant difference detected"
            if cis_overlap(report, a, b)
            else "CIs do not overlap - significant difference detected"
        )
        print(f"\n{a} vs {b}: {verdict}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--decision-log", type=Path, default=DEFAULT_LOG_PATH)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    users_by_id = {r["user_id"]: r for r in pl.read_parquet(args.data_dir / "users.parquet").to_dicts()}
    campaigns_by_id = {
        r["campaign_id"]: r for r in pl.read_parquet(args.data_dir / "campaigns.parquet").to_dicts()
    }
    decisions = read_decisions(args.decision_log)
    report = per_arm_ctr(decisions, users_by_id, campaigns_by_id, random.Random(args.seed))
    _print_report(report)


if __name__ == "__main__":
    main()
