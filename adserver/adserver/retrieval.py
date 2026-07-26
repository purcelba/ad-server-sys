"""Candidate retrieval: filter the campaign catalog down to what's
eligible for this request — cheap, no model. Deliberately the first
stage, before any feature fetch or scoring: it only touches campaign
catalog attributes (status, flight, targeting) and pacing state (budget/
goal remaining), never per-candidate feature values.

Audience eligibility needs the requesting user's `audience_memberships`,
which is otherwise a `feature_service`-served feature — resolved by
fetching *user* features once, early (before retrieval), since Phase 4's
feature-class contract already treats user features as "fetched once per
request" anyway. Retrieval-surviving candidates' *ad* features are fetched
afterward, per candidate — see `features.py`.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Callable

import polars as pl

from adserver.common.audiences import AudienceDef


@dataclass(frozen=True)
class AudienceExclusion:
    campaign_id: str
    audience: str
    definition_version: int


@dataclass(frozen=True)
class RetrievalResult:
    eligible: list[dict[str, Any]]
    excluded_by_audience: list[AudienceExclusion]


def retrieve_candidates(
    campaigns: pl.DataFrame,
    now: dt.date,
    user_audience_memberships: list[str],
    audiences: dict[str, AudienceDef],
    remaining_capacity: Callable[[dict[str, Any]], float],
) -> RetrievalResult:
    """`remaining_capacity(campaign_row) -> float` is injected (not read
    from Redis directly here) so this stays testable without infra —
    `pacing.py` is what actually backs it in the real request path."""
    eligible: list[dict[str, Any]] = []
    excluded: list[AudienceExclusion] = []

    for row in campaigns.to_dicts():
        if row["status"] != "active":
            continue
        if not (row["flight_start"] <= now <= row["flight_end"]):
            continue
        if remaining_capacity(row) <= 0:
            continue

        targeted = row["targeted_audiences"] or []
        if targeted:
            # AUDIENCE ROUTING RULE (phases.md's Phase 5 spec): audiences
            # gate ELIGIBILITY only, and only when purchased. A campaign
            # that targeted an audience is ineligible for users outside
            # it; a campaign that targeted nothing is completely
            # unaffected by audience membership either way. This must
            # never become a system-imposed RELEVANCE filter (cliff
            # edges, self-reinforcing data starvation, thinner auctions
            # for non-members) — relevance flows through scoring's
            # audience *affinity* cross feature instead, never through
            # blocking eligibility.
            matched = [a for a in targeted if a in user_audience_memberships]
            if not matched:
                for audience_name in targeted:
                    audience = audiences.get(audience_name)
                    excluded.append(
                        AudienceExclusion(
                            campaign_id=row["campaign_id"],
                            audience=audience_name,
                            definition_version=audience.definition_version if audience else -1,
                        )
                    )
                continue

        eligible.append(row)

    return RetrievalResult(eligible=eligible, excluded_by_audience=excluded)
