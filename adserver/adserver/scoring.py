"""pCTR scoring: assembles each candidate's feature dict (reusing
`ranking.assemble` — the identical function Phase 4's training path and
its cross-parity test already use, so there's one implementation between
training and serving) and scores it with the live `Scorer`. eCPM = bid x
pCTR ranks auction candidates; guaranteed candidates have no bid (eCPM is
`None` for them — arbitration between guaranteed and auction demand is
`pacing.py`'s job, not this module's).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from adserver.common.audiences import AudienceDef
from adserver.ranking.assemble import from_online_result
from adserver.ranking.scorer import Scorer


@dataclass(frozen=True)
class ScoredCandidate:
    campaign_id: str
    demand_type: str
    pctr: float
    ecpm: float | None
    features: dict[str, Any]


def score_candidates(
    scorer: Scorer,
    user_features: dict[str, Any],
    candidates: list[dict[str, Any]],
    ad_features_by_campaign: dict[str, dict[str, Any]],
    audiences: dict[str, AudienceDef],
    now: dt.datetime,
) -> list[ScoredCandidate]:
    scored: list[ScoredCandidate] = []
    for candidate in candidates:
        campaign_id = candidate["campaign_id"]
        ad_result = ad_features_by_campaign.get(campaign_id, {})
        features = from_online_result(user_features, ad_result, candidate["category"], audiences=audiences)
        features["hour_of_day"] = now.hour

        pctr = scorer.score(features)
        bid = candidate.get("bid")
        ecpm = bid * pctr if bid is not None else None

        scored.append(
            ScoredCandidate(
                campaign_id=campaign_id,
                demand_type=candidate["demand_type"],
                pctr=pctr,
                ecpm=ecpm,
                features=features,
            )
        )
    return scored
