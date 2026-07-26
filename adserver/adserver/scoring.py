"""pCTR scoring: assembles each candidate's feature dict (reusing
`ranking.assemble` — the identical function Phase 4's training path and
its cross-parity test already use, so there's one implementation between
training and serving) and scores it with the live `Scorer`. eCPM = bid x
pCTR ranks auction candidates; guaranteed candidates have no bid (eCPM is
`None` for them — arbitration between guaranteed and auction demand is
`pacing.py`'s job, not this module's).

**Real-time destination boost, layered on top of the trained score.**
Neither v1 nor v2 uses `user_current_destination_category` as a pinned
model input — Phase 4 explicitly excluded every real-time feature, since
there's no historical log of past Redis state to train a coefficient
against (see `ranking/README.md`). AC6 still needs a real-time session
signal to visibly move scores toward matching ads within seconds of the
event, which a trained-but-never-updated model coefficient structurally
cannot do here. Rather than fabricate training data or wait for Phase 6's
decision-log-based retraining, this applies a small, explicit,
non-learned multiplier when the ad's category matches the user's *current*
destination — the same shape as a real ranking system's rule-based
"context boost" layered on top of a base ML score, not something claimed
to be a learned effect. Kept separate from the model and from
`common/crosses.py` (crosses are model *inputs*; this never is one).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any

from adserver.common.audiences import AudienceDef
from adserver.ranking.assemble import from_online_result
from adserver.ranking.scorer import Scorer

# Multiplicative, not additive, so it scales with the model's own
# confidence rather than swamping a low base score or being negligible
# against a high one. Clamped to a valid probability below.
DESTINATION_MATCH_BOOST = 1.5


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
    # user_features is the raw {name: FeatureValue} dict (the same shape
    # from_online_result() below expects) - unwrap .value explicitly here
    # rather than relying on from_online_result's own unwrapping, since
    # this check happens before assembly, per-candidate.
    destination_feature = user_features.get("user_current_destination_category")
    current_destination = destination_feature.value if destination_feature is not None else ""

    scored: list[ScoredCandidate] = []
    for candidate in candidates:
        campaign_id = candidate["campaign_id"]
        ad_result = ad_features_by_campaign.get(campaign_id, {})
        features = from_online_result(user_features, ad_result, candidate["category"], audiences=audiences)
        features["hour_of_day"] = now.hour

        pctr = scorer.score(features)
        if current_destination and current_destination == candidate["category"]:
            pctr = min(1.0, pctr * DESTINATION_MATCH_BOOST)

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
