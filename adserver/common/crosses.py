"""Cross features: pure functions of user + ad + context features already
resolved elsewhere. Never stored, never in the online store — computed
fresh every time from whatever fed them, which is the whole point: this
module is imported by BOTH `ranking/train.py` (fed point-in-time offline
values) and the serving path (fed live `feature_service` values), so a
single implementation is the defense against training-serving skew.

Every function here takes plain values (already normalized dicts/scalars),
never a FeatureResult, a registry object, or anything store-specific —
`ranking/assemble.py`'s two adapters are what bridge each source's actual
shape down to what these functions expect.
"""

from __future__ import annotations

from typing import Any


def x_user_ctr_in_ad_category(user_features: dict[str, Any], ad_category: str) -> float:
    """This user's click-through rate specifically within the ad's own
    category, pulled out of `user_ctr_by_category_30d` (a
    `map[str,float]`) — the personalization signal a flat, ungrouped
    `user_ctr_30d` can't express. Defaults to 0.0 when the map is empty/
    missing or has no entry for this category (no signal yet, not a
    negative one)."""
    by_category = user_features.get("user_ctr_by_category_30d") or {}
    return float(by_category.get(ad_category, 0.0))


# Every cross function this module defines, keyed by its registry-style
# name — ranking/scorer.py checks a model's `x_`-prefixed pinned feature
# names against this, the same enforcement role common/registry.py plays
# for user/ad features.
CROSS_FUNCTIONS = {
    "x_user_ctr_in_ad_category": x_user_ctr_in_ad_category,
}
