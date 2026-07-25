"""Feature assembly: two adapters that normalize each source's actual
shape down to a plain `{name: value}` dict, then hand that to
`common/crosses.py`. The cross functions themselves never see a
FeatureResult or a raw offline_store row — only these adapters know the
difference between the two sources, which is exactly what
`tests/test_ac6_cross_parity.py` verifies: fed the same underlying data,
both adapters must produce the same assembled dict.
"""

from __future__ import annotations

from typing import Any

from adserver.common.crosses import x_user_ctr_in_ad_category

# Non-feature columns present on an offline_store.query_as_of() row —
# stripped out before assembly so only actual feature values remain.
_OFFLINE_METADATA_COLUMNS = {"user_id", "campaign_id", "asof", "entity"}


def _assemble(user_features: dict[str, Any], ad_features: dict[str, Any], ad_category: str) -> dict[str, Any]:
    merged = {**user_features, **ad_features}
    merged["x_user_ctr_in_ad_category"] = x_user_ctr_in_ad_category(user_features, ad_category)
    return merged


def from_offline_row(user_row: dict[str, Any], ad_row: dict[str, Any], ad_category: str) -> dict[str, Any]:
    """`user_row`/`ad_row` are plain dicts from an `offline_store.query_as_of()`
    polars row (e.g. via `.to_dicts()`) — used by `train.py`'s point-in-time
    join."""
    user_features = {k: v for k, v in user_row.items() if k not in _OFFLINE_METADATA_COLUMNS}
    ad_features = {k: v for k, v in ad_row.items() if k not in _OFFLINE_METADATA_COLUMNS}
    return _assemble(user_features, ad_features, ad_category)


def from_online_result(
    user_result: dict[str, Any], ad_result: dict[str, Any], ad_category: str
) -> dict[str, Any]:
    """`user_result`/`ad_result` are `{feature_name: FeatureResult}` dicts —
    exactly what `feature_service.resolver.resolve_query()` returns."""
    user_features = {name: result.value for name, result in user_result.items()}
    ad_features = {name: result.value for name, result in ad_result.items()}
    return _assemble(user_features, ad_features, ad_category)
