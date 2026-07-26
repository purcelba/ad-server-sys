import shutil

import httpx
import pytest

from adserver.adserver.features import FeatureFetchError, fetch_candidate_ad_features, fetch_user_features
from adserver.feature_service.resolver import get_redis_client

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _infra_reachable() -> bool:
    try:
        get_redis_client().ping()
        return True
    except Exception:
        return False


requires_infra = pytest.mark.skipif(not _infra_reachable(), reason="redis/dynamodb-local not reachable — run `make up`")


@requires_infra
def test_fetch_user_features_returns_every_requested_feature(running_feature_service):
    with httpx.Client() as client:
        # generous timeout: a freshly-started feature_service process has
        # no warmed DynamoDB connections yet (cold-start effect already
        # observed in Phase 3) - the production 20ms budget is exercised
        # deliberately, with a warm service, in the degradation-ladder test.
        features = fetch_user_features(client, "u_0001", timeout_s=2.0)
    from adserver.adserver.features import USER_FEATURE_NAMES

    assert set(features) == set(USER_FEATURE_NAMES)
    for value in features.values():
        assert value.freshness_status in {"fresh", "stale", "missing"}


@requires_infra
def test_fetch_candidate_ad_features_returns_one_entry_per_campaign(running_feature_service):
    with httpx.Client() as client:
        features = fetch_candidate_ad_features(client, ["c_0001", "c_0002"], timeout_s=2.0)
    assert set(features) == {"c_0001", "c_0002"}
    assert "ad_ctr_7d" in features["c_0001"]


@requires_infra
def test_fetch_candidate_ad_features_empty_list_returns_empty_dict(running_feature_service):
    with httpx.Client() as client:
        assert fetch_candidate_ad_features(client, []) == {}


@requires_infra
def test_fetch_user_features_raises_feature_fetch_error_when_unreachable():
    with httpx.Client() as client:
        with pytest.raises(FeatureFetchError):
            fetch_user_features(client, "u_0001", timeout_s=0.001)
