"""Phase 1 acceptance criteria (phases.md).

AC1 only, per explicit instruction: `make features` computes and
materializes all registry features; re-running is idempotent.

Requires `make up` (dynamodb-local) — self-skips without it, matching
test_phase0_acceptance.py's pattern.
"""

import shutil

import pytest

from adserver.batch_features.materialize import TABLE_NAME, get_resource
from adserver.batch_features.runner import DEFAULT_REGISTRY_PATH, run
from adserver.common.registry import load_registry
from adserver.datagen.users import HISTORY_END

pytestmark = pytest.mark.skipif(shutil.which("docker") is None, reason="docker not available")


def _dynamo_reachable() -> bool:
    try:
        get_resource().meta.client.list_tables(Limit=1)
        return True
    except Exception:
        return False


requires_dynamo = pytest.mark.skipif(
    not _dynamo_reachable(), reason="dynamodb-local not reachable — run `make up`"
)


def _scan_all_items() -> list[dict]:
    resource = get_resource()
    table = resource.Table(TABLE_NAME)
    items: list[dict] = []
    resp = table.scan()
    items.extend(resp["Items"])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp["Items"])
    return items


@requires_dynamo
def test_ac1_make_features_computes_and_materializes_all_registry_features_idempotently():
    registry = load_registry(DEFAULT_REGISTRY_PATH)
    expected_feature_names = set(registry.keys())
    assert len(expected_feature_names) == 9  # sanity: registry hasn't silently shrunk

    # --- first run ---
    combined_1 = run(as_of=HISTORY_END, materialize_to_dynamo=True)
    assert combined_1["user"].height == 50
    assert combined_1["ad"].height == 40

    items_1 = _scan_all_items()
    assert len(items_1) > 0, "materialization wrote nothing"

    materialized_feature_names = {item["feature_name"] for item in items_1}
    assert materialized_feature_names == expected_feature_names, (
        f"missing from DynamoDB: {expected_feature_names - materialized_feature_names}, "
        f"unexpected: {materialized_feature_names - expected_feature_names}"
    )

    # every registry feature must be materialized for at least one entity
    counts_by_feature = {}
    for item in items_1:
        counts_by_feature[item["feature_name"]] = counts_by_feature.get(item["feature_name"], 0) + 1
    for name in expected_feature_names:
        assert counts_by_feature.get(name, 0) > 0, f"{name} was declared but never materialized"

    values_1 = {(item["entity_key"], item["feature_name"]): item["value"] for item in items_1}

    # --- second run: same as_of, no new data generated in between ---
    combined_2 = run(as_of=HISTORY_END, materialize_to_dynamo=True)
    assert combined_2["user"].height == combined_1["user"].height
    assert combined_2["ad"].height == combined_1["ad"].height

    items_2 = _scan_all_items()
    values_2 = {(item["entity_key"], item["feature_name"]): item["value"] for item in items_2}

    # idempotent: identical (entity, feature) keys and identical values.
    # computed_at is intentionally excluded from this comparison - it's
    # expected to advance on every run; the feature *values* must not.
    assert set(values_1.keys()) == set(values_2.keys())
    mismatches = {k: (values_1[k], values_2[k]) for k in values_1 if values_1[k] != values_2[k]}
    assert not mismatches, f"non-idempotent values on re-run: {mismatches}"
