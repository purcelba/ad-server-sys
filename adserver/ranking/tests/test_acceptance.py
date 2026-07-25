"""Phase 4 acceptance criteria, verified against real data (AC1-5) and
real infra (AC6). AC1-5 share one session-scoped backfill (the offline
Parquet partitions are the same regardless of which config trains against
them) so the whole file doesn't redo a 23-day backfill per test.
"""

from __future__ import annotations

import datetime as dt
import inspect
from pathlib import Path

import polars as pl
import pytest

from adserver.batch_features.materialize import create_table_if_not_exists
from adserver.batch_features.offline_store import query_as_of
from adserver.common.registry import load_registry
from adserver.feature_service.resolver import get_dynamo_table, get_redis_client, resolve_query
from adserver.ranking import model_registry
from adserver.ranking import promote as promote_module
from adserver.ranking import scorer as scorer_module
from adserver.ranking.assemble import from_offline_row, from_online_result
from adserver.ranking.model import PctrModel
from adserver.ranking.model_registry import promote
from adserver.ranking.scorer import Scorer
from adserver.ranking.train import (
    V1_CONFIG,
    V2_CONFIG,
    DEFAULT_REGISTRY_PATH as FEATURE_REGISTRY_PATH,
    _build_estimator,
    _build_rows,
    _label_impressions,
    _matrix,
    _split_train_holdout,
    backfill,
    train,
)


@pytest.fixture(scope="session")
def offline_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("phase4_offline")
    backfill(output_dir=d)
    return d


# ---------------------------------------------------------------------------
# AC1: reproducibility
# ---------------------------------------------------------------------------


def test_ac1_make_train_is_reproducible_for_the_same_seed(tmp_path_factory, offline_dir):
    models_dir_a = tmp_path_factory.mktemp("models_ac1_a")
    models_dir_b = tmp_path_factory.mktemp("models_ac1_b")

    result_a = train(V1_CONFIG, output_dir=offline_dir, models_dir=models_dir_a)
    result_b = train(V1_CONFIG, output_dir=offline_dir, models_dir=models_dir_b)

    assert result_a["eval_report"] == result_b["eval_report"]


# ---------------------------------------------------------------------------
# AC2: eval report beats a popularity baseline; planted-signal check
# ---------------------------------------------------------------------------


def test_ac2_auc_beats_popularity_only_baseline(tmp_path_factory, offline_dir):
    models_dir = tmp_path_factory.mktemp("models_ac2")
    result = train(V1_CONFIG, output_dir=offline_dir, models_dir=models_dir)
    report = result["eval_report"]
    assert report["auc"] > report["baseline_auc"], (
        f"model AUC {report['auc']} should beat the ad's-own-ad_ctr_30d baseline {report['baseline_auc']}"
    )


def test_ac2_planted_signal_higher_cross_feature_predicts_higher_score(tmp_path_factory, offline_dir):
    """The planted segment x category lift (datagen/lifts.py) should show
    up through x_user_ctr_in_ad_category: holdout impressions where this
    user has a high historical CTR in this ad's specific category should
    get a higher predicted pCTR, on average, than ones where it's low."""
    models_dir = tmp_path_factory.mktemp("models_ac2_signal")
    result = train(V1_CONFIG, output_dir=offline_dir, models_dir=models_dir)
    model = result["model"]

    impressions = _label_impressions(Path("data"))
    rows = _build_rows(impressions, offline_dir)
    _, holdout_rows = _split_train_holdout(rows)

    scored = sorted(
        ((row["features"]["x_user_ctr_in_ad_category"], model.predict(row["features"])) for row in holdout_rows),
        key=lambda t: t[0],
    )
    third = len(scored) // 3
    low_third, high_third = scored[:third], scored[-third:]
    avg_low = sum(score for _, score in low_third) / len(low_third)
    avg_high = sum(score for _, score in high_third) / len(high_third)

    assert avg_high > avg_low, (
        f"holdout rows with high x_user_ctr_in_ad_category (avg score {avg_high}) should score higher "
        f"than rows with low x_user_ctr_in_ad_category (avg score {avg_low}) — planted signal not detected"
    )


# ---------------------------------------------------------------------------
# AC3: leakage test — documents the detection method, then proves
# production feature lists exclude the leaked signal.
# ---------------------------------------------------------------------------


def test_ac3_a_leaked_feature_produces_suspiciously_high_auc(offline_dir):
    from sklearn.metrics import roc_auc_score

    impressions = _label_impressions(Path("data"))
    rows = _build_rows(impressions, offline_dir)
    for row in rows:
        # obviously leaked: literally the answer, standing in for any
        # post-click-only signal (e.g. "time since click") that wouldn't
        # exist yet at serving time
        row["features"]["leaked_label"] = float(row["label"])

    train_rows, holdout_rows = _split_train_holdout(rows)
    leaked_config = {**V1_CONFIG, "feature_names": [*V1_CONFIG["feature_names"], "leaked_label"]}

    X_train, y_train = _matrix(train_rows, leaked_config["feature_names"])
    X_holdout, y_holdout = _matrix(holdout_rows, leaked_config["feature_names"])

    estimator = _build_estimator(leaked_config)
    estimator.fit(X_train, y_train)
    leaked_model = PctrModel(estimator, leaked_config["feature_names"])

    y_pred = [leaked_model.predict(row["features"]) for row in holdout_rows]
    leaked_auc = roc_auc_score(y_holdout, y_pred)

    assert leaked_auc > 0.99, f"a feature that literally is the label should produce a near-perfect AUC, got {leaked_auc}"


def test_ac3_production_feature_lists_exclude_the_leaked_feature():
    assert "leaked_label" not in V1_CONFIG["feature_names"]
    assert "leaked_label" not in V2_CONFIG["feature_names"]


# ---------------------------------------------------------------------------
# AC4: promote v2, roll back to v1 — scorer follows the live pointer
# with zero code changes.
# ---------------------------------------------------------------------------


def test_ac4_promote_and_rollback_scorer_follows_the_live_pointer(tmp_path_factory, offline_dir):
    models_dir = tmp_path_factory.mktemp("models_ac4")
    train(V1_CONFIG, output_dir=offline_dir, models_dir=models_dir)
    train(V2_CONFIG, output_dir=offline_dir, models_dir=models_dir)
    registry_path = models_dir / "registry.json"

    promote("pctr", "v1", registry_path)
    scorer_v1 = Scorer(model_registry_path=registry_path)
    assert scorer_v1.feature_names() == V1_CONFIG["feature_names"]

    promote("pctr", "v2", registry_path)
    scorer_v2 = Scorer(model_registry_path=registry_path)
    assert scorer_v2.feature_names() == V2_CONFIG["feature_names"]
    assert scorer_v2.feature_names() != scorer_v1.feature_names()

    promote("pctr", "v1", registry_path)  # rollback: identical command, older version
    scorer_rolled_back = Scorer(model_registry_path=registry_path)
    assert scorer_rolled_back.feature_names() == V1_CONFIG["feature_names"]


# ---------------------------------------------------------------------------
# AC5: opacity proof
# ---------------------------------------------------------------------------


def test_ac5_two_algorithms_produce_the_identical_artifact_type(tmp_path_factory, offline_dir):
    models_dir = tmp_path_factory.mktemp("models_ac5")
    result_v1 = train(V1_CONFIG, output_dir=offline_dir, models_dir=models_dir)
    result_v2 = train(V2_CONFIG, output_dir=offline_dir, models_dir=models_dir)

    assert V1_CONFIG["algorithm"] != V2_CONFIG["algorithm"]
    assert V1_CONFIG["feature_names"] != V2_CONFIG["feature_names"]
    assert type(result_v1["model"]) is PctrModel
    assert type(result_v2["model"]) is PctrModel
    assert type(result_v1["model"]) is type(result_v2["model"])


def test_ac5_registry_promote_and_scorer_never_branch_on_algorithm():
    """Checks actual control flow (If/match conditions), not raw text —
    a docstring mentioning 'algorithm' in prose isn't an opacity
    violation, a conditional keyed on it would be."""
    import ast

    for module in (model_registry, promote_module, scorer_module):
        tree = ast.parse(inspect.getsource(module))
        for node in ast.walk(tree):
            if isinstance(node, ast.If):
                assert "algorithm" not in ast.dump(node.test).lower(), (
                    f"{module.__name__} branches on 'algorithm' — opacity violated"
                )


def test_ac5_scorer_imports_no_ml_library():
    """Same static AST check as tests/test_scorer.py, restated here as the
    AC5-facing assertion."""
    import ast

    tree = ast.parse(inspect.getsource(scorer_module))
    forbidden = {"sklearn", "xgboost", "lightgbm", "torch", "tensorflow"}
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])
    assert not (imported_roots & forbidden)


# ---------------------------------------------------------------------------
# AC6: cross-parity — online (feature_service.resolver, real infra) vs.
# offline (offline_store.query_as_of) must compute the identical cross
# feature for the same underlying (user, ad) pair.
# ---------------------------------------------------------------------------

def _infra_reachable() -> bool:
    try:
        get_redis_client().ping()
        create_table_if_not_exists()
        return True
    except Exception:
        return False


requires_infra = pytest.mark.skipif(
    not _infra_reachable(), reason="redis/dynamodb-local not reachable — run `make up` + `make features`"
)


@requires_infra
def test_ac6_cross_feature_matches_between_online_and_offline_paths():
    feature_registry = load_registry(FEATURE_REGISTRY_PATH)
    redis_client = get_redis_client()
    dynamo_table = get_dynamo_table()
    today = dt.date.today()

    offline_users = query_as_of("user", today).sample(n=10, seed=42).to_dicts()
    campaigns = pl.read_parquet("data/campaigns.parquet").to_dicts()
    offline_ads = query_as_of("ad", today)

    compared = 0
    for i, user_row in enumerate(offline_users):
        campaign = campaigns[i % len(campaigns)]
        campaign_id = campaign["campaign_id"]
        ad_category = campaign["category"]

        online_user_result = resolve_query(
            "user", user_row["user_id"], ["user_ctr_by_category_30d"], feature_registry, redis_client, dynamo_table
        )
        online_ad_result = resolve_query(
            "ad", campaign_id, ["ad_ctr_7d"], feature_registry, redis_client, dynamo_table
        )
        online_cross = from_online_result(online_user_result, online_ad_result, ad_category)[
            "x_user_ctr_in_ad_category"
        ]

        offline_ad_row = offline_ads.filter(pl.col("campaign_id") == campaign_id).to_dicts()[0]
        offline_cross = from_offline_row(user_row, offline_ad_row, ad_category)["x_user_ctr_in_ad_category"]

        assert online_cross == pytest.approx(offline_cross), (
            f"{user_row['user_id']}/{campaign_id}: online={online_cross} offline={offline_cross}"
        )
        compared += 1

    assert compared == 10
