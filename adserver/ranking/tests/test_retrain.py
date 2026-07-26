"""Build-item tests for retrain.py's internals, against a synthetic
decision-log fixture - full end-to-end runs against a real decision log
live in tests/test_acceptance.py (AC2)."""

from __future__ import annotations

import json
import random

import polars as pl

from adserver.ranking.model_registry import load_registry as load_model_registry
from adserver.ranking.retrain import RETRAIN_CONFIG, _split_train_holdout, build_rows, retrain

USERS_BY_ID = {"u_0001": {"user_id": "u_0001", "segment": "foodie"}}
CAMPAIGNS_BY_ID = {"c_0001": {"campaign_id": "c_0001", "category": "food"}}


def _decision(winner="c_0001", scores=None, **overrides):
    base = {
        "winner": winner,
        "user_id": "u_0001",
        "ts": "2026-07-20 12:00:00",
        "scores": scores
        if scores is not None
        else [
            {
                "campaign_id": "c_0001",
                "pctr": 0.1,
                "ecpm": 0.5,
                "features": {name: 1.0 for name in RETRAIN_CONFIG["feature_names"]},
            }
        ],
    }
    base.update(overrides)
    return base


def test_build_rows_skips_no_winner_and_house_ad():
    decisions = [_decision(winner=None), _decision(winner="house_ad", scores=[])]
    rows = build_rows(decisions, USERS_BY_ID, CAMPAIGNS_BY_ID, RETRAIN_CONFIG["feature_names"], random.Random(0))
    assert rows == []


def test_build_rows_skips_winner_absent_from_scores():
    # A guaranteed campaign winning outright is never run through
    # score_candidates - no features logged for it, so it can't become a
    # training row.
    decisions = [_decision(winner="c_0002", scores=[])]
    rows = build_rows(decisions, USERS_BY_ID, CAMPAIGNS_BY_ID, RETRAIN_CONFIG["feature_names"], random.Random(0))
    assert rows == []


def test_build_rows_casts_user_session_active_to_int():
    decisions = [
        _decision(
            scores=[
                {
                    "campaign_id": "c_0001",
                    "pctr": 0.1,
                    "ecpm": 0.5,
                    "features": {**{n: 1.0 for n in RETRAIN_CONFIG["feature_names"]}, "user_session_active": True},
                }
            ]
        )
    ]
    rows = build_rows(decisions, USERS_BY_ID, CAMPAIGNS_BY_ID, RETRAIN_CONFIG["feature_names"], random.Random(0))
    assert rows[0]["features"]["user_session_active"] == 1
    assert isinstance(rows[0]["features"]["user_session_active"], int)


def test_build_rows_skips_pre_amendment_log_lines_missing_features_key():
    decisions = [
        _decision(scores=[{"campaign_id": "c_0001", "pctr": 0.1, "ecpm": 0.5}])  # no "features" key at all
    ]
    rows = build_rows(decisions, USERS_BY_ID, CAMPAIGNS_BY_ID, RETRAIN_CONFIG["feature_names"], random.Random(0))
    assert rows == []


def test_split_train_holdout_is_deterministic_for_a_fixed_seed():
    rows = [{"features": {}, "label": i % 2} for i in range(20)]
    train_a, holdout_a = _split_train_holdout(rows, 0.2, random.Random(42))
    train_b, holdout_b = _split_train_holdout(rows, 0.2, random.Random(42))
    assert train_a == train_b
    assert holdout_a == holdout_b
    assert len(holdout_a) == 4
    assert len(train_a) == 16


def test_retrain_writes_the_full_artifact_directory_and_registers_as_candidate(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    pl.DataFrame([{"user_id": "u_0001", "segment": "foodie", "home_metro": "sf", "created_at": None}]).write_parquet(
        data_dir / "users.parquet"
    )
    pl.DataFrame(
        [
            {
                "campaign_id": "c_0001",
                "category": "food",
                "advertiser_name": "a",
                "demand_type": "auction",
                "bid": 1.0,
                "budget": 100.0,
                "impression_goal": None,
                "flight_start": None,
                "flight_end": None,
                "status": "active",
            }
        ]
    ).write_parquet(data_dir / "campaigns.parquet")

    decision_log_path = tmp_path / "decision_log.jsonl"
    features = {name: 1.0 for name in RETRAIN_CONFIG["feature_names"]}
    with decision_log_path.open("w") as f:
        for i in range(60):
            entry = _decision(
                scores=[{"campaign_id": "c_0001", "pctr": 0.1, "ecpm": 0.5, "features": features}]
            )
            entry["ts"] = f"2026-07-20 {i % 24:02d}:00:00"
            f.write(json.dumps(entry) + "\n")

    models_dir = tmp_path / "models"
    result = retrain(data_dir=data_dir, decision_log_path=decision_log_path, models_dir=models_dir)

    version_dir = models_dir / "pctr" / "v3"
    assert version_dir == result["version_dir"]
    assert (version_dir / "model.pkl").exists()
    assert json.loads((version_dir / "feature_names.json").read_text()) == RETRAIN_CONFIG["feature_names"]
    assert (version_dir / "training_config.json").exists()
    assert (version_dir / "eval_report.json").exists()

    registry = load_model_registry(models_dir / "registry.json")
    assert registry["pctr"]["v3"]["status"] == "candidate"
    assert registry["pctr"]["v3"]["path"] == str(version_dir)
