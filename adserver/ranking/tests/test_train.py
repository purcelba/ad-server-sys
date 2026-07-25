"""Build-item tests for train.py's internals — full end-to-end training
runs (backfill + fit + write artifact) live in tests/test_acceptance.py's
AC-specific tests, which reuse a single session-scoped backfill/train pass
rather than repeating it per test."""

from __future__ import annotations

import json

from adserver.ranking.model_registry import load_registry as load_model_registry
from adserver.ranking.train import V1_CONFIG, _fill_registry_defaults, backfill, train


def test_fill_registry_defaults_substitutes_none_for_a_known_feature():
    row = {"user_id": "u_1", "user_ctr_30d": None, "user_impressions_7d": 5}
    filled = _fill_registry_defaults(row)
    assert filled["user_ctr_30d"] == 0.0  # the registry default for user_ctr_30d
    assert filled["user_impressions_7d"] == 5


def test_fill_registry_defaults_leaves_non_registry_keys_untouched():
    row = {"user_id": "u_1", "asof": None}
    filled = _fill_registry_defaults(row)
    assert filled["asof"] is None  # "asof" isn't a registered feature - not this function's job


def test_backfill_is_idempotent(tmp_path):
    output_dir = tmp_path / "features"
    backfill(output_dir=output_dir)
    mtime_first = (output_dir / "entity=user").stat().st_mtime

    backfill(output_dir=output_dir)  # second call should skip every already-backfilled day
    mtime_second = (output_dir / "entity=user").stat().st_mtime

    assert mtime_first == mtime_second


def test_train_writes_the_full_artifact_directory_and_registers_as_candidate(tmp_path):
    output_dir = tmp_path / "features"
    models_dir = tmp_path / "models"

    result = train(V1_CONFIG, output_dir=output_dir, models_dir=models_dir)

    version_dir = models_dir / "pctr" / "v1"
    assert version_dir == result["version_dir"]
    assert (version_dir / "model.pkl").exists()
    assert json.loads((version_dir / "feature_names.json").read_text()) == V1_CONFIG["feature_names"]
    assert (version_dir / "training_config.json").exists()
    assert (version_dir / "eval_report.json").exists()

    registry = load_model_registry(models_dir / "registry.json")
    assert registry["pctr"]["v1"]["status"] == "candidate"
    assert registry["pctr"]["v1"]["path"] == str(version_dir)
