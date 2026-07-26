"""Retraining from a real decision log, not synthetic history.

`train.py` point-in-time joins Phase 0's synthetic impression history
against Phase 1's offline feature store — every real-time (Redis) feature
is structurally absent there, since there's no historical log of past
Redis state to join against a 20-day-old synthetic impression (see
`ranking/README.md`). A real decision log (Phase 5's `/serve`) doesn't
have that problem: each logged decision already carries the exact
feature dict, real-time features included, that produced its scores. So
this module skips the point-in-time join entirely and reads training
rows straight out of `decision_log.jsonl`.

**One row per decision where the winner is a real campaign.** A decision
with no winner, or whose winner is the house ad, isn't a real impression
to a catalog ad and contributes nothing (`winner is None` covers "no
demand won the slot" — a guaranteed campaign winning still logs a real
`winner`, but see the note on `_extract_row` below). Other candidates
that were scored but didn't win aren't real impressions either — this
matches how training data works in a real ad system: an impression is
something that was actually shown, not everything that was scored.

**No labels exist.** Clicks are simulated retroactively via
`ops.outcomes.simulate_click`, the same helper `ops/readout.py` uses, so
the two consumers can never diverge on what counts as a click.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

import polars as pl
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from adserver.adserver.decision_log import DEFAULT_LOG_PATH, read_decisions
from adserver.ops.outcomes import simulate_click
from adserver.ranking import model_registry
from adserver.ranking.model import PctrModel
from adserver.ranking.train import V1_CONFIG

LOGICAL_NAME = "pctr"
DEFAULT_DATA_DIR = Path("data")
DEFAULT_MODELS_DIR = Path("models")

# V1_CONFIG's numeric feature set plus user_session_active, cast to 0/1 -
# the concrete, minimal fulfillment of train.py's forward pointer: a
# real-time feature, trainable now because the decision log captures its
# actual per-request value. user_current_destination_category is
# categorical and would need encoding infrastructure this module doesn't
# otherwise need - left for a future config (see ranking/README.md).
RETRAIN_CONFIG: dict[str, Any] = {
    "version": "v3",
    "algorithm": "logistic_regression",
    "feature_names": [*V1_CONFIG["feature_names"], "user_session_active"],
    "hyperparams": {},
    "seed": 42,
}

HOLDOUT_FRACTION = 0.2


def _winner_features(decision: dict[str, Any]) -> dict[str, Any] | None:
    """The winning candidate's assembled feature dict, as logged by
    `service.py`'s `"scores"` entries. `None` if the winner was never
    scored - true for a guaranteed campaign winning outright (arbitrate()
    never runs it through `score_candidates`, see `service.py`) or for
    any decision predating the Phase 6 amendment that added `"features"`
    to logged scores."""
    for scored in decision.get("scores", []):
        if scored["campaign_id"] == decision["winner"]:
            return scored.get("features")
    return None


def build_rows(
    decisions: list[dict[str, Any]],
    users_by_id: dict[str, dict[str, Any]],
    campaigns_by_id: dict[str, dict[str, Any]],
    feature_names: list[str],
    rng: random.Random,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for decision in decisions:
        winner = decision["winner"]
        if winner is None or winner == "house_ad":
            continue
        raw_features = _winner_features(decision)
        if raw_features is None:
            continue
        if any(name not in raw_features for name in feature_names):
            continue  # e.g. user_session_active absent in a pre-amendment log line

        features = {name: raw_features[name] for name in feature_names}
        if "user_session_active" in features:
            features["user_session_active"] = int(bool(features["user_session_active"]))

        label = int(simulate_click(decision, users_by_id, campaigns_by_id, rng))
        rows.append({"features": features, "label": label})
    return rows


def _split_train_holdout(
    rows: list[dict[str, Any]], holdout_fraction: float, rng: random.Random
) -> tuple[list[dict], list[dict]]:
    shuffled = list(rows)
    rng.shuffle(shuffled)
    n_holdout = max(1, int(len(shuffled) * holdout_fraction)) if shuffled else 0
    return shuffled[n_holdout:], shuffled[:n_holdout]


def _matrix(rows: list[dict[str, Any]], feature_names: list[str]) -> tuple[list[list[float]], list[int]]:
    X = [[row["features"][name] for name in feature_names] for row in rows]
    y = [row["label"] for row in rows]
    return X, y


def _build_estimator(config: dict[str, Any]):
    return make_pipeline(
        StandardScaler(), LogisticRegression(random_state=config["seed"], **config["hyperparams"])
    )


def retrain(
    config: dict[str, Any] = RETRAIN_CONFIG,
    data_dir: Path = DEFAULT_DATA_DIR,
    decision_log_path: Path = DEFAULT_LOG_PATH,
    models_dir: Path = DEFAULT_MODELS_DIR,
) -> dict[str, Any]:
    """Reads decisions, builds one training row per real-campaign win,
    simulates labels, fits `config`'s estimator, evaluates on a fixed-seed
    random holdout, and writes the versioned artifact directory -
    registering it as a `candidate`, exactly like `train.py`."""
    users_by_id = {r["user_id"]: r for r in pl.read_parquet(data_dir / "users.parquet").to_dicts()}
    campaigns_by_id = {r["campaign_id"]: r for r in pl.read_parquet(data_dir / "campaigns.parquet").to_dicts()}
    decisions = read_decisions(decision_log_path)

    rng = random.Random(config["seed"])
    rows = build_rows(decisions, users_by_id, campaigns_by_id, config["feature_names"], rng)
    if not rows:
        raise ValueError(
            f"no usable training rows found in {decision_log_path} - "
            "run real traffic through /serve first (see ops/README.md)"
        )
    train_rows, holdout_rows = _split_train_holdout(rows, HOLDOUT_FRACTION, rng)

    X_train, y_train = _matrix(train_rows, config["feature_names"])
    X_holdout, y_holdout = _matrix(holdout_rows, config["feature_names"])

    estimator = _build_estimator(config)
    estimator.fit(X_train, y_train)
    model = PctrModel(estimator, config["feature_names"])

    y_pred = [model.predict(row["features"]) for row in holdout_rows]
    auc = float(roc_auc_score(y_holdout, y_pred)) if len(set(y_holdout)) > 1 else float("nan")

    live_comparison: dict[str, Any] | None = None
    try:
        live_path = model_registry.get_live_path(LOGICAL_NAME, path=models_dir / "registry.json")
        live_model = PctrModel.load(live_path / "model.pkl")
        live_pred = [
            live_model.predict({name: row["features"][name] for name in live_model.feature_names()})
            for row in holdout_rows
        ]
        live_auc = float(roc_auc_score(y_holdout, live_pred)) if len(set(y_holdout)) > 1 else float("nan")
        live_comparison = {"live_version": live_path.name, "live_auc": live_auc}
    except model_registry.ModelRegistryError:
        pass  # no live version registered yet - nothing to compare against

    eval_report: dict[str, Any] = {
        "auc": auc,
        "n_train": len(train_rows),
        "n_holdout": len(holdout_rows),
    }
    if live_comparison is not None:
        eval_report.update(live_comparison)

    version_dir = models_dir / "pctr" / config["version"]
    model.save(version_dir / "model.pkl")
    (version_dir / "feature_names.json").write_text(json.dumps(config["feature_names"], indent=2) + "\n")
    training_config = {
        "algorithm": config["algorithm"],
        "hyperparams": config["hyperparams"],
        "seed": config["seed"],
        "source": "decision_log",
        "decision_log_path": str(decision_log_path),
    }
    (version_dir / "training_config.json").write_text(json.dumps(training_config, indent=2) + "\n")
    (version_dir / "eval_report.json").write_text(json.dumps(eval_report, indent=2) + "\n")

    model_registry.register_version(
        LOGICAL_NAME,
        config["version"],
        str(version_dir),
        status="candidate",
        path=models_dir / "registry.json",
    )

    return {"model": model, "eval_report": eval_report, "version_dir": version_dir}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    result = retrain()
    print(f"trained {RETRAIN_CONFIG['version']} ({RETRAIN_CONFIG['algorithm']}) -> {result['version_dir']}")
    print(json.dumps(result["eval_report"], indent=2))


if __name__ == "__main__":
    main()
