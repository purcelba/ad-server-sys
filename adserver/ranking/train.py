"""pCTR training: point-in-time joins the synthetic impression history
against Phase 1's offline feature store, trains a version from a small
config dict, and writes a versioned artifact directory.

Real-time (Redis) features are structurally absent from every config
here — there is no historical log of past Redis state to join against a
20-day-old synthetic impression. See ranking/README.md for why, and for
the forward pointer to Phase 5's `make retrain` (training from a real
decision log), which is the point real-time features actually become
trainable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import polars as pl
from sklearn.calibration import calibration_curve
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from adserver.batch_features.offline_store import available_partitions, query_as_of
from adserver.batch_features.runner import DEFAULT_DATA_DIR, DEFAULT_OUTPUT_DIR, DEFAULT_REGISTRY_PATH
from adserver.batch_features.runner import run as run_batch_jobs
from adserver.common.registry import load_registry
from adserver.datagen.users import HISTORY_END, HISTORY_START
from adserver.ranking import model_registry
from adserver.ranking.assemble import from_offline_row
from adserver.ranking.model import PctrModel

_FEATURE_REGISTRY = load_registry(DEFAULT_REGISTRY_PATH)


def _fill_registry_defaults(row: dict[str, Any]) -> dict[str, Any]:
    """`offline_store` rows can carry `null` for a windowed feature with
    no data yet (Phase 1's convention — a real "no signal" distinct from
    a fabricated zero); `feature_service.resolver` substitutes the
    registry default for exactly this case at read time. Training reads
    `offline_store` directly rather than going through the service, so it
    must apply the identical defaults policy itself, or train silently on
    NaN."""
    filled = dict(row)
    for name, value in filled.items():
        if value is None and name in _FEATURE_REGISTRY:
            filled[name] = _FEATURE_REGISTRY[name].default
    return filled

LOGICAL_NAME = "pctr"
DEFAULT_MODELS_DIR = Path("models")

# See "Time split for the holdout" in the phase-4 plan: fixed calendar
# dates, not a random split, so a re-run with the same data is exactly
# reproducible (AC1). The first 7 days of the synthetic window are
# excluded entirely as a feature warm-up buffer - their trailing 7d/30d
# windows are mostly empty and (confirmed directly) the very first day's
# row-count quality gate fails outright with zero users represented.
WARMUP_DAYS = 7
HOLDOUT_DAYS = 5
TRAIN_START = HISTORY_START + dt.timedelta(days=WARMUP_DAYS)
HOLDOUT_START = HISTORY_END - dt.timedelta(days=HOLDOUT_DAYS - 1)

V1_CONFIG: dict[str, Any] = {
    "version": "v1",
    "algorithm": "logistic_regression",
    "feature_names": [
        "user_ctr_30d",
        "user_impressions_7d",
        "user_rides_per_week",
        "user_account_age_days",
        "ad_ctr_7d",
        "ad_ctr_30d",
        "ad_impressions_7d",
        "campaign_spend_yesterday",
        "hour_of_day",
        "x_user_ctr_in_ad_category",
    ],
    "hyperparams": {},
    "seed": 42,
}

# A different algorithm AND a different (smaller) feature list, per AC5's
# opacity proof — both flow through the identical registry/promote/scorer
# path with zero code changes.
V2_CONFIG: dict[str, Any] = {
    "version": "v2",
    "algorithm": "hist_gradient_boosting",
    "feature_names": [
        "user_ctr_30d",
        "user_impressions_7d",
        "user_account_age_days",
        "ad_ctr_7d",
        "ad_impressions_7d",
        "hour_of_day",
        "x_user_ctr_in_ad_category",
    ],
    "hyperparams": {"max_depth": 3},
    "seed": 42,
}


def backfill(
    data_dir: Path = DEFAULT_DATA_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    start: dt.date = TRAIN_START,
    end: dt.date = HISTORY_END,
) -> None:
    """Runs Phase 1's already point-in-time-correct batch jobs once per
    day across [start, end] via the untouched `batch_features.runner.run`
    (no Phase 1 code modified — this is new orchestration calling it in a
    loop), writing one real offline Parquet partition per day. Skips a
    day whose partition already exists, so re-running `make train` never
    redoes the backfill for data that hasn't changed."""
    existing = set(available_partitions("user", output_dir))
    d = start
    while d <= end:
        if d not in existing:
            run_batch_jobs(as_of=d, data_dir=data_dir, output_dir=output_dir, materialize_to_dynamo=False)
        d += dt.timedelta(days=1)


def _label_impressions(data_dir: Path) -> pl.DataFrame:
    """One row per impression in [TRAIN_START, HISTORY_END], with
    label=1 if some click's click_id points back at it."""
    events = pl.read_parquet(data_dir / "events.parquet")
    clicked_ids = events.filter(pl.col("event_type") == "click")["click_id"].drop_nulls().to_list()
    impressions = events.filter(
        (pl.col("event_type") == "impression")
        & (pl.col("event_date") >= TRAIN_START)
        & (pl.col("event_date") <= HISTORY_END)
    )
    return impressions.with_columns(pl.col("event_id").is_in(clicked_ids).alias("label"))


def _build_rows(impressions: pl.DataFrame, output_dir: Path) -> list[dict[str, Any]]:
    """The point-in-time join: for each day, pull that day's exact
    materialized user/ad partitions (already backfilled) and assemble one
    training row per impression via the same `assemble.from_offline_row`
    the cross-parity test also exercises."""
    rows: list[dict[str, Any]] = []
    for event_date in sorted(impressions["event_date"].unique().to_list()):
        day_impressions = impressions.filter(pl.col("event_date") == event_date)
        user_by_id = {r["user_id"]: r for r in query_as_of("user", event_date, output_dir).to_dicts()}
        ad_by_id = {r["campaign_id"]: r for r in query_as_of("ad", event_date, output_dir).to_dicts()}

        for imp in day_impressions.to_dicts():
            user_row = user_by_id.get(imp["user_id"])
            ad_row = ad_by_id.get(imp["campaign_id"])
            if user_row is None or ad_row is None:
                continue  # no materialized features for this entity on this day - skip, never fabricate
            features = from_offline_row(_fill_registry_defaults(user_row), _fill_registry_defaults(ad_row), imp["category"])
            features["hour_of_day"] = imp["hour_of_day"]
            rows.append(
                {
                    "features": features,
                    "label": int(imp["label"]),
                    "event_date": event_date,
                    # kept alongside, not inside, "features" - this is provenance for
                    # analysis (e.g. eval_plots.py's per-category calibration), not a
                    # model input; _matrix() only ever reads config["feature_names"].
                    "category": imp["category"],
                }
            )
    return rows


def _split_train_holdout(rows: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    train_rows = [r for r in rows if r["event_date"] < HOLDOUT_START]
    holdout_rows = [r for r in rows if r["event_date"] >= HOLDOUT_START]
    return train_rows, holdout_rows


def _matrix(rows: list[dict[str, Any]], feature_names: list[str]) -> tuple[list[list[float]], list[int]]:
    X = [[row["features"][name] for name in feature_names] for row in rows]
    y = [row["label"] for row in rows]
    return X, y


def _build_estimator(config: dict[str, Any]):
    algorithm = config["algorithm"]
    if algorithm == "logistic_regression":
        # user_impressions_7d etc. run in the hundreds while the CTR
        # features are 0-1 - unscaled, lbfgs failed to converge within
        # its default iteration budget. A Pipeline still implements
        # .predict_proba(X), so PctrModel doesn't need to know or care.
        return make_pipeline(
            StandardScaler(), LogisticRegression(random_state=config["seed"], **config["hyperparams"])
        )
    if algorithm == "hist_gradient_boosting":
        return HistGradientBoostingClassifier(random_state=config["seed"], **config["hyperparams"])
    raise ValueError(f"unknown algorithm {algorithm!r}")


def train(
    config: dict[str, Any],
    data_dir: Path = DEFAULT_DATA_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    models_dir: Path = DEFAULT_MODELS_DIR,
) -> dict[str, Any]:
    """Backfills, builds the point-in-time training matrix, fits `config`'s
    algorithm, evaluates on the fixed holdout, and writes the full
    versioned artifact directory (model + feature list + training config
    + eval report), registering it as a `candidate`. Returns the in-memory
    model + eval report for callers (tests, `main()`) that want them
    without re-reading from disk."""
    backfill(data_dir, output_dir)
    impressions = _label_impressions(data_dir)
    rows = _build_rows(impressions, output_dir)
    train_rows, holdout_rows = _split_train_holdout(rows)

    X_train, y_train = _matrix(train_rows, config["feature_names"])
    X_holdout, y_holdout = _matrix(holdout_rows, config["feature_names"])

    estimator = _build_estimator(config)
    estimator.fit(X_train, y_train)
    model = PctrModel(estimator, config["feature_names"])

    y_pred = [model.predict(row["features"]) for row in holdout_rows]
    auc = float(roc_auc_score(y_holdout, y_pred))
    baseline_scores = [row["features"]["ad_ctr_30d"] for row in holdout_rows]
    baseline_auc = float(roc_auc_score(y_holdout, baseline_scores))
    fraction_positive, mean_predicted = calibration_curve(y_holdout, y_pred, n_bins=10, strategy="quantile")

    eval_report = {
        "auc": auc,
        "baseline_auc": baseline_auc,
        "n_train": len(train_rows),
        "n_holdout": len(holdout_rows),
        "calibration_curve": {
            "mean_predicted": mean_predicted.tolist(),
            "fraction_positive": fraction_positive.tolist(),
        },
    }

    version_dir = models_dir / "pctr" / config["version"]
    model.save(version_dir / "model.pkl")
    (version_dir / "feature_names.json").write_text(json.dumps(config["feature_names"], indent=2) + "\n")
    training_config = {
        "algorithm": config["algorithm"],
        "hyperparams": config["hyperparams"],
        "seed": config["seed"],
        "train_date_range": [TRAIN_START.isoformat(), (HOLDOUT_START - dt.timedelta(days=1)).isoformat()],
        "holdout_date_range": [HOLDOUT_START.isoformat(), HISTORY_END.isoformat()],
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
    parser.add_argument("--version", choices=["v1", "v2"], default="v1")
    args = parser.parse_args()
    config = V1_CONFIG if args.version == "v1" else V2_CONFIG

    result = train(config)
    print(f"trained {config['version']} ({config['algorithm']}) -> {result['version_dir']}")
    print(json.dumps(result["eval_report"], indent=2))


if __name__ == "__main__":
    main()
