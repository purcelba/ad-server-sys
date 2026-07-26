"""Model-performance validation plot: observed CTR by ad category (context
— what does real signal look like in the holdout window), and predicted
vs. actual CTR on the holdout (calibration — does "the model says 3%"
actually mean ~3% of those impressions get clicked?).

Ad-hoc diagnostic at the user's request, not a phases.md acceptance
criterion. Reuses eval_report.json's already-computed calibration curve
(no retraining) and reads events.parquet directly for the category panel
(independent of the trained model, same spirit as datagen/eda.py's plots).
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import polars as pl
import typer

from adserver.batch_features.runner import DEFAULT_OUTPUT_DIR
from adserver.ranking import model_registry
from adserver.ranking.model import PctrModel
from adserver.ranking.train import HISTORY_END, HOLDOUT_START, _build_rows, _label_impressions, _split_train_holdout

app = typer.Typer(add_completion=False)

DEFAULT_MODELS_DIR = Path("models")
DEFAULT_DATA_DIR = Path("data")


def _observed_ctr_by_category(data_dir: Path = DEFAULT_DATA_DIR) -> pl.DataFrame:
    """Observed CTR per ad category, restricted to the holdout window —
    the same category-level computation style as datagen/eda.py, but
    unconditioned on model predictions: a raw "does the underlying data
    look like what we'd expect" context panel."""
    events = pl.read_parquet(data_dir / "events.parquet")
    holdout = events.filter((pl.col("event_date") >= HOLDOUT_START) & (pl.col("event_date") <= HISTORY_END))

    impressions = holdout.filter(pl.col("event_type") == "impression").group_by("category").len().rename({"len": "impressions"})
    clicks = holdout.filter(pl.col("event_type") == "click").group_by("category").len().rename({"len": "clicks"})

    ctr = impressions.join(clicks, on="category", how="left").fill_null(0)
    return ctr.with_columns((pl.col("clicks") / pl.col("impressions")).alias("ctr")).sort("category")


def _load_eval_report(version_dir: Path) -> dict:
    return json.loads((version_dir / "eval_report.json").read_text())


def _resolve_version_dir(version: str | None, live: bool, models_dir: Path) -> Path:
    if live:
        return Path(model_registry.get_live_path("pctr", models_dir / "registry.json"))
    return models_dir / "pctr" / version


def plot_eval(version_dir: Path, data_dir: Path = DEFAULT_DATA_DIR, out_path: Path | None = None) -> Path:
    out_path = out_path or version_dir / "eval_plot.png"
    eval_report = _load_eval_report(version_dir)
    category_ctr = _observed_ctr_by_category(data_dir)
    calibration = eval_report["calibration_curve"]

    fig, (ax_category, ax_calibration) = plt.subplots(1, 2, figsize=(12, 5))

    ax_category.bar(category_ctr["category"].to_list(), category_ctr["ctr"].to_list(), color="steelblue")
    ax_category.set_title("Observed CTR by ad category (holdout window)")
    ax_category.set_ylabel("CTR")
    ax_category.tick_params(axis="x", rotation=30)
    for label in ax_category.get_xticklabels():
        label.set_ha("right")

    mean_predicted = calibration["mean_predicted"]
    fraction_positive = calibration["fraction_positive"]
    lo, hi = 0.0, max(max(mean_predicted), max(fraction_positive)) * 1.1
    ax_calibration.plot([lo, hi], [lo, hi], linestyle="--", color="gray", label="perfect calibration (y=x)")
    ax_calibration.scatter(mean_predicted, fraction_positive, color="darkorange", zorder=3)
    ax_calibration.set_xlim(lo, hi)
    ax_calibration.set_ylim(lo, hi)
    ax_calibration.set_xlabel("mean predicted CTR (per decile bin)")
    ax_calibration.set_ylabel("actual CTR (per decile bin)")
    ax_calibration.set_title(
        f"Predicted vs. actual CTR, holdout (AUC={eval_report['auc']:.3f}, baseline={eval_report['baseline_auc']:.3f})"
    )
    ax_calibration.legend()

    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return out_path


def _holdout_predictions_by_category(model: PctrModel, data_dir: Path, output_dir: Path) -> pl.DataFrame:
    impressions = _label_impressions(data_dir)
    rows = _build_rows(impressions, output_dir)
    _, holdout_rows = _split_train_holdout(rows)

    records = [
        {"category": row["category"], "label": row["label"], "predicted": model.predict(row["features"])}
        for row in holdout_rows
    ]
    return (
        pl.DataFrame(records)
        .group_by("category")
        .agg(
            pl.col("label").mean().alias("actual_ctr"),
            pl.col("predicted").mean().alias("predicted_ctr"),
            pl.len().alias("n"),
        )
        .sort("category")
    )


def plot_calibration_by_category(
    version_dir: Path,
    data_dir: Path = DEFAULT_DATA_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    out_path: Path | None = None,
) -> Path:
    """One point per category: mean predicted CTR vs. actual CTR on the
    holdout, colored by category. A per-category breakdown of the same
    calibration question plot_eval()'s aggregate-decile panel asks —
    useful for spotting whether the model is systematically off for a
    specific category rather than just noisy overall."""
    out_path = out_path or version_dir / "eval_plot_by_category.png"
    model = PctrModel.load(version_dir / "model.pkl")
    per_category = _holdout_predictions_by_category(model, data_dir, output_dir)

    categories = per_category["category"].to_list()
    predicted = per_category["predicted_ctr"].to_list()
    actual = per_category["actual_ctr"].to_list()

    fig, ax = plt.subplots(figsize=(7, 7))
    lo, hi = 0.0, max(max(predicted), max(actual)) * 1.15
    ax.plot([lo, hi], [lo, hi], linestyle="--", color="gray", zorder=1, label="perfect calibration (y=x)")

    cmap = plt.get_cmap("tab10")
    for i, (category, p, a) in enumerate(zip(categories, predicted, actual)):
        ax.scatter(p, a, s=160, color=cmap(i % 10), edgecolors="white", linewidths=1.5, zorder=3, label=category)

    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.set_xlabel("mean predicted CTR")
    ax.set_ylabel("actual CTR")
    ax.set_title("Predicted vs. actual CTR by ad category (holdout)")
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return out_path


@app.command()
def main(
    version: str = typer.Option(None, help="Model version, e.g. v1. Ignored if --live is set."),
    live: bool = typer.Option(True, help="Use whichever version is currently live in models/registry.json."),
    models_dir: Path = typer.Option(DEFAULT_MODELS_DIR),
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
) -> None:
    if not live and version is None:
        raise typer.BadParameter("pass --version, or omit it and use --live (the default)")
    version_dir = _resolve_version_dir(version, live, models_dir)
    out_path = plot_eval(version_dir, data_dir)
    typer.echo(f"wrote {out_path}")
    by_category_path = plot_calibration_by_category(version_dir, data_dir)
    typer.echo(f"wrote {by_category_path}")


if __name__ == "__main__":
    app()
