"""EDA visualizations: observed CTR by segment x category, and by segment x
time-of-day. Plain sanity cross-checks against lifts.py on the generated
data, not a reimplementation of the lift logic."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import polars as pl
import typer

from adserver.datagen.lifts import NIGHT_HOURS

app = typer.Typer(add_completion=False)


def _ctr_pivot(events: pl.DataFrame, by: str) -> pl.DataFrame:
    impressions = events.filter(events["event_type"] == "impression")
    clicks = events.filter(events["event_type"] == "click")

    imp_counts = impressions.group_by(["segment", by]).len().rename({"len": "impressions"})
    click_counts = clicks.group_by(["segment", by]).len().rename({"len": "clicks"})

    ctr = imp_counts.join(click_counts, on=["segment", by], how="left").fill_null(0)
    ctr = ctr.with_columns((pl.col("clicks") / pl.col("impressions")).alias("ctr"))
    return ctr.pivot(on=by, index="segment", values="ctr").fill_null(0.0)


def plot_ctr_by_segment_category(events: pl.DataFrame, out_path: Path) -> None:
    pivot = _ctr_pivot(events, "category").sort("segment")
    segments = pivot["segment"].to_list()
    categories = [c for c in pivot.columns if c != "segment"]
    matrix = pivot.select(categories).to_numpy()

    fig, ax = plt.subplots(figsize=(8, 6))
    im = ax.imshow(matrix, cmap="viridis", aspect="auto")
    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=45, ha="right")
    ax.set_yticks(range(len(segments)))
    ax.set_yticklabels(segments)
    ax.set_title("Observed CTR by user segment x campaign category")
    for i in range(len(segments)):
        for j in range(len(categories)):
            ax.text(j, i, f"{matrix[i, j]:.1%}", ha="center", va="center", color="white", fontsize=8)
    fig.colorbar(im, ax=ax, label="CTR")
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def _with_time_bucket(events: pl.DataFrame) -> pl.DataFrame:
    """Day/night bucket, matching how lifts.py actually conditions on time.

    Raw hourly buckets are too sparse per (segment, hour) cell at this data
    volume (single-digit clicks per cell) for CTR to be legible signal
    rather than noise; day/night is the granularity the lift table itself
    uses.
    """
    night_hours = list(NIGHT_HOURS)
    return events.with_columns(
        pl.when(pl.col("hour_of_day").is_in(night_hours))
        .then(pl.lit("night"))
        .otherwise(pl.lit("day"))
        .alias("time_bucket")
    )


def plot_ctr_by_segment_time_bucket(events: pl.DataFrame, out_path: Path) -> None:
    events = _with_time_bucket(events)
    pivot = _ctr_pivot(events, "time_bucket").sort("segment")
    segments = pivot["segment"].to_list()
    buckets = [b for b in ["day", "night"] if b in pivot.columns]
    matrix = pivot.select(buckets).to_numpy()

    fig, ax = plt.subplots(figsize=(5, 6))
    im = ax.imshow(matrix, cmap="magma", aspect="auto")
    ax.set_xticks(range(len(buckets)))
    ax.set_xticklabels(buckets)
    ax.set_yticks(range(len(segments)))
    ax.set_yticklabels(segments)
    ax.set_title("Observed CTR by user segment x time of day")
    for i in range(len(segments)):
        for j in range(len(buckets)):
            ax.text(j, i, f"{matrix[i, j]:.1%}", ha="center", va="center", color="white", fontsize=9)
    fig.colorbar(im, ax=ax, label="CTR")
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def plot_click_volume_by_segment_hour(events: pl.DataFrame, out_path: Path) -> None:
    """Diagnostic: raw click counts per (segment, hour) cell, showing why
    hourly-granularity CTR is too sparse to read as signal at this scale."""
    clicks = events.filter(events["event_type"] == "click")
    counts = clicks.group_by(["segment", "hour_of_day"]).len().rename({"len": "clicks"})
    pivot = counts.pivot(on="hour_of_day", index="segment", values="clicks").fill_null(0).sort("segment")

    segments = pivot["segment"].to_list()
    hours = sorted((c for c in pivot.columns if c != "segment"), key=int)
    matrix = pivot.select(hours).to_numpy()

    fig, ax = plt.subplots(figsize=(10, 6))
    im = ax.imshow(matrix, cmap="magma", aspect="auto")
    ax.set_xticks(range(len(hours)))
    ax.set_xticklabels(hours)
    ax.set_yticks(range(len(segments)))
    ax.set_yticklabels(segments)
    ax.set_xlabel("hour_of_day")
    ax.set_title("Raw click volume by user segment x hour (diagnostic)")
    fig.colorbar(im, ax=ax, label="clicks")
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def run(data_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    events = pl.read_parquet(data_dir / "events.parquet")
    plot_ctr_by_segment_category(events, out_dir / "ctr_by_segment_category.png")
    plot_ctr_by_segment_time_bucket(events, out_dir / "ctr_by_segment_time_bucket.png")
    plot_click_volume_by_segment_hour(events, out_dir / "click_volume_by_segment_hour.png")


@app.command()
def main(
    data_dir: Path = typer.Option(Path("data"), "--data-dir", help="Directory with events.parquet"),
    out: Path = typer.Option(Path("data/eda"), "--out", help="Output directory for PNGs"),
) -> None:
    run(data_dir, out)
    typer.echo(f"Wrote EDA plots to {out}/")


if __name__ == "__main__":
    app()
