import numpy as np

from adserver.datagen.campaigns import generate_campaigns
from adserver.datagen.eda import (
    plot_click_volume_by_segment_hour,
    plot_ctr_by_segment_category,
    plot_ctr_by_segment_time_bucket,
)
from adserver.datagen.events import generate_events
from adserver.datagen.users import generate_users


def _events(seed=42):
    rng = np.random.default_rng(seed)
    users = generate_users(rng)
    campaigns = generate_campaigns(rng)
    return generate_events(rng, users, campaigns)


def test_ctr_by_segment_category_png_written(tmp_path):
    out = tmp_path / "ctr_by_segment_category.png"
    plot_ctr_by_segment_category(_events(), out)
    assert out.exists()
    assert out.stat().st_size > 1000


def test_ctr_by_segment_time_bucket_png_written(tmp_path):
    out = tmp_path / "ctr_by_segment_time_bucket.png"
    plot_ctr_by_segment_time_bucket(_events(), out)
    assert out.exists()
    assert out.stat().st_size > 1000


def test_click_volume_by_segment_hour_png_written(tmp_path):
    out = tmp_path / "click_volume_by_segment_hour.png"
    plot_click_volume_by_segment_hour(_events(), out)
    assert out.exists()
    assert out.stat().st_size > 1000


def test_ctr_has_nonzero_variance_across_segment_category_cells():
    events = _events()
    from adserver.datagen.eda import _ctr_pivot

    pivot = _ctr_pivot(events, "category")
    categories = [c for c in pivot.columns if c != "segment"]
    matrix = pivot.select(categories).to_numpy()
    assert matrix.std() > 0.005, "expected meaningfully varied CTR across segment x category cells"


def test_nightlife_night_ctr_beats_nightlife_day():
    """The day/night bucket should surface nightlife's planted night lift
    cleanly, now that per-cell sample sizes are large enough (this was the
    whole point of bucketing instead of 24 raw hours)."""
    from adserver.datagen.eda import _ctr_pivot, _with_time_bucket

    events = _with_time_bucket(_events())
    pivot = _ctr_pivot(events, "time_bucket")
    row = pivot.filter(pivot["segment"] == "nightlife")
    assert row["night"].item() > row["day"].item()
