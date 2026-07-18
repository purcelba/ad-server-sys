import numpy as np

from adserver.datagen.campaigns import generate_campaigns
from adserver.datagen.eda import plot_ctr_by_segment_category, plot_ctr_by_segment_hour
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


def test_ctr_by_segment_hour_png_written(tmp_path):
    out = tmp_path / "ctr_by_segment_hour.png"
    plot_ctr_by_segment_hour(_events(), out)
    assert out.exists()
    assert out.stat().st_size > 1000


def test_ctr_has_nonzero_variance_across_segment_category_cells():
    events = _events()
    from adserver.datagen.eda import _ctr_pivot

    pivot = _ctr_pivot(events, "category")
    categories = [c for c in pivot.columns if c != "segment"]
    matrix = pivot.select(categories).to_numpy()
    assert matrix.std() > 0.005, "expected meaningfully varied CTR across segment x category cells"
