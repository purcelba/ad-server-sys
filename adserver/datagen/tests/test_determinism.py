import filecmp

from adserver.datagen.cli import run

FILES = ["users.parquet", "campaigns.parquet", "events.parquet"]


def test_same_seed_produces_identical_files(tmp_path):
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    run(seed=42, out=out1)
    run(seed=42, out=out2)

    for f in FILES:
        assert filecmp.cmp(out1 / f, out2 / f, shallow=False), f"{f} differs across identical-seed runs"


def test_different_seeds_produce_different_events(tmp_path):
    out1 = tmp_path / "run1"
    out2 = tmp_path / "run2"
    run(seed=42, out=out1)
    run(seed=7, out=out2)

    assert not filecmp.cmp(out1 / "events.parquet", out2 / "events.parquet", shallow=False)
