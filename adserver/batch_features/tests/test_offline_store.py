import datetime as dt

from adserver.batch_features.offline_store import available_partitions, query_as_of
from adserver.batch_features.runner import run

EARLY = dt.date(2026, 7, 3)
LATE = dt.date(2026, 7, 18)


def _seed_two_partitions(output_dir):
    run(as_of=EARLY, output_dir=output_dir)
    run(as_of=LATE, output_dir=output_dir)


def test_available_partitions_discovers_via_duckdb(tmp_path):
    output_dir = tmp_path / "features"
    _seed_two_partitions(output_dir)
    assert available_partitions("user", output_dir) == [EARLY, LATE]
    assert available_partitions("ad", output_dir) == [EARLY, LATE]


def test_available_partitions_empty_when_nothing_materialized(tmp_path):
    assert available_partitions("user", tmp_path / "nothing_here") == []


def test_query_as_of_returns_most_recent_qualifying_partition(tmp_path):
    output_dir = tmp_path / "features"
    _seed_two_partitions(output_dir)

    between = query_as_of("user", dt.date(2026, 7, 10), output_dir)
    assert between.height == 50
    assert set(between["asof"].to_list()) == {EARLY}

    exact = query_as_of("user", LATE, output_dir)
    assert set(exact["asof"].to_list()) == {LATE}


def test_query_as_of_returns_empty_before_any_partition(tmp_path):
    output_dir = tmp_path / "features"
    _seed_two_partitions(output_dir)
    result = query_as_of("user", dt.date(2026, 6, 1), output_dir)
    assert result.height == 0


def test_query_as_of_never_leaks_future_data(tmp_path):
    """The point-in-time contract: a query as_of an early date must never
    return values from a later partition, even though a later one exists."""
    output_dir = tmp_path / "features"
    _seed_two_partitions(output_dir)
    result = query_as_of("user", EARLY, output_dir)
    assert LATE not in set(result["asof"].to_list())
