from adserver.feature_service.metrics import Metrics


def test_records_request_count_and_error_count():
    metrics = Metrics()
    metrics.record_request(latency_ms=1.0)
    metrics.record_request(latency_ms=2.0, error=True)

    snapshot = metrics.snapshot()
    assert snapshot["request_count"] == 2
    assert snapshot["error_count"] == 1


def test_p99_and_avg_computed_from_samples():
    metrics = Metrics()
    for ms in [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]:
        metrics.record_request(latency_ms=ms)

    snapshot = metrics.snapshot()
    assert snapshot["latency_ms_avg"] > 0
    assert snapshot["latency_ms_p99"] == 100  # the 99th percentile of 10 samples is the max


def test_histogram_buckets_are_cumulative():
    metrics = Metrics()
    metrics.record_request(latency_ms=3.0)
    snapshot = metrics.snapshot()
    hist = snapshot["latency_histogram_ms"]
    assert hist["<= 1ms"] == 0
    assert hist["<= 5ms"] == 1
    assert hist["<= 1000ms"] == 1


def test_empty_metrics_has_sane_defaults():
    metrics = Metrics()
    snapshot = metrics.snapshot()
    assert snapshot["request_count"] == 0
    assert snapshot["latency_ms_p99"] == 0.0
