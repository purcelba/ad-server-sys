from adserver.adserver.metrics import Metrics


def test_records_request_and_error_counts():
    metrics = Metrics()
    metrics.record_request({"total": 5.0})
    metrics.record_request({"total": 6.0}, error=True)
    snapshot = metrics.snapshot()
    assert snapshot["request_count"] == 2
    assert snapshot["error_count"] == 1


def test_per_stage_latency_tracked_independently():
    metrics = Metrics()
    metrics.record_request({"retrieval": 1.0, "features": 10.0, "scoring": 2.0, "bidder": 25.0, "total": 40.0})
    snapshot = metrics.snapshot()
    assert snapshot["stage_latency_ms"]["features"]["latency_ms_avg"] == 10.0
    assert snapshot["stage_latency_ms"]["bidder"]["latency_ms_avg"] == 25.0
    assert snapshot["stage_latency_ms"]["total"]["latency_ms_avg"] == 40.0


def test_fallback_rung_counts_tracked():
    metrics = Metrics()
    metrics.record_request({"total": 1.0}, fallback_rung="model_load_failure_house_ad")
    metrics.record_request({"total": 1.0}, fallback_rung="model_load_failure_house_ad")
    metrics.record_request({"total": 1.0})
    snapshot = metrics.snapshot()
    assert snapshot["fallback_rung_counts"] == {"model_load_failure_house_ad": 2}


def test_empty_metrics_has_sane_defaults():
    snapshot = Metrics().snapshot()
    assert snapshot["request_count"] == 0
    assert snapshot["stage_latency_ms"]["total"]["latency_ms_p99"] == 0.0
