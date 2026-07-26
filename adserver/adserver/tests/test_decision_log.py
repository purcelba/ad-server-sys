import concurrent.futures

from adserver.adserver.decision_log import log_decision, read_decisions


def test_log_decision_appends_a_json_line(tmp_path):
    path = tmp_path / "log.jsonl"
    log_decision({"request_id": "r1", "winner": "c_0001"}, path=path)
    log_decision({"request_id": "r2", "winner": None}, path=path)

    decisions = read_decisions(path)
    assert [d["request_id"] for d in decisions] == ["r1", "r2"]


def test_read_decisions_on_missing_file_returns_empty_list(tmp_path):
    assert read_decisions(tmp_path / "does_not_exist.jsonl") == []


def test_log_decision_serializes_non_json_native_values(tmp_path):
    import datetime as dt

    path = tmp_path / "log.jsonl"
    log_decision({"request_id": "r1", "ts": dt.datetime(2026, 7, 20, 12, 0)}, path=path)
    decisions = read_decisions(path)
    assert decisions[0]["ts"] == "2026-07-20 12:00:00"


def test_concurrent_writes_dont_corrupt_or_lose_lines(tmp_path):
    path = tmp_path / "log.jsonl"

    def _write(i):
        log_decision({"request_id": f"r{i}"}, path=path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(_write, range(100)))

    decisions = read_decisions(path)
    assert len(decisions) == 100
    assert {d["request_id"] for d in decisions} == {f"r{i}" for i in range(100)}
