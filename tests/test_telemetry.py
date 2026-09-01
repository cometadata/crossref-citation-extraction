import json

from citations_pipeline.telemetry import EventLogger


def test_event_logger_phase_emits_json(capsys) -> None:
    events = EventLogger()
    with events.phase("demo"):
        events.emit("progress", current=1)
    out = capsys.readouterr().out
    lines = [ln for ln in out.splitlines() if ln.startswith("PIPELINE_EVENT=")]
    payloads = [json.loads(ln.removeprefix("PIPELINE_EVENT=")) for ln in lines]
    assert [p["event"] for p in payloads] == [
        "phase_started",
        "progress",
        "phase_completed",
    ]
    assert payloads[0]["phase"] == "demo"
    assert "duration_seconds" in payloads[2]


def test_event_logger_phase_failure_emits_phase_failed(capsys) -> None:
    events = EventLogger()
    try:
        with events.phase("boom"):
            raise ValueError("x")
    except ValueError:
        pass
    out = capsys.readouterr().out
    events_seen = [
        json.loads(ln.removeprefix("PIPELINE_EVENT="))["event"]
        for ln in out.splitlines()
        if ln.startswith("PIPELINE_EVENT=")
    ]
    assert "phase_failed" in events_seen
