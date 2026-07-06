from pathlib import Path

import orchestrator
from storage import redis_client


def test_fallback_path_marks_instrument_master_write(monkeypatch):
    """Account 2 disabled -> orchestrator falls back to a direct download. That
    fallback must still call redis_client.mark_write("instrument_master"), or a
    real successful refresh looks like "never written" to the gap watchdog."""
    monkeypatch.setattr(orchestrator, "acct2", None)
    monkeypatch.setattr(
        "connectors.instrument_master.download_instrument_master",
        lambda: Path("/tmp/fake_scrip_master.csv"),
    )
    redis_client.client.delete("nifty:last_successful_write:instrument_master")

    orchestrator.instrument_master_refresh_job()

    assert redis_client.client.get("nifty:last_successful_write:instrument_master") is not None
