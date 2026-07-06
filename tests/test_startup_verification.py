from unittest.mock import MagicMock

import orchestrator


def test_verify_startup_security_ids_pass(monkeypatch):
    monkeypatch.setattr(orchestrator, "verify_known_security_ids", lambda: {"NIFTY": "13"})
    alert = MagicMock()
    monkeypatch.setattr(orchestrator, "log_and_alert", alert)

    assert orchestrator.verify_startup_security_ids() is True
    alert.assert_not_called()


def test_verify_startup_security_ids_fail_alerts_and_returns_false(monkeypatch):
    def _raise():
        raise RuntimeError("NIFTY 50 security_id drifted: hardcoded 13, instrument master now says 99")

    monkeypatch.setattr(orchestrator, "verify_known_security_ids", _raise)
    alert = MagicMock()
    monkeypatch.setattr(orchestrator, "log_and_alert", alert)

    assert orchestrator.verify_startup_security_ids() is False
    alert.assert_called_once()
    assert alert.call_args.kwargs.get("severity") == "critical" or "critical" in alert.call_args.args


def test_main_aborts_before_scheduling_when_verification_fails(monkeypatch):
    """Full startup path: verification failure must prevent refresh_expiry_list,
    websocket start, and scheduler construction from ever running."""
    monkeypatch.setattr(orchestrator, "verify_startup_security_ids", lambda: False)

    fake_lock = MagicMock()
    fake_lock.acquire.return_value = True
    monkeypatch.setattr(orchestrator, "OrchestratorLock", lambda *a, **k: fake_lock)

    refresh_called = MagicMock()
    monkeypatch.setattr(orchestrator, "refresh_expiry_list", refresh_called)
    ws_start_called = MagicMock()
    monkeypatch.setattr(orchestrator, "websocket_start_job", ws_start_called)
    build_scheduler_called = MagicMock()
    monkeypatch.setattr(orchestrator, "build_scheduler", build_scheduler_called)

    orchestrator.main()

    refresh_called.assert_not_called()
    ws_start_called.assert_not_called()
    build_scheduler_called.assert_not_called()
    fake_lock.release.assert_called_once()  # lock must still be released on early abort


def test_main_continues_when_verification_passes(monkeypatch):
    monkeypatch.setattr(orchestrator, "verify_startup_security_ids", lambda: True)

    fake_lock = MagicMock()
    fake_lock.acquire.return_value = True
    monkeypatch.setattr(orchestrator, "OrchestratorLock", lambda *a, **k: fake_lock)

    monkeypatch.setattr(orchestrator, "refresh_expiry_list", MagicMock())
    monkeypatch.setattr(orchestrator, "is_market_hours_ist", lambda: False)

    fake_scheduler = MagicMock()
    build_scheduler_called = MagicMock(return_value=fake_scheduler)
    monkeypatch.setattr(orchestrator, "build_scheduler", build_scheduler_called)

    orchestrator.main()

    build_scheduler_called.assert_called_once()
    fake_scheduler.start.assert_called_once()
