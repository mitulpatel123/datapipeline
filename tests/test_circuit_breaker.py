import pytest

from connectors.dhan_request_manager import CircuitState, DhanRequestManager, is_rate_limit_signal


def _success_fn(*args, **kwargs):
    return {"status": "success", "remarks": "", "data": {"ok": True}}


def _rate_limited_fn(*args, **kwargs):
    return {"status": "failure", "remarks": "Too many requests. Further requests may result in the user being blocked.", "data": ""}


def _generic_failure_fn(*args, **kwargs):
    return {"status": "failure", "remarks": "some transient error", "data": ""}


@pytest.fixture
def manager(monkeypatch):
    # Circuit trips call log_and_alert, which sends a real Telegram message -- mock it
    # so running this suite doesn't spam the user's phone with test alerts.
    monkeypatch.setattr("connectors.dhan_request_manager.log_and_alert", lambda *a, **k: None)
    monkeypatch.setattr("connectors.dhan_request_manager.DhanRequestManager._log_request", lambda *a, **k: None)
    m = DhanRequestManager("test_account")
    for fam in m.families.values():
        fam.min_interval_limiter = None  # don't slow the test suite down with real waits
    return m


def test_is_rate_limit_signal_detects_known_phrases():
    assert is_rate_limit_signal("Too many requests. Further requests may result in the user being blocked.")
    assert is_rate_limit_signal({"error": "429"})
    assert not is_rate_limit_signal("Invalid Request")


def test_successful_call_passes_through(manager):
    response = manager.call("optionchain", "option_chain", _success_fn)
    assert response["status"] == "success"
    assert manager.families["optionchain"].circuit_state == CircuitState.CLOSED


def test_rate_limit_trips_breaker_without_retry_loop(manager):
    call_count = {"n": 0}

    def counting_rate_limited_fn(*args, **kwargs):
        call_count["n"] += 1
        return _rate_limited_fn()

    with pytest.raises(RuntimeError, match="rate-limited"):
        manager.call("optionchain", "option_chain", counting_rate_limited_fn)

    # Must not have retried into the rate limit -- exactly one call.
    assert call_count["n"] == 1
    assert manager.families["optionchain"].circuit_state == CircuitState.COOLDOWN


def test_breaker_open_blocks_subsequent_calls_without_calling_fn(manager):
    with pytest.raises(RuntimeError):
        manager.call("optionchain", "option_chain", _rate_limited_fn)

    call_count = {"n": 0}

    def tracked_fn(*args, **kwargs):
        call_count["n"] += 1
        return _success_fn()

    with pytest.raises(RuntimeError, match="circuit breaker is open"):
        manager.call("optionchain", "option_chain", tracked_fn)
    assert call_count["n"] == 0, "fn must not be called while the breaker is open"


def test_second_incident_disables_other_families(manager):
    # A cooled-down family blocks itself from further calls (tested above), so tier 2/3
    # escalation in real usage only happens via a hit on a DIFFERENT family, or after
    # real time passes for the first family's cooldown to elapse. Exercise the
    # escalation state machine directly rather than waiting on real cooldowns.
    manager._record_rate_limit_hit("optionchain", "first hit")
    manager._record_rate_limit_hit("marketquote", "second hit")

    assert manager.families["marketquote"].circuit_state == CircuitState.COOLDOWN
    assert manager.families["historical"].circuit_state == CircuitState.DISABLED
    assert manager.families["instrument_master"].circuit_state == CircuitState.DISABLED
    assert manager.is_available("historical") is False


def test_third_incident_disables_everything(manager):
    manager._record_rate_limit_hit("optionchain", "first hit")
    manager._record_rate_limit_hit("marketquote", "second hit")
    manager._record_rate_limit_hit("historical", "third hit")

    for family in manager.families.values():
        assert family.circuit_state == CircuitState.DISABLED


def test_generic_failure_retries_then_raises(manager, monkeypatch):
    monkeypatch.setattr("connectors.dhan_request_manager.time.sleep", lambda seconds: None)
    call_count = {"n": 0}

    def counting_fn(*args, **kwargs):
        call_count["n"] += 1
        return _generic_failure_fn()

    with pytest.raises(RuntimeError, match="Dhan API call failed"):
        manager.call("optionchain", "option_chain", counting_fn)

    assert call_count["n"] > 1, "non-rate-limit failures should retry with backoff"
