from unittest.mock import MagicMock

import pytest

from storage import redis_client
from utils.process_lock import OrchestratorLock

TEST_LOCK_KEY = "nifty:orchestrator:lock:test"


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    redis_client.client.delete(TEST_LOCK_KEY)


@pytest.fixture(autouse=True)
def _mock_alert(monkeypatch):
    monkeypatch.setattr("storage.ingest.log_and_alert", lambda *a, **k: None)


def _make_lock(owner_id, exit_fn=None, on_lock_lost=None):
    return OrchestratorLock(
        redis_client.client, TEST_LOCK_KEY, ttl_seconds=60, owner_id=owner_id,
        on_lock_lost=on_lock_lost, exit_fn=exit_fn or MagicMock(),
    )


def test_second_acquire_fails_while_first_holds_lock():
    lock_a = _make_lock("owner-a")
    lock_b = _make_lock("owner-b")
    assert lock_a.acquire() is True
    assert lock_b.acquire() is False


def test_ownership_intact_renews_and_returns_true():
    lock_a = _make_lock("owner-a")
    assert lock_a.acquire() is True
    assert lock_a._check_ownership_once() is True
    assert lock_a.lost_lock_event.is_set() is False


def test_ownership_lost_triggers_fatal_shutdown_path():
    """The exact scenario from the fix spec: owner A holds the lock, something
    overwrites it with owner B, A's heartbeat check must fail closed."""
    lock_a = _make_lock("owner-a")
    assert lock_a.acquire() is True

    # Simulate another process taking over the same key (e.g. after a TTL race).
    redis_client.client.set(TEST_LOCK_KEY, "owner-b")

    exit_fn = MagicMock()
    on_lock_lost = MagicMock()
    lock_a.exit_fn = exit_fn
    lock_a.on_lock_lost = on_lock_lost

    result = lock_a._check_ownership_once()

    assert result is False
    assert lock_a.lost_lock_event.is_set() is True
    on_lock_lost.assert_called_once()
    exit_fn.assert_called_once()


def test_on_lock_lost_callback_failure_does_not_prevent_exit():
    """A buggy on_lock_lost callback must not stop the process from still exiting --
    that would defeat the entire fail-closed guarantee."""
    lock_a = _make_lock("owner-a")
    assert lock_a.acquire() is True
    redis_client.client.set(TEST_LOCK_KEY, "owner-b")

    exit_fn = MagicMock()
    lock_a.exit_fn = exit_fn
    lock_a.on_lock_lost = MagicMock(side_effect=RuntimeError("boom"))

    lock_a._check_ownership_once()

    exit_fn.assert_called_once()


def test_transient_redis_read_failure_is_not_treated_as_lock_loss():
    lock_a = _make_lock("owner-a")
    assert lock_a.acquire() is True

    broken_client = MagicMock()
    broken_client.get.side_effect = ConnectionError("redis down")
    lock_a.redis_client = broken_client

    exit_fn = MagicMock()
    lock_a.exit_fn = exit_fn

    result = lock_a._check_ownership_once()

    assert result is True  # transient read failure, not ownership loss
    exit_fn.assert_not_called()


def test_release_only_deletes_key_if_still_owner():
    lock_a = _make_lock("owner-a")
    assert lock_a.acquire() is True
    redis_client.client.set(TEST_LOCK_KEY, "owner-b")  # someone else now owns it

    lock_a.release()

    assert redis_client.client.get(TEST_LOCK_KEY) == "owner-b"  # must not clobber owner-b's lock
