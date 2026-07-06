"""Redis-based single-process lock so two orchestrator processes/terminals can't
double-poll Dhan and trigger rate limits (spec item 8).

Ownership loss during a run (e.g. Redis flushed, TTL raced, another process wrote
the same key) is treated as FATAL, not just logged: the whole point of this lock is
that at most one process is ever polling Dhan, so continuing to run after losing
ownership defeats it. The heartbeat alerts, invokes an optional on_lock_lost
callback (for a best-effort graceful scheduler shutdown), and then terminates the
process via exit_fn (os._exit by default -- deliberately not sys.exit, since this
runs on a background thread and sys.exit there would only end that thread while
the scheduler kept firing Dhan jobs from the main thread).
"""
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)


class OrchestratorLock:
    def __init__(self, redis_client, key: str, ttl_seconds: int, owner_id: str, on_lock_lost=None, exit_fn=None):
        self.redis_client = redis_client
        self.key = key
        self.ttl_seconds = ttl_seconds
        self.owner_id = owner_id
        self.on_lock_lost = on_lock_lost
        self.exit_fn = exit_fn or (lambda: os._exit(1))
        self._heartbeat_thread: threading.Thread | None = None
        self._stop_heartbeat = threading.Event()
        self.lost_lock_event = threading.Event()

    def acquire(self) -> bool:
        return bool(self.redis_client.set(self.key, self.owner_id, ex=self.ttl_seconds, nx=True))

    def _check_ownership_once(self) -> bool:
        """Renews the TTL if we still own the lock. Returns False and triggers the
        fatal-shutdown path if ownership has been lost. Split out from the heartbeat
        loop so it can be exercised deterministically in tests."""
        try:
            current = self.redis_client.get(self.key)
        except Exception:
            logger.exception("Orchestrator lock heartbeat failed to read lock state")
            return True  # a transient Redis read failure is not the same as losing the lock

        if current == self.owner_id:
            self.redis_client.expire(self.key, self.ttl_seconds)
            return True

        self._handle_lock_lost(current)
        return False

    def _handle_lock_lost(self, current_owner):
        self.lost_lock_event.set()
        message = (
            f"FATAL: orchestrator lock '{self.key}' ownership lost -- current holder is "
            f"{current_owner!r}, expected {self.owner_id!r}. Shutting down immediately so "
            f"this process cannot keep polling Dhan alongside whatever now holds the lock."
        )
        logger.critical(message)
        try:
            from storage.ingest import log_and_alert

            log_and_alert("orchestrator_lock", message, severity="critical")
        except Exception:
            logger.exception("Failed to send lock-loss alert")

        if self.on_lock_lost:
            try:
                self.on_lock_lost()
            except Exception:
                logger.exception("on_lock_lost callback failed")

        self.exit_fn()

    def start_heartbeat(self):
        def _beat():
            interval = max(5, self.ttl_seconds // 3)
            while not self._stop_heartbeat.wait(interval):
                if not self._check_ownership_once():
                    return  # fatal shutdown already triggered

        self._heartbeat_thread = threading.Thread(target=_beat, daemon=True)
        self._heartbeat_thread.start()

    def release(self):
        self._stop_heartbeat.set()
        try:
            current = self.redis_client.get(self.key)
            if current == self.owner_id:
                self.redis_client.delete(self.key)
        except Exception:
            logger.exception("Failed to release orchestrator lock")
