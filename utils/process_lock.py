"""Redis-based single-process lock so two orchestrator processes/terminals can't
double-poll Dhan and trigger rate limits (spec item 8)."""
import logging
import threading
import time

logger = logging.getLogger(__name__)


class OrchestratorLock:
    def __init__(self, redis_client, key: str, ttl_seconds: int, owner_id: str):
        self.redis_client = redis_client
        self.key = key
        self.ttl_seconds = ttl_seconds
        self.owner_id = owner_id
        self._heartbeat_thread: threading.Thread | None = None
        self._stop_heartbeat = threading.Event()

    def acquire(self) -> bool:
        return bool(self.redis_client.set(self.key, self.owner_id, ex=self.ttl_seconds, nx=True))

    def start_heartbeat(self):
        def _beat():
            interval = max(5, self.ttl_seconds // 3)
            while not self._stop_heartbeat.wait(interval):
                try:
                    current = self.redis_client.get(self.key)
                    if current == self.owner_id:
                        self.redis_client.expire(self.key, self.ttl_seconds)
                    else:
                        logger.error("Orchestrator lock %s no longer owned by us -- another instance may have taken over", self.key)
                except Exception:
                    logger.exception("Orchestrator lock heartbeat failed")

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
