"""Centralizes every Dhan REST call: account-level + endpoint-family rate limiting,
a circuit breaker on 429/soft-ban signals, retry/backoff, and request logging.

No connector should call the dhanhq SDK directly outside this module. A 429 seen by
one code path (e.g. derived analytics fetching a futures quote) must be visible to
every other path hitting that account, or the circuit breaker protecting the option
chain job is blind to load created elsewhere -- which is exactly how both Dhan
accounts got soft-limited during development (see feedback_dhan_api_quirks memory /
README "Known issues").

Escalation (per spec): a 429 is any status!=success response whose remarks/status
text contains a rate-limit signal (429, "too many requests", "rate limit", "blocked",
"temporarily blocked", "further requests may result"). Tracked per ACCOUNT (not per
family) for escalation tier, since Dhan's warnings are clearly account-wide risk
signals even when a single endpoint triggered them:
  - 1st hit: pause the triggering family 60s, then run it at reduced speed for 15min.
  - 2nd hit (this session): pause the triggering family 5min, then reduced speed;
    disable every OTHER ("non-critical") family for this account for the rest of
    the session.
  - 3rd hit (this session): disable ALL REST families for this account until the
    next process start (a fresh session). WebSocket, if already connected, is
    untouched -- it's a separate connection type with its own documented limits.

Do NOT keep retrying aggressively after a 429 is detected -- retrying into an active
rate limit is what turns a warning into an actual block. On a rate-limit signal we
immediately trip the breaker and raise, skipping the normal backoff-retry loop.
"""
import logging
import threading
import time
from datetime import datetime, timezone
from enum import Enum

from config import settings
from connectors.rate_limiter import MinIntervalLimiter, TokenBucketRateLimiter
from storage.ingest import log_and_alert

logger = logging.getLogger(__name__)

RATE_LIMIT_SIGNALS = (
    "429",
    "too many requests",
    "rate limit",
    "blocked",
    "temporarily blocked",
    "further requests may result",
)

MAX_RETRIES = 3
BACKOFF_CAP_SECONDS = 4
REDUCED_SPEED_MULTIPLIER = 3.0


class CircuitState(str, Enum):
    CLOSED = "closed"
    COOLDOWN = "cooldown"
    REDUCED = "reduced"
    DISABLED = "disabled"


def is_rate_limit_signal(remarks) -> bool:
    text = str(remarks).lower()
    return any(sig in text for sig in RATE_LIMIT_SIGNALS)


class _FamilyState:
    def __init__(self, min_interval_seconds: float):
        self.min_interval_limiter = MinIntervalLimiter(min_interval_seconds) if min_interval_seconds > 0 else None
        self.circuit_state = CircuitState.CLOSED
        self.cooldown_until: float | None = None
        self.reduced_until: float | None = None


class DhanRequestManager:
    """One instance per Dhan account (acct1 / acct2) -- never share across accounts."""

    def __init__(self, account_label: str):
        self.account_label = account_label
        self.token_bucket = TokenBucketRateLimiter(
            rate_per_second=settings.DHAN_GLOBAL_RPS, capacity=settings.DHAN_GLOBAL_BURST
        )
        self.families: dict[str, _FamilyState] = {
            "optionchain": _FamilyState(settings.DHAN_OPTIONCHAIN_MIN_INTERVAL_SECONDS),
            "marketquote": _FamilyState(settings.DHAN_MARKETQUOTE_MIN_INTERVAL_SECONDS),
            "historical": _FamilyState(0.25),
            "instrument_master": _FamilyState(0),
        }
        self.incident_count = 0
        self.account_lock = threading.Lock()

    def _family(self, endpoint_family: str) -> _FamilyState:
        if endpoint_family not in self.families:
            self.families[endpoint_family] = _FamilyState(0)
        return self.families[endpoint_family]

    def is_available(self, endpoint_family: str) -> bool:
        fam = self._family(endpoint_family)
        now = time.monotonic()
        if fam.circuit_state == CircuitState.DISABLED:
            return False
        if fam.circuit_state == CircuitState.COOLDOWN:
            if fam.cooldown_until is not None and now >= fam.cooldown_until:
                fam.circuit_state = CircuitState.REDUCED
                fam.reduced_until = now + settings.DHAN_429_REDUCED_SPEED_MINUTES * 60
                logger.info(
                    "%s/%s circuit breaker: cooldown elapsed, entering REDUCED speed mode",
                    self.account_label, endpoint_family,
                )
            else:
                return False
        if fam.circuit_state == CircuitState.REDUCED and fam.reduced_until is not None and now >= fam.reduced_until:
            fam.circuit_state = CircuitState.CLOSED
            fam.reduced_until = None
            logger.info("%s/%s circuit breaker: back to CLOSED (normal speed)", self.account_label, endpoint_family)
        return True

    def _record_rate_limit_hit(self, endpoint_family: str, remarks):
        with self.account_lock:
            self.incident_count += 1
            tier = self.incident_count
            now = time.monotonic()

            if tier == 1:
                fam = self._family(endpoint_family)
                fam.circuit_state = CircuitState.COOLDOWN
                fam.cooldown_until = now + settings.DHAN_429_FIRST_COOLDOWN_SECONDS
                msg = (
                    f"{self.account_label}/{endpoint_family}: FIRST rate-limit signal this session. "
                    f"Pausing {settings.DHAN_429_FIRST_COOLDOWN_SECONDS}s, then reduced-speed for "
                    f"{settings.DHAN_429_REDUCED_SPEED_MINUTES}min. Remarks: {remarks}"
                )
            elif tier == 2:
                for name, fam in self.families.items():
                    if name == endpoint_family:
                        fam.circuit_state = CircuitState.COOLDOWN
                        fam.cooldown_until = now + settings.DHAN_429_SECOND_COOLDOWN_SECONDS
                    else:
                        fam.circuit_state = CircuitState.DISABLED
                msg = (
                    f"{self.account_label}/{endpoint_family}: SECOND rate-limit signal this session. "
                    f"Pausing {settings.DHAN_429_SECOND_COOLDOWN_SECONDS}s on {endpoint_family}; all "
                    f"other REST families disabled for {self.account_label} for the rest of this "
                    f"session. Remarks: {remarks}"
                )
            else:
                for fam in self.families.values():
                    fam.circuit_state = CircuitState.DISABLED
                    fam.cooldown_until = None
                    fam.reduced_until = None
                msg = (
                    f"{self.account_label}/{endpoint_family}: THIRD rate-limit signal this session -- "
                    f"ALL REST calls disabled for {self.account_label} until the next process start. "
                    f"WebSocket (if already connected) is unaffected. Remarks: {remarks}"
                )

        logger.error(msg)
        log_and_alert(f"{self.account_label}_circuit_breaker", msg, severity="critical")

    def call(self, endpoint_family: str, endpoint_name: str, fn, *args, **kwargs) -> dict:
        """fn is a bound dhanhq SDK method, e.g. client.option_chain. Returns the SDK's
        response dict on success. Raises RuntimeError if the breaker is open, a
        rate-limit signal is detected, or retries are exhausted."""
        if not self.is_available(endpoint_family):
            raise RuntimeError(
                f"Dhan {self.account_label}/{endpoint_family} circuit breaker is open -- "
                f"skipping this call to avoid worsening a rate-limit/ban situation."
            )

        fam = self._family(endpoint_family)
        delay = 1.0
        last_response = None
        last_latency_ms = None

        for attempt in range(1, MAX_RETRIES + 2):
            if fam.min_interval_limiter is not None:
                fam.min_interval_limiter.acquire(endpoint_family)
            if fam.circuit_state == CircuitState.REDUCED and fam.min_interval_limiter is not None:
                time.sleep(fam.min_interval_limiter.min_interval * (REDUCED_SPEED_MULTIPLIER - 1))
            self.token_bucket.acquire()

            start = time.monotonic()
            try:
                last_response = fn(*args, **kwargs)
            except Exception as exc:
                # The dhanhq SDK normally catches network/timeout errors itself and
                # returns a {"status": "failure", ...} dict -- but don't rely on that
                # holding for every code path forever. An uncaught exception here would
                # otherwise skip logging, retry, and circuit-breaker handling entirely.
                # Convert it into the same synthetic failure shape and fall through to
                # the existing unified failure/retry logic below.
                last_response = {
                    "status": "failure",
                    "remarks": f"{type(exc).__name__}: {exc}",
                    "data": "",
                }
                logger.warning(
                    "%s/%s (%s) raised %s on attempt %d: %s",
                    self.account_label, endpoint_family, endpoint_name, type(exc).__name__, attempt, exc,
                )
            last_latency_ms = int((time.monotonic() - start) * 1000)

            success = last_response.get("status") == "success"
            remarks = last_response.get("remarks")
            rate_limited = (not success) and is_rate_limit_signal(remarks)

            self._log_request(
                endpoint_family, endpoint_name, success, remarks, last_latency_ms, attempt, rate_limited, fam.circuit_state
            )

            if success:
                return last_response

            if rate_limited:
                self._record_rate_limit_hit(endpoint_family, remarks)
                raise RuntimeError(f"Dhan rate-limited on {self.account_label}/{endpoint_family}: {remarks}")

            logger.warning(
                "%s/%s (%s) call failed (attempt %d/%d): %s",
                self.account_label, endpoint_family, endpoint_name, attempt, MAX_RETRIES + 1, remarks,
            )
            if attempt <= MAX_RETRIES:
                time.sleep(delay)
                delay = min(delay * 2, BACKOFF_CAP_SECONDS)

        remarks = last_response.get("remarks") if last_response else "no response"
        log_and_alert(
            f"{self.account_label}_{endpoint_family}",
            f"{self.account_label}/{endpoint_family} ({endpoint_name}): Dhan API call failed after "
            f"{MAX_RETRIES + 1} attempts. Details: {remarks}",
        )
        raise RuntimeError(f"Dhan API call failed on {self.account_label}/{endpoint_family}: {remarks}")

    def _log_request(self, endpoint_family, endpoint_name, success, remarks, latency_ms, attempt, rate_limited, circuit_state):
        try:
            from storage.postgres_client import get_session
            from storage.postgres_models import ApiRequestLog

            with get_session() as session:
                session.add(
                    ApiRequestLog(
                        fetched_at=datetime.now(timezone.utc),
                        source_account=self.account_label,
                        endpoint_family=endpoint_family,
                        endpoint_name=endpoint_name,
                        success=success,
                        status_code=None,
                        dhan_status=("success" if success else "failed"),
                        remarks=(str(remarks)[:1000] if remarks else None),
                        latency_ms=latency_ms,
                        attempt=attempt,
                        rate_limited=rate_limited,
                        circuit_state=str(circuit_state.value if hasattr(circuit_state, "value") else circuit_state),
                    )
                )
        except Exception:
            logger.exception("Failed to write api_request_log row")


_managers: dict[str, DhanRequestManager] = {}
_managers_lock = threading.Lock()


def get_request_manager(account_label: str) -> DhanRequestManager:
    with _managers_lock:
        if account_label not in _managers:
            _managers[account_label] = DhanRequestManager(account_label)
        return _managers[account_label]
