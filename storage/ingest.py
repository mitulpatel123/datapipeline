"""Shared validate-then-write helpers used by every connector.

Every write path goes: pydantic validation -> redis idempotency check -> postgres insert.
Rejects are logged to system_errors and reported as one batched Telegram alert per call
(never per-row) per spec section 6.2.
"""
import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from alerts.telegram_alert import send_telegram_alert
from storage import redis_client
from storage.postgres_models import OptionChainSnapshot, SystemError, TickData
from storage.validation_schemas import OptionChainRowIn, TickDataIn

logger = logging.getLogger(__name__)


def log_system_error(session, component: str, message: str, severity: str = "error"):
    session.add(
        SystemError(
            fetched_at=datetime.now(timezone.utc),
            component=component,
            error_message=message[:2000],
            severity=severity,
            resolved=False,
        )
    )


def log_and_alert(component: str, message: str, severity: str = "error"):
    """Every Telegram alert should leave an audit trail in system_errors -- the Phase 1j
    daily report reconstructs "alerts fired" from this table, so any alert path that
    skips this function is invisible to that report."""
    from storage.postgres_client import get_session

    try:
        with get_session() as session:
            log_system_error(session, component, message, severity=severity)
    except Exception:
        logger.exception("Failed to log system_error for alert from %s", component)
    send_telegram_alert(f"[data-pipeline] {message}")


def _report_rejects(component: str, session, rejects: list[str]):
    if not rejects:
        return
    summary = f"{component}: {len(rejects)} record(s) rejected on validation. First error: {rejects[0][:300]}"
    log_system_error(session, component, summary, severity="warning")
    send_telegram_alert(f"[data-pipeline] {summary}")


def store_option_chain_snapshot(session, rows: list[dict], source_account: str) -> tuple[int, int]:
    stored, rejects = 0, []
    for raw in rows:
        raw = {**raw, "source_account": source_account}
        try:
            validated = OptionChainRowIn(**raw)
        except ValidationError as exc:
            rejects.append(str(exc))
            continue
        if redis_client.is_duplicate(validated.dedupe_key()):
            continue
        session.add(
            OptionChainSnapshot(
                fetched_at=validated.fetched_at,
                source_account=validated.source_account,
                expiry=validated.expiry,
                strike=validated.strike,
                option_type=validated.option_type,
                ltp=validated.ltp,
                oi=validated.oi,
                prev_oi=validated.prev_oi,
                volume=validated.volume,
                iv=validated.iv,
                delta=validated.delta,
                theta=validated.theta,
                gamma=validated.gamma,
                vega=validated.vega,
                bid=validated.bid,
                ask=validated.ask,
                underlying_ltp=validated.underlying_ltp,
            )
        )
        stored += 1

    _report_rejects("option_chain_snapshot", session, rejects)
    if stored:
        redis_client.mark_write("option_chain_snapshots")
    return stored, len(rejects)


def store_tick_rows(session, rows: list[dict], source_account: str) -> tuple[int, int]:
    stored, rejects = 0, []
    for raw in rows:
        raw = {**raw, "source_account": source_account}
        try:
            validated = TickDataIn(**raw)
        except ValidationError as exc:
            rejects.append(str(exc))
            continue
        if redis_client.is_duplicate(validated.dedupe_key()):
            continue
        session.add(
            TickData(
                fetched_at=validated.fetched_at,
                source_account=validated.source_account,
                security_id=validated.security_id,
                symbol=validated.symbol,
                ltp=validated.ltp,
                ltt=validated.ltt,
                volume=validated.volume,
                oi=validated.oi,
                bid_depth=validated.bid_depth,
                ask_depth=validated.ask_depth,
            )
        )
        stored += 1

    _report_rejects("tick_data", session, rejects)
    if stored:
        redis_client.mark_write("tick_data")
    return stored, len(rejects)
