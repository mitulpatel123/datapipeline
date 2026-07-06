"""Shared validate-then-write helpers used by every connector.

Every write path goes: pydantic validation -> redis idempotency check -> postgres insert.
Rejects are logged to system_errors and reported as one batched Telegram alert per call
(never per-row) per spec section 6.2. Heavy validation failure (>20% of a batch) also
quarantines a sample of the raw payload to bad_payloads for later inspection -- one bad
row should never silently kill the rest of a snapshot, but heavy failure is worth a
closer look than a one-line summary gives you.
"""
import json
import logging
from datetime import datetime, timezone

from pydantic import ValidationError

from alerts.telegram_alert import send_telegram_alert
from storage import redis_client
from storage.postgres_models import BadPayload, OptionChainSnapshot, SystemError, TickData
from storage.validation_schemas import OptionChainRowIn, TickDataIn

logger = logging.getLogger(__name__)

SNAPSHOT_ROW_COUNT_HISTORY = 20
PARTIAL_SNAPSHOT_THRESHOLD = 0.7
HEAVY_REJECT_RATIO = 0.2


def compute_option_row_quality_flags(row: dict) -> dict:
    flags = {
        "missing_ltp": row.get("ltp") in (None, 0),
        "missing_oi": row.get("oi") is None,
        "zero_volume": (row.get("volume") or 0) == 0,
        "missing_greeks": any(row.get(g) is None for g in ("delta", "theta", "gamma", "vega")),
        "bid_gt_ask": bool(row.get("bid") and row.get("ask") and row["bid"] > row["ask"]),
        "stale_snapshot": (row.get("ltp") or 0) == 0 and (row.get("oi") or 0) == 0 and (row.get("volume") or 0) == 0,
    }
    return {k: v for k, v in flags.items() if v}


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


def _check_partial_snapshot(session, expiry, stored_count: int, source_account: str):
    """Alert if this snapshot has far fewer rows than recent history suggests it should
    (spec item 23) -- e.g. a truncated/partial Dhan response that still parses fine."""
    history_key = f"nifty:snapshot_row_count_hist:{expiry}"
    history = [int(v) for v in redis_client.client.lrange(history_key, 0, -1)]
    if len(history) >= 5:
        sorted_hist = sorted(history)
        median = sorted_hist[len(sorted_hist) // 2]
        if median > 0 and stored_count < median * PARTIAL_SNAPSHOT_THRESHOLD:
            log_and_alert(
                f"{source_account}_partial_snapshot",
                f"{source_account}: option chain snapshot for {expiry} has only {stored_count} rows, "
                f"below {int(PARTIAL_SNAPSHOT_THRESHOLD * 100)}% of the recent median ({median}) -- "
                f"possible partial/truncated response.",
                severity="warning",
            )
    redis_client.client.lpush(history_key, stored_count)
    redis_client.client.ltrim(history_key, 0, SNAPSHOT_ROW_COUNT_HISTORY - 1)
    redis_client.client.expire(history_key, 6 * 3600)


def _quarantine_bad_payload(session, component: str, source_account: str, rows: list[dict], rejects: list[str]):
    sample = json.dumps(rows[:5], default=str)[:4000]
    session.add(
        BadPayload(
            fetched_at=datetime.now(timezone.utc),
            source_account=source_account,
            component=component,
            reason=f"{len(rejects)}/{len(rows)} rows rejected on validation. First error: {rejects[0][:500]}",
            raw_payload=sample,
        )
    )


def store_option_chain_snapshot(session, rows: list[dict], source_account: str) -> tuple[int, int]:
    stored, rejects = 0, []
    expiry = rows[0]["expiry"] if rows else None
    for raw in rows:
        raw = {**raw, "source_account": source_account}
        try:
            validated = OptionChainRowIn(**raw)
        except ValidationError as exc:
            rejects.append(str(exc))
            continue
        if redis_client.is_duplicate(validated.dedupe_key()):
            continue
        quality_flags = compute_option_row_quality_flags(raw)
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
                data_quality_flags=(quality_flags or None),
                security_id=validated.security_id,
            )
        )
        stored += 1

    _report_rejects("option_chain_snapshot", session, rejects)
    if rows and rejects and (len(rejects) / len(rows)) > HEAVY_REJECT_RATIO:
        _quarantine_bad_payload(session, "option_chain_snapshot", source_account, rows, rejects)
    if stored:
        redis_client.mark_write(f"option_chain_snapshots:{source_account}")
        if expiry:
            _check_partial_snapshot(session, expiry, stored, source_account)
    return stored, len(rejects)


def store_tick_rows(session, rows: list[dict], source_account: str) -> tuple[int, int]:
    stored, rejects = 0, []
    stored_symbols: set[str] = set()
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
        stored_symbols.add(validated.symbol)

    _report_rejects("tick_data", session, rejects)
    # Per-source/instrument marks (e.g. tick_data:acct1_ws:NIFTY) so the gap watchdog can
    # tell a dead websocket apart from a still-healthy quote-reconciliation feed even
    # though both write to the same tick_data table.
    for symbol in stored_symbols:
        redis_client.mark_write(f"tick_data:{source_account}:{symbol}")
    return stored, len(rejects)
