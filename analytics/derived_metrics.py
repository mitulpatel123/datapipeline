"""Derived analytics computed from each new option chain snapshot (spec section 3C).

Volatility Regime is intentionally omitted -- it depends on India VIX, which is
blocked pending Section 2 manually-sourced endpoints (see connectors/scraper_vix.py).

Futures Basis does NOT call Dhan itself. It reads nifty:futures:nearest:latest from
Redis, populated by the centralized quote call in
connectors.dhan_account1.DhanAccount1.fetch_market_quote_reconciliation. Calling Dhan
directly from here would create a second, uncoordinated /marketfeed/quote request on
every option-chain cycle (every ~3s) that the circuit breaker protecting the account's
REST calls has no visibility into -- exactly the kind of hidden collision that
contributed to both accounts getting rate-limited during development.
"""
import logging
from datetime import datetime, timezone

from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import DerivedAnalytics

logger = logging.getLogger(__name__)

VOLUME_ZSCORE_WINDOW = 20
OI_PREV_TTL_SECONDS = 6 * 3600
VOL_HIST_TTL_SECONDS = 6 * 3600
FUTURES_CACHE_STALE_SECONDS = 60
FUTURES_BASIS_WARN_COOLDOWN_SECONDS = 300


def _atm_strike(strikes: list[float], underlying_ltp: float) -> float:
    return min(strikes, key=lambda s: abs(s - underlying_ltp))


def compute_pcr(rows: list[dict]) -> dict:
    put_oi = sum((r.get("oi") or 0) for r in rows if r["option_type"] == "PE")
    call_oi = sum((r.get("oi") or 0) for r in rows if r["option_type"] == "CE")
    put_vol = sum((r.get("volume") or 0) for r in rows if r["option_type"] == "PE")
    call_vol = sum((r.get("volume") or 0) for r in rows if r["option_type"] == "CE")
    return {
        "pcr_oi": (put_oi / call_oi) if call_oi else None,
        "pcr_volume": (put_vol / call_vol) if call_vol else None,
    }


def compute_max_pain(rows: list[dict]) -> float | None:
    strikes = sorted({r["strike"] for r in rows})
    if not strikes:
        return None
    by_strike_type = {(r["strike"], r["option_type"]): r for r in rows}

    best_strike, best_payout = None, None
    for candidate in strikes:
        payout = 0.0
        for strike in strikes:
            ce = by_strike_type.get((strike, "CE"))
            pe = by_strike_type.get((strike, "PE"))
            ce_oi = (ce.get("oi") or 0) if ce else 0
            pe_oi = (pe.get("oi") or 0) if pe else 0
            payout += max(candidate - strike, 0) * ce_oi  # CE writer pays out ITM
            payout += max(strike - candidate, 0) * pe_oi  # PE writer pays out ITM
        if best_payout is None or payout < best_payout:
            best_payout, best_strike = payout, candidate
    return best_strike


def compute_moneyness(rows: list[dict], underlying_ltp: float) -> dict:
    strikes = sorted({r["strike"] for r in rows})
    if not strikes:
        return {}
    atm = _atm_strike(strikes, underlying_ltp)
    labels = {}
    for r in rows:
        strike, opt_type = r["strike"], r["option_type"]
        if strike == atm:
            labels[(strike, opt_type)] = "ATM"
        elif (opt_type == "CE" and strike < underlying_ltp) or (opt_type == "PE" and strike > underlying_ltp):
            labels[(strike, opt_type)] = "ITM"
        else:
            labels[(strike, opt_type)] = "OTM"
    return labels


def compute_iv_skew(rows: list[dict], underlying_ltp: float) -> dict:
    """Nearest OTM call and nearest OTM put BY STRIKE DISTANCE from spot (strikes are
    sorted ascending, so the first strike above spot / last strike below spot are
    already the nearest by construction)."""
    strikes = sorted({r["strike"] for r in rows})
    if not strikes:
        return {"otm_put_skew": None, "otm_call_skew": None}
    atm = _atm_strike(strikes, underlying_ltp)
    by_strike_type = {(r["strike"], r["option_type"]): r for r in rows}

    atm_ce, atm_pe = by_strike_type.get((atm, "CE")), by_strike_type.get((atm, "PE"))
    atm_ivs = [leg["iv"] for leg in (atm_ce, atm_pe) if leg and leg.get("iv")]
    atm_iv = sum(atm_ivs) / len(atm_ivs) if atm_ivs else None
    if atm_iv is None:
        return {"otm_put_skew": None, "otm_call_skew": None}

    otm_calls = [s for s in strikes if s > underlying_ltp]
    otm_puts = [s for s in strikes if s < underlying_ltp]
    otm_call_iv = by_strike_type.get((otm_calls[0], "CE"), {}).get("iv") if otm_calls else None
    otm_put_iv = by_strike_type.get((otm_puts[-1], "PE"), {}).get("iv") if otm_puts else None

    return {
        "otm_put_skew": (otm_put_iv - atm_iv) if otm_put_iv else None,
        "otm_call_skew": (otm_call_iv - atm_iv) if otm_call_iv else None,
    }


def compute_oi_change(expiry: str, rows: list[dict]) -> dict:
    """Delta vs OUR last snapshot (tracked in redis), not Dhan's day-over-day previous_oi
    (which we already store separately as prev_oi on each row)."""
    changes = {}
    for r in rows:
        redis_key = f"nifty:oi_prev:{expiry}:{r['strike']}:{r['option_type']}"
        prev_raw = redis_client.client.get(redis_key)
        prev_oi = float(prev_raw) if prev_raw is not None else None
        cur_oi = r.get("oi")
        if cur_oi is not None:
            redis_client.client.set(redis_key, cur_oi, ex=OI_PREV_TTL_SECONDS)
        changes[(r["strike"], r["option_type"])] = (
            (cur_oi - prev_oi) if (cur_oi is not None and prev_oi is not None) else None
        )
    return changes


def compute_volume_zscore(rows: list[dict]) -> dict:
    zscores = {}
    for r in rows:
        redis_key = f"nifty:vol_hist:{r['strike']}:{r['option_type']}"
        history = [float(v) for v in redis_client.client.lrange(redis_key, 0, -1)]
        cur_vol = r.get("volume") or 0
        if len(history) >= 5:
            mean = sum(history) / len(history)
            variance = sum((v - mean) ** 2 for v in history) / len(history)
            std = variance**0.5
            zscores[(r["strike"], r["option_type"])] = ((cur_vol - mean) / std) if std else None
        else:
            zscores[(r["strike"], r["option_type"])] = None
        redis_client.client.lpush(redis_key, cur_vol)
        redis_client.client.ltrim(redis_key, 0, VOLUME_ZSCORE_WINDOW - 1)
        redis_client.client.expire(redis_key, VOL_HIST_TTL_SECONDS)
    return zscores


def compute_atm_and_oi_summary(rows: list[dict], underlying_ltp: float) -> dict:
    """Pure math, no Dhan calls -- summary fields useful for Phase 2/3 packets."""
    strikes = sorted({r["strike"] for r in rows})
    summary = {
        "atm_strike": _atm_strike(strikes, underlying_ltp) if strikes and underlying_ltp else None,
        "highest_call_oi_strike": None,
        "highest_put_oi_strike": None,
        "highest_call_volume_strike": None,
        "highest_put_volume_strike": None,
        "total_call_oi": sum((r.get("oi") or 0) for r in rows if r["option_type"] == "CE"),
        "total_put_oi": sum((r.get("oi") or 0) for r in rows if r["option_type"] == "PE"),
        "total_call_volume": sum((r.get("volume") or 0) for r in rows if r["option_type"] == "CE"),
        "total_put_volume": sum((r.get("volume") or 0) for r in rows if r["option_type"] == "PE"),
    }

    def _argmax_strike(option_type: str, field: str):
        candidates = [r for r in rows if r["option_type"] == option_type and r.get(field) is not None]
        if not candidates:
            return None
        return max(candidates, key=lambda r: r[field])["strike"]

    summary["highest_call_oi_strike"] = _argmax_strike("CE", "oi")
    summary["highest_put_oi_strike"] = _argmax_strike("PE", "oi")
    summary["highest_call_volume_strike"] = _argmax_strike("CE", "volume")
    summary["highest_put_volume_strike"] = _argmax_strike("PE", "volume")
    return summary


def compute_futures_basis(underlying_ltp: float) -> float | None:
    if underlying_ltp is None:
        return None
    cached = redis_client.get_latest("nifty:futures:nearest:latest")
    if not cached:
        _warn_once_per_cooldown("No cached futures quote yet -- skipping futures_basis")
        return None
    fetched_at = datetime.fromisoformat(cached["fetched_at"])
    age_seconds = (datetime.now(timezone.utc) - fetched_at).total_seconds()
    if age_seconds > FUTURES_CACHE_STALE_SECONDS:
        _warn_once_per_cooldown(f"Cached futures quote is stale ({age_seconds:.0f}s old) -- skipping futures_basis")
        return None
    return float(cached["ltp"]) - underlying_ltp


def _warn_once_per_cooldown(message: str):
    cooldown_key = "nifty:futures_basis_warn_cooldown"
    if redis_client.client.set(cooldown_key, "1", ex=FUTURES_BASIS_WARN_COOLDOWN_SECONDS, nx=True):
        logger.warning(message)


def build_market_packet(expiry: str, underlying_ltp, pcr, atm_summary, futures_basis, fetched_at: datetime):
    """Structured, LLM-free JSON summary for later phases -- no LLM calls happen here,
    this only prepares clean data."""
    packet = {
        "fetched_at": fetched_at.isoformat(),
        "expiry": expiry,
        "underlying_ltp": underlying_ltp,
        "pcr_oi": pcr.get("pcr_oi"),
        "pcr_volume": pcr.get("pcr_volume"),
        "futures_basis": futures_basis,
        **atm_summary,
    }
    redis_client.set_latest("nifty:market_packet:latest", packet, ttl=300)
    return packet


def compute_and_store(expiry: str) -> int:
    snapshot = redis_client.get_latest(f"nifty:optionchain:{expiry}:latest")
    if not snapshot:
        logger.warning("No cached option chain snapshot for %s, skipping derived analytics", expiry)
        return 0

    rows = snapshot["rows"]
    underlying_ltp = snapshot["underlying_ltp"]
    fetched_at = datetime.now(timezone.utc)

    pcr = compute_pcr(rows)
    max_pain = compute_max_pain(rows)
    moneyness = compute_moneyness(rows, underlying_ltp)
    iv_skew = compute_iv_skew(rows, underlying_ltp)
    oi_change = compute_oi_change(expiry, rows)
    vol_z = compute_volume_zscore(rows)
    futures_basis = compute_futures_basis(underlying_ltp)
    atm_summary = compute_atm_and_oi_summary(rows, underlying_ltp)

    stored = 0
    with get_session() as session:
        def add(metric_name, value, strike=None, extra=None):
            nonlocal stored
            if value is None:
                return
            session.add(
                DerivedAnalytics(
                    fetched_at=fetched_at,
                    source_account="derived",
                    metric_name=metric_name,
                    value=value if not isinstance(value, str) else None,
                    expiry=expiry,
                    strike=strike,
                    extra=extra,
                )
            )
            stored += 1

        add("pcr_oi", pcr["pcr_oi"])
        add("pcr_volume", pcr["pcr_volume"])
        add("max_pain", max_pain)
        add("iv_skew_otm_put", iv_skew["otm_put_skew"])
        add("iv_skew_otm_call", iv_skew["otm_call_skew"])
        add("futures_basis", futures_basis)
        add("atm_strike", atm_summary["atm_strike"])
        add("highest_call_oi_strike", atm_summary["highest_call_oi_strike"])
        add("highest_put_oi_strike", atm_summary["highest_put_oi_strike"])
        add("highest_call_volume_strike", atm_summary["highest_call_volume_strike"])
        add("highest_put_volume_strike", atm_summary["highest_put_volume_strike"])
        add("total_call_oi", atm_summary["total_call_oi"])
        add("total_put_oi", atm_summary["total_put_oi"])
        add("total_call_volume", atm_summary["total_call_volume"])
        add("total_put_volume", atm_summary["total_put_volume"])

        for (strike, opt_type), change in oi_change.items():
            add("oi_change", change, strike=strike, extra={"option_type": opt_type})
        for (strike, opt_type), z in vol_z.items():
            add("volume_zscore", z, strike=strike, extra={"option_type": opt_type})
        for (strike, opt_type), label in moneyness.items():
            session.add(
                DerivedAnalytics(
                    fetched_at=fetched_at, source_account="derived", metric_name="moneyness",
                    value=None, expiry=expiry, strike=strike, extra={"option_type": opt_type, "label": label},
                )
            )
            stored += 1

    if stored:
        redis_client.mark_write("derived_analytics")
    build_market_packet(expiry, underlying_ltp, pcr, atm_summary, futures_basis, fetched_at)
    return stored
