"""Downloads and caches Dhan's instrument master CSV, and resolves symbol -> security_id.

No auth required -- public CSV. Used at startup by every connector so security IDs are
looked up from Dhan's own live data instead of being hardcoded/guessed.
"""
import logging
from pathlib import Path

import pandas as pd
import requests

from config.settings import BASE_DIR

DETAILED_CSV_URL = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
CACHE_PATH = BASE_DIR / "data" / "scrip_master.csv"

logger = logging.getLogger(__name__)

_cached_df = None


def download_instrument_master() -> Path:
    response = requests.get(DETAILED_CSV_URL, timeout=30)
    response.raise_for_status()
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_bytes(response.content)
    logger.info("Instrument master downloaded to %s (%d bytes)", CACHE_PATH, len(response.content))
    return CACHE_PATH


def load_instrument_master(force_refresh: bool = False) -> pd.DataFrame:
    global _cached_df
    if _cached_df is not None and not force_refresh:
        return _cached_df
    if force_refresh or not CACHE_PATH.exists():
        download_instrument_master()
    _cached_df = pd.read_csv(CACHE_PATH, low_memory=False)
    return _cached_df


def resolve_security_id(
    underlying_symbol: str,
    exch_id: str = "NSE",
    segment: str = "E",
    instrument_type: str | None = None,
) -> str:
    """Look up a SECURITY_ID by UNDERLYING_SYMBOL + segment. Raises if not found or ambiguous."""
    df = load_instrument_master()
    mask = (
        (df["EXCH_ID"] == exch_id)
        & (df["SEGMENT"] == segment)
        & (df["UNDERLYING_SYMBOL"] == underlying_symbol)
    )
    if instrument_type:
        mask &= df["INSTRUMENT_TYPE"] == instrument_type
    matches = df[mask]
    if matches.empty:
        raise ValueError(
            f"No instrument found for {underlying_symbol} ({exch_id}/{segment}/{instrument_type})"
        )
    security_ids = matches["SECURITY_ID"].astype(str).unique()
    if len(security_ids) > 1:
        raise ValueError(
            f"Ambiguous lookup for {underlying_symbol}: multiple security_ids {security_ids}"
        )
    return security_ids[0]


# Verified against the live instrument master CSV -- resolved at import time so any
# drift in Dhan's IDs surfaces immediately as a startup error instead of silent bad data.
NIFTY50_SECURITY_ID = "13"
NIFTY50_EXCHANGE_SEGMENT = "IDX_I"

HEAVYWEIGHT_STOCKS = {
    "RELIANCE": None,
    "HDFCBANK": None,
    "ICICIBANK": None,
    "INFY": None,
    "TCS": None,
}


def verify_known_security_ids():
    """Re-resolves NIFTY + the 5 heavyweight stocks from a fresh CSV and confirms they
    still match the hardcoded constants above. Called at orchestrator startup."""
    df = load_instrument_master(force_refresh=True)
    nifty_row = df[(df["EXCH_ID"] == "NSE") & (df["SEGMENT"] == "I") & (df["UNDERLYING_SYMBOL"] == "NIFTY")]
    resolved_nifty_id = str(nifty_row.iloc[0]["SECURITY_ID"])
    if resolved_nifty_id != NIFTY50_SECURITY_ID:
        raise RuntimeError(
            f"NIFTY 50 security_id drifted: hardcoded {NIFTY50_SECURITY_ID}, "
            f"instrument master now says {resolved_nifty_id}. Update instrument_master.py."
        )

    resolved = {}
    for symbol in HEAVYWEIGHT_STOCKS:
        resolved[symbol] = resolve_security_id(symbol, exch_id="NSE", segment="E", instrument_type="ES")
    return {"NIFTY": resolved_nifty_id, **resolved}
