from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class OptionChainSnapshot(Base):
    __tablename__ = "option_chain_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    expiry: Mapped[object] = mapped_column(Date, nullable=False)
    strike: Mapped[float] = mapped_column(Numeric, nullable=False)
    option_type: Mapped[str] = mapped_column(String(2), nullable=False)  # CE / PE
    ltp: Mapped[float] = mapped_column(Numeric, nullable=True)
    oi: Mapped[int] = mapped_column(Integer, nullable=True)
    prev_oi: Mapped[int] = mapped_column(Integer, nullable=True)
    volume: Mapped[int] = mapped_column(Integer, nullable=True)
    iv: Mapped[float] = mapped_column(Numeric, nullable=True)
    delta: Mapped[float] = mapped_column(Numeric, nullable=True)
    theta: Mapped[float] = mapped_column(Numeric, nullable=True)
    gamma: Mapped[float] = mapped_column(Numeric, nullable=True)
    vega: Mapped[float] = mapped_column(Numeric, nullable=True)
    bid: Mapped[float] = mapped_column(Numeric, nullable=True)
    ask: Mapped[float] = mapped_column(Numeric, nullable=True)
    underlying_ltp: Mapped[float] = mapped_column(Numeric, nullable=True)
    data_quality_flags: Mapped[object] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        Index("ix_ocs_fetched_at", "fetched_at"),
        Index("ix_ocs_expiry_strike", "expiry", "strike", "option_type"),
    )


class OhlcvIntraday(Base):
    __tablename__ = "ohlcv_intraday"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    security_id: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    interval: Mapped[str] = mapped_column(String, nullable=False)  # 1min/5min/15min/day
    open: Mapped[float] = mapped_column(Numeric, nullable=True)
    high: Mapped[float] = mapped_column(Numeric, nullable=True)
    low: Mapped[float] = mapped_column(Numeric, nullable=True)
    close: Mapped[float] = mapped_column(Numeric, nullable=True)
    volume: Mapped[int] = mapped_column(Integer, nullable=True)
    oi: Mapped[int] = mapped_column(Integer, nullable=True)
    bar_timestamp: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_ohlcv_fetched_at", "fetched_at"),
        Index("ix_ohlcv_symbol_interval_bar", "symbol", "interval", "bar_timestamp"),
        UniqueConstraint(
            "source_account", "symbol", "interval", "bar_timestamp", name="uq_ohlcv_bar"
        ),
    )


class TickData(Base):
    __tablename__ = "tick_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    security_id: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    ltp: Mapped[float] = mapped_column(Numeric, nullable=True)
    ltt: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=True)
    volume: Mapped[int] = mapped_column(Integer, nullable=True)
    oi: Mapped[int] = mapped_column(Integer, nullable=True)
    bid_depth: Mapped[object] = mapped_column(JSONB, nullable=True)
    ask_depth: Mapped[object] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        Index("ix_tick_fetched_at", "fetched_at"),
        Index("ix_tick_security_id", "security_id"),
        Index("ix_tick_account_security_ltt", "source_account", "security_id", "ltt"),
    )


class GlobalIndex(Base):
    __tablename__ = "global_indices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    symbol: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[float] = mapped_column(Numeric, nullable=True)
    change_pct: Mapped[float] = mapped_column(Numeric, nullable=True)

    __table_args__ = (
        Index("ix_gidx_fetched_at", "fetched_at"),
        Index("ix_gidx_symbol", "symbol"),
    )


class IndiaVix(Base):
    __tablename__ = "india_vix"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    value: Mapped[float] = mapped_column(Numeric, nullable=True)

    __table_args__ = (Index("ix_vix_fetched_at", "fetched_at"),)


class GiftNifty(Base):
    __tablename__ = "gift_nifty"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    value: Mapped[float] = mapped_column(Numeric, nullable=True)

    __table_args__ = (Index("ix_giftnifty_fetched_at", "fetched_at"),)


class FiiDiiData(Base):
    __tablename__ = "fii_dii_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    category: Mapped[str] = mapped_column(String, nullable=False)  # FII / DII
    segment: Mapped[str] = mapped_column(String, nullable=False)  # cash / fno
    buy_value: Mapped[float] = mapped_column(Numeric, nullable=True)
    sell_value: Mapped[float] = mapped_column(Numeric, nullable=True)
    net_value: Mapped[float] = mapped_column(Numeric, nullable=True)
    date: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        Index("ix_fiidii_fetched_at", "fetched_at"),
        Index("ix_fiidii_category_segment_date", "category", "segment", "date"),
    )


class NewsSentiment(Base):
    __tablename__ = "news_sentiment"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    headline: Mapped[str] = mapped_column(String, nullable=False)
    summary: Mapped[str] = mapped_column(String, nullable=True)
    sentiment: Mapped[str] = mapped_column(String, nullable=True)  # kept for backward compatibility
    sentiment_score: Mapped[float] = mapped_column(Numeric, nullable=True)
    source: Mapped[str] = mapped_column(String, nullable=True)
    url: Mapped[str] = mapped_column(String, nullable=True)
    published_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_news_fetched_at", "fetched_at"),
        Index("uq_news_url", "url", unique=True, postgresql_where=text("url IS NOT NULL")),
    )


class DerivedAnalytics(Base):
    __tablename__ = "derived_analytics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    metric_name: Mapped[str] = mapped_column(String, nullable=False)
    value: Mapped[float] = mapped_column(Numeric, nullable=True)
    expiry: Mapped[object] = mapped_column(Date, nullable=True)
    strike: Mapped[float] = mapped_column(Numeric, nullable=True)
    extra: Mapped[object] = mapped_column(JSONB, nullable=True)

    __table_args__ = (
        Index("ix_derived_fetched_at", "fetched_at"),
        Index("ix_derived_metric_name", "metric_name"),
    )


class SystemError(Base):
    __tablename__ = "system_errors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    component: Mapped[str] = mapped_column(String, nullable=False)
    error_message: Mapped[str] = mapped_column(String, nullable=False)
    severity: Mapped[str] = mapped_column(String, nullable=False)
    resolved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    __table_args__ = (Index("ix_syserr_fetched_at", "fetched_at"),)


class DataGapLog(Base):
    __tablename__ = "data_gap_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    expected_fetch_time: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    actual_fetch_time: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=True)
    data_type: Mapped[str] = mapped_column(String, nullable=False)
    gap_seconds: Mapped[float] = mapped_column(Numeric, nullable=True)
    symbol: Mapped[str] = mapped_column(String, nullable=True)
    security_id: Mapped[str] = mapped_column(String, nullable=True)
    severity: Mapped[str] = mapped_column(String, nullable=True)
    resolved_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_gaplog_fetched_at", "fetched_at"),
        Index("ix_gaplog_data_type", "data_type"),
    )


class ApiRequestLog(Base):
    """Every Dhan REST attempt (including retries) goes through here via
    connectors.dhan_request_manager -- this is what the daily report uses to compute
    real request counts and 429 incidents per account/endpoint."""

    __tablename__ = "api_request_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    endpoint_family: Mapped[str] = mapped_column(String, nullable=False)
    endpoint_name: Mapped[str] = mapped_column(String, nullable=False)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False)
    status_code: Mapped[int] = mapped_column(Integer, nullable=True)
    dhan_status: Mapped[str] = mapped_column(String, nullable=True)
    remarks: Mapped[str] = mapped_column(String, nullable=True)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=True)
    rate_limited: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    circuit_state: Mapped[str] = mapped_column(String, nullable=True)
    cooldown_until: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_apilog_fetched_at", "fetched_at"),
        Index("ix_apilog_account_family_fetched", "source_account", "endpoint_family", "fetched_at"),
    )


class BadPayload(Base):
    """Quarantine for payloads that fail validation heavily -- lets one malformed
    response get inspected later without crashing the ingest job or losing the
    rest of a snapshot."""

    __tablename__ = "bad_payloads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fetched_at: Mapped[object] = mapped_column(DateTime(timezone=True), nullable=False)
    source_account: Mapped[str] = mapped_column(String, nullable=True)

    component: Mapped[str] = mapped_column(String, nullable=False)
    reason: Mapped[str] = mapped_column(String, nullable=False)
    raw_payload: Mapped[str] = mapped_column(String, nullable=True)

    __table_args__ = (Index("ix_badpayload_fetched_at", "fetched_at"),)
