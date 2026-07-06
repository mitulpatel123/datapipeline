"""Additive schema migration -- scripts/init_db.py's create_all() only creates
tables that don't exist yet; it never alters an existing table's columns/indexes.
Run this any time storage/postgres_models.py gains a new column, table, or index.

Safe to run any number of times: every check queries Postgres's own catalog
(via SQLAlchemy's inspector) before acting, so a second run reports "already
exists" instead of re-running DDL or erroring.
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import inspect, text

from storage.postgres_client import engine
from storage.postgres_models import Base

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("migrate_phase1_schema")


def _column_exists(inspector, table: str, column: str) -> bool:
    return any(col["name"] == column for col in inspector.get_columns(table))


def _index_or_constraint_exists(inspector, table: str, name: str) -> bool:
    indexes = {idx["name"] for idx in inspector.get_indexes(table)}
    constraints = {uc["name"] for uc in inspector.get_unique_constraints(table)}
    return name in indexes or name in constraints


def _ensure_column(conn, inspector, table: str, column: str, coltype: str):
    label = f"{table}.{column}"
    if _column_exists(inspector, table, column):
        logger.info("Already exists: %s", label)
        return
    conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {coltype}"))
    logger.info("Created: %s", label)


def _ensure_index(conn, inspector, table: str, index_name: str, ddl: str, label: str | None = None):
    label = label or index_name
    if _index_or_constraint_exists(inspector, table, index_name):
        logger.info("Already exists: %s", label)
        return
    conn.execute(text(ddl))
    logger.info("Created: %s", label)


def _duplicate_url_count(conn) -> int:
    result = conn.execute(
        text(
            "SELECT count(*) FROM ("
            "  SELECT url FROM news_sentiment WHERE url IS NOT NULL GROUP BY url HAVING count(*) > 1"
            ") dupes"
        )
    )
    return result.scalar() or 0


def run():
    # New tables (api_request_log, bad_payloads, etc.) that don't exist at all yet --
    # create_all is itself idempotent (checkfirst=True), so this is safe to repeat.
    Base.metadata.create_all(engine, checkfirst=True)
    logger.info("Ensured all tables from postgres_models exist (create_all, idempotent).")

    inspector = inspect(engine)  # fresh snapshot, in case create_all just added tables

    with engine.begin() as conn:
        _ensure_column(conn, inspector, "option_chain_snapshots", "data_quality_flags", "JSONB")
        _ensure_column(conn, inspector, "option_chain_snapshots", "security_id", "VARCHAR")
        _ensure_index(
            conn, inspector, "option_chain_snapshots", "ix_ocs_security_id",
            "CREATE INDEX IF NOT EXISTS ix_ocs_security_id ON option_chain_snapshots (security_id)",
        )

        _ensure_column(conn, inspector, "news_sentiment", "sentiment_score", "NUMERIC")

        dup_count = _duplicate_url_count(conn)
        if dup_count > 0:
            logger.warning(
                "SKIPPING unique index on news_sentiment.url -- found %d duplicate URL value(s) "
                "already stored. Clean these up (keep the newest row per url) before this index "
                "can be created. Not creating the index does NOT crash anything; it just means "
                "duplicate news rows can keep accumulating until it's cleaned up.",
                dup_count,
            )
        else:
            _ensure_index(
                conn, inspector, "news_sentiment", "uq_news_url",
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_news_url ON news_sentiment (url) WHERE url IS NOT NULL",
            )

        _ensure_column(conn, inspector, "data_gap_log", "symbol", "VARCHAR")
        _ensure_column(conn, inspector, "data_gap_log", "security_id", "VARCHAR")
        _ensure_column(conn, inspector, "data_gap_log", "severity", "VARCHAR")
        _ensure_column(conn, inspector, "data_gap_log", "resolved_at", "TIMESTAMPTZ")

        if _index_or_constraint_exists(inspector, "ohlcv_intraday", "uq_ohlcv_bar"):
            logger.info("Already exists: ohlcv_intraday unique bar constraint")
        else:
            conn.execute(
                text(
                    "ALTER TABLE ohlcv_intraday ADD CONSTRAINT uq_ohlcv_bar "
                    "UNIQUE (source_account, symbol, interval, bar_timestamp)"
                )
            )
            logger.info("Created: ohlcv_intraday unique bar constraint")

        _ensure_index(
            conn, inspector, "tick_data", "ix_tick_account_security_ltt",
            "CREATE INDEX IF NOT EXISTS ix_tick_account_security_ltt ON tick_data (source_account, security_id, ltt)",
        )

    logger.info("Migration complete.")


if __name__ == "__main__":
    try:
        run()
    except Exception:
        logger.exception("Migration FAILED")
        sys.exit(1)
