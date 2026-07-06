"""Additive schema migration -- scripts/init_db.py's create_all() only creates
tables that don't exist yet; it never alters an existing table's columns/indexes
(spec item 27). Run this any time storage/postgres_models.py gains a new column,
table, or index. Every statement is IF NOT EXISTS / idempotent, safe to re-run.
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from storage.postgres_client import engine
from storage.postgres_models import Base

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("migrate_db")

# Each entry: a human label + the DDL to run. Order matters (new tables before
# columns that might reference them, though none currently do).
STATEMENTS = [
    ("option_chain_snapshots.data_quality_flags", """
        ALTER TABLE option_chain_snapshots ADD COLUMN IF NOT EXISTS data_quality_flags JSONB
    """),
    ("option_chain_snapshots.security_id", """
        ALTER TABLE option_chain_snapshots ADD COLUMN IF NOT EXISTS security_id VARCHAR
    """),
    ("option_chain_snapshots security_id index", """
        CREATE INDEX IF NOT EXISTS ix_ocs_security_id ON option_chain_snapshots (security_id)
    """),
    ("news_sentiment.sentiment_score", """
        ALTER TABLE news_sentiment ADD COLUMN IF NOT EXISTS sentiment_score NUMERIC
    """),
    ("news_sentiment unique url index", """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_news_url ON news_sentiment (url) WHERE url IS NOT NULL
    """),
    ("data_gap_log.symbol", """
        ALTER TABLE data_gap_log ADD COLUMN IF NOT EXISTS symbol VARCHAR
    """),
    ("data_gap_log.security_id", """
        ALTER TABLE data_gap_log ADD COLUMN IF NOT EXISTS security_id VARCHAR
    """),
    ("data_gap_log.severity", """
        ALTER TABLE data_gap_log ADD COLUMN IF NOT EXISTS severity VARCHAR
    """),
    ("data_gap_log.resolved_at", """
        ALTER TABLE data_gap_log ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ
    """),
    ("ohlcv_intraday unique bar constraint", """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'uq_ohlcv_bar'
            ) THEN
                ALTER TABLE ohlcv_intraday
                ADD CONSTRAINT uq_ohlcv_bar UNIQUE (source_account, symbol, interval, bar_timestamp);
            END IF;
        END
        $$;
    """),
    ("tick_data account/security/ltt index", """
        CREATE INDEX IF NOT EXISTS ix_tick_account_security_ltt
        ON tick_data (source_account, security_id, ltt)
    """),
]


def run():
    # New tables (api_request_log, bad_payloads) that don't exist at all yet.
    Base.metadata.create_all(engine, checkfirst=True)
    logger.info("Ensured all tables from postgres_models exist.")

    with engine.begin() as conn:
        for label, ddl in STATEMENTS:
            try:
                conn.execute(text(ddl))
                logger.info("OK: %s", label)
            except Exception as exc:
                logger.error("FAILED: %s -- %s", label, exc)
                raise

    logger.info("Migration complete.")


if __name__ == "__main__":
    run()
