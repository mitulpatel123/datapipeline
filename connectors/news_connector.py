"""News sentiment via Marketaux (verified live against the real API -- field names
confirmed from an actual response, not docs). Stubs out if no API key is configured,
per spec: don't build a fragile scraper fallback first.
"""
import logging
from datetime import datetime, timezone

import requests
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from config import settings
from storage import redis_client
from storage.ingest import log_system_error
from storage.postgres_client import get_session
from storage.postgres_models import NewsSentiment

logger = logging.getLogger(__name__)

MARKETAUX_URL = "https://api.marketaux.com/v1/news/all"
SEARCH_QUERY = "Nifty OR Sensex OR RBI OR India economy OR NSE OR BSE"
POSITIVE_THRESHOLD = 0.1
NEGATIVE_THRESHOLD = -0.1


def _sentiment_label(score: float | None) -> str | None:
    if score is None:
        return None
    if score > POSITIVE_THRESHOLD:
        return "positive"
    if score < NEGATIVE_THRESHOLD:
        return "negative"
    return "neutral"


def fetch_and_store_news(limit: int = 10) -> int:
    if not settings.MARKETAUX_API_KEY:
        raise NotImplementedError("MARKETAUX_API_KEY not configured -- news sentiment disabled")

    response = requests.get(
        MARKETAUX_URL,
        params={
            "api_token": settings.MARKETAUX_API_KEY,
            "search": SEARCH_QUERY,
            "language": "en",
            "limit": limit,
            "sort": "published_desc",
        },
        timeout=15,
    )
    response.raise_for_status()
    articles = response.json().get("data", [])

    fetched_at = datetime.now(timezone.utc)
    rows = []
    for article in articles:
        url = article.get("url")
        if not url:
            continue

        entities = article.get("entities", [])
        scores = [e["sentiment_score"] for e in entities if e.get("sentiment_score") is not None]
        sentiment_score = round(sum(scores) / len(scores), 4) if scores else None

        published_at = article.get("published_at")
        published_dt = None
        if published_at:
            try:
                published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            except ValueError:
                published_dt = None

        rows.append(
            {
                "fetched_at": fetched_at,
                "source_account": "external",
                "headline": article.get("title", "")[:500],
                "summary": (article.get("description") or article.get("snippet") or "")[:2000],
                "sentiment": _sentiment_label(sentiment_score),
                "sentiment_score": sentiment_score,
                "source": article.get("source"),
                "url": url,
                "published_at": published_dt,
            }
        )

    stored = 0
    if rows:
        # Redis dedupe alone (60s TTL) isn't reliable across the 5-minute news job
        # interval -- the same Marketaux article reappearing later would hit Postgres's
        # unique url index and raise IntegrityError with a plain ORM insert. Upsert
        # against that same partial unique index instead so a duplicate URL is silently
        # skipped, never a crash.
        with get_session() as session:
            stmt = pg_insert(NewsSentiment).values(rows)
            stmt = stmt.on_conflict_do_nothing(index_elements=["url"], index_where=text("url IS NOT NULL"))
            result = session.execute(stmt)
            stored = result.rowcount or 0

        skipped = len(rows) - stored
        if skipped:
            logger.info("news_connector: skipped %d duplicate-url article(s) this cycle", skipped)
            # Logged (not alerted) so the daily report can query system_errors for a
            # per-day duplicate/skipped count -- this is routine, not a failure.
            with get_session() as session:
                log_system_error(
                    session, "news_connector",
                    f"skipped {skipped} duplicate-url article(s) this cycle", severity="info",
                )

    # The stream is healthy whenever the API call itself succeeded -- mark it even on a
    # cycle with zero new (non-duplicate) articles, so the gap watchdog doesn't treat
    # "no fresh news right now" as "the news job stopped running".
    redis_client.mark_write("news_sentiment")
    return stored
