"""News sentiment via Marketaux (verified live against the real API -- field names
confirmed from an actual response, not docs). Stubs out if no API key is configured,
per spec: don't build a fragile scraper fallback first.
"""
import logging
from datetime import datetime, timezone

import requests

from config import settings
from storage import redis_client
from storage.postgres_client import get_session
from storage.postgres_models import NewsSentiment

logger = logging.getLogger(__name__)

MARKETAUX_URL = "https://api.marketaux.com/v1/news/all"
SEARCH_QUERY = "Nifty OR Sensex OR RBI OR India economy OR NSE OR BSE"


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
    stored = 0
    with get_session() as session:
        for article in articles:
            url = article.get("url")
            if not url:
                continue
            dedupe_key = f"news:{url}"
            if redis_client.is_duplicate(dedupe_key):
                continue

            entities = article.get("entities", [])
            scores = [e["sentiment_score"] for e in entities if e.get("sentiment_score") is not None]
            sentiment = str(round(sum(scores) / len(scores), 4)) if scores else None

            published_at = article.get("published_at")
            published_dt = None
            if published_at:
                try:
                    published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
                except ValueError:
                    published_dt = None

            session.add(
                NewsSentiment(
                    fetched_at=fetched_at,
                    source_account="external",
                    headline=article.get("title", "")[:500],
                    summary=(article.get("description") or article.get("snippet") or "")[:2000],
                    sentiment=sentiment,
                    source=article.get("source"),
                    url=url,
                    published_at=published_dt,
                )
            )
            stored += 1

    if stored:
        redis_client.mark_write("news_sentiment")
    return stored
