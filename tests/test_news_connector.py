from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import delete, select

import connectors.news_connector as nc
from storage.postgres_client import get_session
from storage.postgres_models import NewsSentiment, SystemError

TEST_URL = "http://test.example/duplicate-news-article"


def _fake_response(articles):
    resp = MagicMock()
    resp.raise_for_status = lambda: None
    resp.json.return_value = {"data": articles}
    return resp


def _article(sentiment_score=None):
    entities = [{"sentiment_score": sentiment_score}] if sentiment_score is not None else []
    return {
        "title": "Test headline",
        "description": "Test summary",
        "source": "test.com",
        "url": TEST_URL,
        "published_at": "2026-07-06T10:00:00.000000Z",
        "entities": entities,
    }


@pytest.fixture(autouse=True)
def _cleanup():
    yield
    with get_session() as session:
        session.execute(delete(NewsSentiment).where(NewsSentiment.url == TEST_URL))
        session.execute(delete(SystemError).where(SystemError.component == "news_connector"))


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setattr("connectors.news_connector.settings.MARKETAUX_API_KEY", "test-key")


def test_duplicate_url_does_not_crash_and_stores_once(monkeypatch):
    monkeypatch.setattr(
        "connectors.news_connector.requests.get",
        lambda *a, **k: _fake_response([_article(sentiment_score=0.5)]),
    )

    first = nc.fetch_and_store_news()
    assert first == 1

    # Same URL appears again in a later cycle (e.g. Marketaux re-surfaces it) --
    # must not raise IntegrityError.
    second = nc.fetch_and_store_news()
    assert second == 0

    with get_session() as session:
        rows = session.execute(select(NewsSentiment).where(NewsSentiment.url == TEST_URL)).scalars().all()
    assert len(rows) == 1


def test_numeric_sentiment_goes_into_sentiment_score(monkeypatch):
    monkeypatch.setattr(
        "connectors.news_connector.requests.get",
        lambda *a, **k: _fake_response([_article(sentiment_score=0.6)]),
    )
    nc.fetch_and_store_news()

    with get_session() as session:
        row = session.execute(select(NewsSentiment).where(NewsSentiment.url == TEST_URL)).scalar_one()
    assert float(row.sentiment_score) == pytest.approx(0.6)
    assert row.sentiment == "positive"


def test_missing_sentiment_score_stores_none_and_no_label(monkeypatch):
    monkeypatch.setattr(
        "connectors.news_connector.requests.get",
        lambda *a, **k: _fake_response([_article(sentiment_score=None)]),
    )
    nc.fetch_and_store_news()

    with get_session() as session:
        row = session.execute(select(NewsSentiment).where(NewsSentiment.url == TEST_URL)).scalar_one()
    assert row.sentiment_score is None
    assert row.sentiment is None


def test_mark_write_happens_even_with_zero_new_articles(monkeypatch):
    from storage import redis_client

    redis_client.client.delete("nifty:last_successful_write:news_sentiment")
    monkeypatch.setattr("connectors.news_connector.requests.get", lambda *a, **k: _fake_response([]))

    stored = nc.fetch_and_store_news()
    assert stored == 0
    assert redis_client.client.get("nifty:last_successful_write:news_sentiment") is not None
