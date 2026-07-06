# Nifty 50 Data Pipeline -- Phase 1

Read-only data collection layer for the Nifty 50 options trading system. This phase
collects, validates, and stores option chain, tick, historical, global market, and
news data. No trading/order logic, no ML, no LLM calls -- see the spec for phase
boundaries.

## Setup

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
cp .env.example .env   # fill in credentials
brew services start postgresql@16   # if not already running
./venv/bin/python scripts/test_connections.py   # verify Redis + Postgres
./venv/bin/python scripts/init_db.py             # create tables
```

## Running

```bash
./venv/bin/python orchestrator.py
```

Runs one `BlockingScheduler` process (Asia/Kolkata timezone, regardless of the host
machine's local timezone) with every job: option chain (3s), websocket ticks,
quote reconciliation (5s), derived analytics, global indices (5min), news (5min),
gap watchdog (1min), EOD historical backfill, instrument master refresh.

Daily report (run after market close, or pass `--date YYYY-MM-DD`):

```bash
./venv/bin/python scripts/generate_daily_report.py
```

## Status

- Phases 1a-1e, 1g-1i: complete and live-tested against real Dhan/yfinance/Marketaux APIs.
- Phase 1f (scrapers): India VIX, GIFT Nifty, and FII/DII are stubbed with
  `NotImplementedError` in `connectors/scraper_*.py` -- these need manually-captured
  endpoints (NSE requires a Referer header + session cookie; see each stub's
  docstring). News sentiment via Marketaux works.
- Phase 1j (7-day soak test): not yet started. Deferred to avoid compounding the
  rate-limit incident below.

## Known issues / operational notes

- **Both Dhan accounts brushed rate limits during development testing** (HTTP 429,
  "further requests may result in the user being blocked") from cumulative test
  volume in one session. Not a code bug -- the retry/backoff/alert design handled
  it correctly. Start the real 7-day soak with a fresh rate-limit budget (e.g. next
  market session), and avoid running multiple ad-hoc test scripts back-to-back
  against the live option chain endpoint.
- Dhan's SDK response nesting is inconsistent across endpoints: optionchain/
  expirylist/quote responses are double-nested (`response["data"]["data"]`),
  historical chart responses are not (`response["data"]` directly). See comments
  in `connectors/dhan_account1.py` / `dhan_account2.py`.
- Several Dhan timestamp fields are IST wall-clock values mislabeled as if they
  were UTC/epoch (`last_trade_time` in marketfeed/quote, the websocket LTT epoch
  which is off from true UTC by exactly +5:30). Both are corrected in the
  connectors -- see the docstrings on `_parse_ltt` and `_PatchedMarketFeed.utc_time`.
- NIFTY (index) silently drops Full-mode (21) websocket subscriptions -- it has no
  order book. Use Quote mode (17) for indices; equities support Full mode fine.
