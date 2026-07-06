# Nifty 50 Data Pipeline -- Phase 1

Read-only data collection layer for the Nifty 50 options trading system. This phase
collects, validates, and stores option chain, tick, historical, global market, and
news data. No trading/order logic, no ML, no LLM calls -- see the spec for phase
boundaries.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in credentials

# Start Redis/Postgres first
brew services start redis
brew services start postgresql@16
createdb nifty_data   # first time only

python scripts/test_connections.py     # verify Redis + Postgres
python scripts/init_db.py              # create tables (new install)
python scripts/migrate_phase1_schema.py  # apply schema changes (existing install -- safe to re-run any time)
```

If Redis/Postgres aren't installed at all:

```bash
brew install redis postgresql@16
```

If you already have an older local database and don't want to run the migration script for
any reason, the guaranteed-clean alternative is a full local reset (**destroys all data
collected so far**):

```bash
dropdb nifty_data
createdb nifty_data
python scripts/init_db.py
```

## Running

```bash
SOAK_MODE=safe python orchestrator.py
```

Runs one `BlockingScheduler` process (Asia/Kolkata timezone, regardless of the host
machine's local timezone) with every job: option chain, websocket ticks (NIFTY, 5
heavyweight stocks, nearest futures contract, and -- when
`ENABLE_OPTION_WEBSOCKET_UNIVERSE=true` -- a live ATM±N option universe), quote
reconciliation, derived analytics, global indices, news, gap watchdog, EOD
historical backfill, instrument master refresh.

> **Do not run multiple orchestrator processes at the same time.** The repo has a
> Redis process lock (a second instance exits immediately with an alert, and if a
> running instance ever loses lock ownership it hard-exits rather than keep
> polling), but you should still avoid starting two terminals on purpose --
> double-running the orchestrator risks double-polling Dhan and tripping HTTP 429 /
> temporary throttling before the lock even has a chance to react.

**WebSocket is the primary live/tick feed.** REST option chain is a heavy snapshot
path (Greeks/OI/full chain state) that runs on its own limited, buffered cadence.
Market quote REST is reconciliation/backup only -- it cross-checks the websocket
feed and must never be called from a second, uncoordinated job (see
`connectors/dhan_request_manager.py`: every Dhan REST call goes through one shared
rate limiter/circuit breaker per account, so nothing can bypass this by accident).

**Account 1** is used for live websocket + the active NIFTY option chain.
**Account 2** is historical/reference/failover only -- it must not duplicate-poll
Account 1's live endpoints; it exists so historical backfill keeps running even if
Account 1 gets throttled.

**Every Dhan REST call is rate-limited, circuit-broken, and logged** through
`connectors/dhan_request_manager.py`: an account-level token bucket, per-endpoint-
family minimum intervals, and a breaker that trips on any 429/soft-ban signal and
escalates (60s cooldown -> 15min reduced speed -> on a 2nd hit, 5min cooldown +
non-critical REST disabled -> on a 3rd hit, all REST disabled until the next
process start). It does not retry into a detected rate limit.

Cadence is controlled by `SOAK_MODE` in `.env` (or as an env var override, as above):

| SOAK_MODE | option chain | quote reconciliation |
|---|---|---|
| `safe` (default) | every 6s | every 15s |
| `normal` | every 4s | every 10s |
| `production` | every 3.3s | every 5-10s |

WebSocket stays live in every mode -- only the REST polling cadence changes. **Do
not start directly in `production` mode** -- always ramp safe -> normal -> production
(see the soak test plan below).

Daily report (run after market close, or pass `--date YYYY-MM-DD`):

```bash
./venv/bin/python scripts/generate_daily_report.py
```

Writes both a Markdown report and a JSON summary to `logs/`, with a PASS/WARN/FAIL
verdict (FAIL on any 429 while in normal/production mode; WARN on >1% missing
cycles on a critical stream, a critical-severity gap, or a 429 while in safe mode).

## 7-Day Soak Test Plan

Do not start directly in production mode, and always run a 1-hour dry run on
`SOAK_MODE=safe` before committing to a full trading day, let alone the full 7-day run.

```bash
# 1. Activate environment
source venv/bin/activate

# 2. Start Redis/Postgres first
brew services start redis
brew services start postgresql@16

# 3. Initialize or migrate DB
python scripts/init_db.py
python scripts/migrate_phase1_schema.py

# 4. Run a 1-hour dry run first, on safe mode, before trusting a full day to it
SOAK_MODE=safe python orchestrator.py
# ... let it run for about an hour during market hours, then Ctrl-C ...

# 5. Generate a report to sanity-check the dry run
python scripts/generate_daily_report.py --date YYYY-MM-DD
```

Only once the 1-hour dry run's report looks sane (no unexplained 429s, no critical
gaps, option chain/websocket rows actually landing), start the real multi-day plan:

- **Day 1 -- `SOAK_MODE=safe`**: option chain every 6s, quote reconciliation every
  15s, websocket on, no ad-hoc Dhan scripts running alongside the orchestrator.
- **Day 2 -- `SOAK_MODE=normal`**: option chain every 4s, quote reconciliation
  every 10s.
- **Days 3-7 -- `SOAK_MODE=production`**: option chain every 3.3s, quote
  reconciliation every 5-10s, websocket primary, Account 2 only for
  historical/EOD/failover.

After each trading day: `python scripts/generate_daily_report.py --date YYYY-MM-DD`.

**Ready for Phase 2 only if all 7 daily reports show:** zero Dhan 429/rate-limit
incidents in production-like mode, no orchestrator duplicate-lock violations,
websocket downtime under 2 minutes/day (or clearly explained), option-chain actual
cycles >=98% of expected during market hours, critical tick streams >=98% expected
health, no unexplained validation-reject spikes, no duplicate OHLCV bars after
reruns, and a PASS or only minor WARNs.

## Filling in the blocked scrapers (Section 2)

`connectors/scraper_vix.py`, `scraper_gift_nifty.py`, and `scraper_fii_dii.py` are
stubbed with `NotImplementedError` -- these are deliberately not guessed. To unblock
them:

1. Open the target site in Chrome -> right-click -> **Inspect** -> **Network** tab
   -> filter **Fetch/XHR** -> reload the page.
2. Find the request returning the JSON with the number shown on screen.
3. Copy the **Request URL**, method, and required headers (NSE typically needs a
   `Referer` header and a session cookie fetched from the homepage first).
4. Copy a sample JSON response so the field names are known.
5. Hand those details over and the corresponding `connectors/scraper_*.py` stub can
   be implemented against the real endpoint.

The gap watchdog does not require these -- they're simply absent from its watch
list until implemented and scheduled, so their absence never falsely fires a gap.

## Status

- Phases 1a-1i: complete and live-tested against real Dhan/yfinance/Marketaux APIs.
- Hardening pass (rate-limit/ban-risk fixes, orchestrator lock, websocket option
  universe, gap watchdog rewrite, migrations, data-quality flags, daily report
  rewrite, test suite): complete -- see git history for the detailed HARDEN-* commits.
- Phase 1f (scrapers): India VIX, GIFT Nifty, and FII/DII remain stubbed pending
  Section 2 endpoints (see above). News sentiment via Marketaux works.
- Phase 1j (7-day soak test): not yet started. Start on `SOAK_MODE=safe` at the
  next market session per the validation plan above.

## Tests

```bash
./venv/bin/python -m compileall .
./venv/bin/pytest -q
```

90 tests, all offline (no live Dhan calls or credentials needed) but exercising
real local Postgres/Redis: rate limiter, circuit breaker escalation (including SDK
exceptions), expiry selection, IST time helpers (including the futures-contract
date fix), gap watchdog (including the expanded futures/heavyweight keys and
optional-stream handling), derived metrics, OHLCV dedup, schema migration
idempotency, `.env.example` consistency, and daily report verdict logic.

## Known issues / operational notes

- **Both Dhan accounts brushed rate limits during development testing** (HTTP 429,
  "further requests may result in the user being blocked") from cumulative test
  volume in one session -- this is what motivated the hardening pass (centralized
  request manager, circuit breaker, orchestrator lock, SOAK_MODE). Even so, avoid
  running multiple ad-hoc test scripts back-to-back against the live option chain
  endpoint outside the orchestrator.
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
- `scripts/init_db.py`'s `create_all()` only creates missing tables, it never alters
  an existing table's columns/indexes. Run `scripts/migrate_phase1_schema.py` after
  pulling any change to `storage/postgres_models.py` -- it's safe to run any number
  of times (checks Postgres's own catalog before acting) and warns instead of
  crashing if it finds duplicate news URLs blocking the unique index.
