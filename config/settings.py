import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

DHAN_CLIENT_ID_1 = os.getenv("DHAN_CLIENT_ID_1")
DHAN_ACCESS_TOKEN_1 = os.getenv("DHAN_ACCESS_TOKEN_1")
DHAN_CLIENT_ID_2 = os.getenv("DHAN_CLIENT_ID_2") or None
DHAN_ACCESS_TOKEN_2 = os.getenv("DHAN_ACCESS_TOKEN_2") or None
DHAN_ACCOUNT_2_ENABLED = bool(DHAN_CLIENT_ID_2 and DHAN_ACCESS_TOKEN_2)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://localhost:5432/nifty_data")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY") or None

# Dhan global rate limit: 5 requests/sec TOTAL per account, shared across every
# endpoint type (option chain, quotes, historical, etc.) -- not 5 per endpoint.
DHAN_MAX_REQUESTS_PER_SECOND = 5
DHAN_OPTIONCHAIN_MIN_INTERVAL_SECONDS = 3  # 1 unique optionchain request per 3s
DHAN_MARKETFEED_MIN_INTERVAL_SECONDS = 1

IST = "Asia/Kolkata"
MARKET_OPEN = "09:00"
MARKET_CLOSE = "15:35"

LOG_DIR = BASE_DIR / "logs"
