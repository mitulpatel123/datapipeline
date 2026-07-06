import logging

import requests

from config import settings

logger = logging.getLogger(__name__)


def send_telegram_alert(message: str) -> bool:
    if not settings.TELEGRAM_BOT_TOKEN or not settings.TELEGRAM_CHAT_ID:
        logger.warning("Telegram not configured, dropping alert: %s", message)
        return False

    url = f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(
            url,
            json={"chat_id": settings.TELEGRAM_CHAT_ID, "text": message},
            timeout=10,
        )
        response.raise_for_status()
        return True
    except Exception:
        logger.exception("Failed to send Telegram alert")
        return False
