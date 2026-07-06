"""FII/DII cash + F&O participant OI scraper -- BLOCKED pending Section 2 manual research.

nseindia.com, same cookie/Referer pattern as India VIX. Per spec: do not guess this
endpoint. Fill in once captured via Chrome DevTools Network tab.
"""


def fetch_fii_dii() -> dict:
    raise NotImplementedError("waiting on manually-sourced endpoint for FII/DII data (spec section 2)")
