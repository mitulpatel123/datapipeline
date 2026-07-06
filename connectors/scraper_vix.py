"""India VIX scraper -- BLOCKED pending Section 2 manual research.

nseindia.com loads VIX via a background XHR that requires a Referer header and a
session cookie fetched from the homepage first. Per spec: do not guess this endpoint.
Fill in the URL/headers/response shape once captured via Chrome DevTools Network tab.
"""


def fetch_india_vix() -> dict:
    raise NotImplementedError("waiting on manually-sourced endpoint for India VIX (spec section 2)")
