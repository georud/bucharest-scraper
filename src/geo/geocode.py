from __future__ import annotations

import json
import logging
import time
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"


def normalize_address(address: str) -> str:
    """Cache key: lowercased, whitespace-collapsed."""
    return " ".join(address.lower().split())


def _http_fetch(query: str, base_url: str, user_agent: str, timeout: int) -> list[dict]:
    params = urllib.parse.urlencode(
        {"q": query, "format": "jsonv2", "limit": 1, "countrycodes": "ro"})
    req = urllib.request.Request(f"{base_url}?{params}", headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


class Geocoder:
    """Forward-geocode addresses via Nominatim with a DB-backed cache and
    persistent retry across runs. The HTTP call is injectable for testing."""

    def __init__(self, db, fetch_fn=None, base_url: str = _NOMINATIM_URL,
                 user_agent: str = "bucharest-str-research/1.0", rate_limit_s: float = 1.0,
                 timeout: int = 20, max_retries: int = 5):
        self.db = db
        self.base_url = base_url
        self.user_agent = user_agent
        self.rate_limit_s = rate_limit_s
        self.timeout = timeout
        self.max_retries = max_retries
        self._fetch_fn = fetch_fn or (
            lambda q: _http_fetch(q, self.base_url, self.user_agent, self.timeout))
        self._last_call = 0.0

    def _throttle(self):
        if self.rate_limit_s:
            wait = self.rate_limit_s - (time.monotonic() - self._last_call)
            if wait > 0:
                time.sleep(wait)
        self._last_call = time.monotonic()

    def geocode(self, address: str) -> tuple[float, float] | None:
        """Return (lat, lng) or None. Caches successes forever; failures are
        retried on each run until `max_retries` attempts accumulate."""
        key = normalize_address(address)
        cached = self.db.get_geocode(key)
        if cached:
            if cached["status"] == "ok":
                return (cached["latitude"], cached["longitude"])
            if cached["status"] == "not_found" or cached["attempts"] >= self.max_retries:
                return None
        attempts = (cached["attempts"] if cached else 0)

        self._throttle()
        try:
            results = self._fetch_fn(address)
        except Exception as e:  # network/timeout/parse — transient, retry next run
            logger.warning("Geocode failed for %r: %s", address, e)
            self.db.upsert_geocode(key, "failed", None, None, None, attempts + 1)
            return None

        if not results:
            self.db.upsert_geocode(key, "not_found", None, None, None, attempts + 1)
            return None
        top = results[0]
        lat, lng = float(top["lat"]), float(top["lon"])
        self.db.upsert_geocode(key, "ok", lat, lng, top.get("category"), attempts + 1)
        return (lat, lng)
