from __future__ import annotations

import logging
import random

logger = logging.getLogger(__name__)


class ProxyManager:
    """Manages proxy rotation for scraping requests."""

    def __init__(self, proxy_urls: list[str] | None = None):
        self.proxies = proxy_urls or []
        self._current_index = 0
        self._failed: set[str] = set()

    @property
    def enabled(self) -> bool:
        return len(self.proxies) > 0

    def get_proxy(self) -> str | None:
        """Get the next proxy URL, skipping failed ones."""
        if not self.proxies:
            return None

        available = [p for p in self.proxies if p not in self._failed]
        if not available:
            logger.warning("All proxies failed, resetting failure list")
            self._failed.clear()
            available = self.proxies

        proxy = available[self._current_index % len(available)]
        self._current_index += 1
        return proxy

    def get_random_proxy(self) -> str | None:
        """Get a random proxy, skipping failed ones."""
        if not self.proxies:
            return None

        available = [p for p in self.proxies if p not in self._failed]
        if not available:
            self._failed.clear()
            available = self.proxies

        return random.choice(available)

    def mark_failed(self, proxy_url: str):
        """Mark a proxy as failed."""
        self._failed.add(proxy_url)
        logger.warning("Proxy marked as failed: %s", proxy_url[:30] + "...")

    def mark_success(self, proxy_url: str):
        """Remove a proxy from the failed list on success."""
        self._failed.discard(proxy_url)

    def get_curl_proxy(self) -> dict | None:
        """Get proxy dict for curl_cffi."""
        proxy = self.get_proxy()
        if proxy:
            return {"http": proxy, "https": proxy}
        return None

    def get_playwright_proxy(self) -> dict | None:
        """Get proxy dict for Playwright."""
        proxy = self.get_proxy()
        if proxy:
            return {"server": proxy}
        return None
