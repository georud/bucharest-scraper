from __future__ import annotations

import logging
import re

from playwright.async_api import async_playwright, Browser, BrowserContext, Page

logger = logging.getLogger(__name__)

BOOKING_URL = "https://www.booking.com"


class BookingSession:
    """Manages Booking.com session: browser, CSRF token, cookies."""

    def __init__(self):
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self.csrf_token: str | None = None
        self.cookies: list[dict] = []
        self._request_count = 0

    async def bootstrap(self, search_url: str | None = None) -> None:
        """Launch browser, navigate to Booking.com, extract CSRF token and cookies.

        Args:
            search_url: Full search URL with dates. Falls back to homepage.
        """
        logger.info("Bootstrapping Booking.com session via Playwright...")

        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        self._page = await self._context.new_page()

        # Navigate to search page — need networkidle for JS to populate full HTML
        url = search_url or BOOKING_URL
        await self._page.goto(url, wait_until="networkidle", timeout=60000)

        # Try to dismiss cookie consent
        try:
            accept_btn = self._page.locator('[id="onetrust-accept-btn-handler"]')
            if await accept_btn.is_visible(timeout=3000):
                await accept_btn.click()
        except Exception:
            pass

        # Extract CSRF token from page source
        content = await self._page.content()
        self.csrf_token = self._extract_csrf(content)

        if self.csrf_token:
            logger.info("CSRF token obtained: %s...", self.csrf_token[:20])
        else:
            logger.warning("Could not extract CSRF token from page")

        # Capture cookies
        self.cookies = await self._context.cookies()
        logger.info("Captured %d cookies", len(self.cookies))

    def _extract_csrf(self, html: str) -> str | None:
        """Extract CSRF token from Booking.com page HTML."""
        patterns = [
            r"b_csrf_token:\s*'([^']+)'",
            r'"csrf_token":\s*"([^"]+)"',
            r'name="csrf_token"\s+content="([^"]+)"',
            r"X-Booking-CSRF['\"]?\s*[:=]\s*['\"]([^'\"]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                return match.group(1)
        return None

    async def refresh_session(self) -> None:
        """Refresh CSRF token by reloading the page."""
        logger.info("Refreshing Booking.com session...")
        if self._page:
            await self._page.reload(wait_until="domcontentloaded", timeout=30000)
            content = await self._page.content()
            new_token = self._extract_csrf(content)
            if new_token:
                self.csrf_token = new_token
                logger.info("CSRF token refreshed: %s...", self.csrf_token[:20])
            self.cookies = await self._context.cookies()
        self._request_count = 0

    def get_cookie_header(self) -> str:
        """Format cookies as a header string for curl_cffi."""
        return "; ".join(f"{c['name']}={c['value']}" for c in self.cookies)

    def increment_request_count(self) -> int:
        self._request_count += 1
        return self._request_count

    @property
    def request_count(self) -> int:
        return self._request_count

    @property
    def page(self) -> Page | None:
        return self._page

    @property
    def context(self) -> BrowserContext | None:
        return self._context

    async def close(self) -> None:
        """Clean up browser resources."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        self._browser = None
        self._playwright = None
        logger.info("Booking.com session closed")
