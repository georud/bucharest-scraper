from __future__ import annotations

import json
import logging
import math
from pathlib import Path

from ...config import AppConfig, DATA_DIR
from ...anti_detect.delays import AdaptiveDelay
from ...anti_detect.proxy import ProxyManager
from ...grid.generator import GridCell
from ...models.listing import Listing
from ..base import BaseScraper
from .parser import parse_pyairbnb_results, parse_raw_api_results, extract_pagination_cursor

logger = logging.getLogger(__name__)

RAW_DIR = DATA_DIR / "raw" / "airbnb"


def _zoom_from_bbox(bbox: dict) -> int:
    """Compute a Google Maps-style zoom level from a bounding box."""
    lng_span = abs(bbox["ne_lng"] - bbox["sw_lng"])
    if lng_span <= 0:
        return 15
    zoom = math.log2(360.0 / lng_span)
    return max(1, min(20, round(zoom)))


class AirbnbScraper(BaseScraper):
    """Airbnb scraper using pyairbnb library with fallback to Playwright."""

    def __init__(self, config: AppConfig, proxy_manager: ProxyManager | None = None):
        super().__init__(config)
        self.delay = AdaptiveDelay(config.scraping)
        self.proxy = proxy_manager or ProxyManager(config.proxy_urls)
        self._pyairbnb = None
        self._api_key: str | None = None
        self._search_hash: str | None = None

    async def init_session(self) -> None:
        """Initialize pyairbnb and prepare for scraping."""
        import asyncio
        from ..booking.graphql import get_dates

        RAW_DIR.mkdir(parents=True, exist_ok=True)

        if self.config.city.airbnb_use_dates:
            self._checkin, self._checkout = get_dates(
                self.config.city.checkin_offset_days,
                self.config.city.checkout_offset_days,
            )
        else:
            self._checkin = None
            self._checkout = None
            logger.info("Airbnb scraper using undated search for maximum listing coverage")

        try:
            import pyairbnb
            self._pyairbnb = pyairbnb
            logger.info("Airbnb scraper initialized with pyairbnb")

            # Cache API key and search hash at session level
            proxy = self.proxy.get_proxy()
            loop = asyncio.get_running_loop()
            self._api_key = await loop.run_in_executor(
                None, pyairbnb.api.get, proxy or ""
            )
            logger.info("Cached Airbnb API key")

            try:
                self._search_hash = await loop.run_in_executor(
                    None, pyairbnb.search.fetch_stays_search_hash, proxy or ""
                )
                logger.info("Cached fresh StaysSearch hash")
            except Exception as e:
                logger.warning("Could not fetch StaysSearch hash, using default: %s", e)
                self._search_hash = ""
        except ImportError:
            logger.warning("pyairbnb not installed, will use Playwright fallback only")

    async def scrape_cell(self, cell: GridCell) -> list[Listing]:
        """Scrape a single grid cell for Airbnb listings."""
        bbox = cell.bbox

        # Try pyairbnb first
        if self._pyairbnb:
            try:
                listings = await self._scrape_via_pyairbnb(cell)
                self.delay.on_success()
                return listings  # return even if empty — empty means no listings in cell
            except Exception as e:
                logger.warning("pyairbnb failed for cell %s: %s", cell.cell_id, e)
                self.delay.on_error()

        # Fallback to Playwright (only when pyairbnb is unavailable or errored)
        try:
            listings = await self._scrape_via_playwright(cell)
            self.delay.on_success()
            return listings
        except Exception as e:
            logger.error("Playwright scrape failed for cell %s: %s", cell.cell_id, e)
            self.delay.on_error()
            return []

    async def _scrape_via_pyairbnb(self, cell: GridCell) -> list[Listing]:
        """Use pyairbnb raw API with our own parser to avoid from_search() price bug."""
        import asyncio

        bbox = cell.bbox
        proxy = self.proxy.get_proxy()
        zoom = _zoom_from_bbox(bbox)
        checkin = self._checkin.isoformat() if self._checkin else ""
        checkout = self._checkout.isoformat() if self._checkout else ""

        api_key = self._api_key
        search_hash = self._search_hash or ""

        if not api_key:
            # Fallback: re-fetch key
            loop = asyncio.get_running_loop()
            api_key = await loop.run_in_executor(
                None, self._pyairbnb.api.get, proxy or ""
            )
            self._api_key = api_key

        all_listings: list[Listing] = []
        cursor = ""
        page = 0
        first_raw = None

        try:
            while True:
                loop = asyncio.get_running_loop()
                raw_json = await loop.run_in_executor(
                    None,
                    lambda c=cursor: self._pyairbnb.search.get(
                        api_key, c, checkin, checkout,
                        bbox["ne_lat"], bbox["ne_lng"], bbox["sw_lat"], bbox["sw_lng"],
                        zoom, "EUR", "", 0, 100000,
                        [], False, 0, 0, 0, 0, 0, 0,
                        "en", proxy or "", search_hash,
                    ),
                )

                if page == 0:
                    first_raw = raw_json

                listings = parse_raw_api_results(raw_json, cell.cell_id)
                all_listings.extend(listings)
                page += 1

                next_cursor = extract_pagination_cursor(raw_json)
                if not next_cursor or not listings:
                    break
                cursor = next_cursor

        except Exception as e:
            logger.warning(
                "Raw API parsing failed for cell %s (page %d): %s — falling back to search_all",
                cell.cell_id, page, e,
            )
            return await self._scrape_via_pyairbnb_fallback(cell)

        # Save first page raw for debugging
        if first_raw:
            self._save_raw(cell.cell_id, first_raw)

        logger.info("Cell %s: %d Airbnb listings via raw API (%d pages)", cell.cell_id, len(all_listings), page)
        return all_listings

    async def _scrape_via_pyairbnb_fallback(self, cell: GridCell) -> list[Listing]:
        """Fallback: use pyairbnb.search_all() with standardize.from_search()."""
        import asyncio

        bbox = cell.bbox
        proxy = self.proxy.get_proxy()

        def _search():
            return self._pyairbnb.search_all(
                check_in=self._checkin.isoformat() if self._checkin else "",
                check_out=self._checkout.isoformat() if self._checkout else "",
                ne_lat=bbox["ne_lat"],
                ne_long=bbox["ne_lng"],
                sw_lat=bbox["sw_lat"],
                sw_long=bbox["sw_lng"],
                zoom_value=_zoom_from_bbox(bbox),
                price_min=0,
                price_max=100000,
                currency="EUR",
                proxy_url=proxy or "",
            )

        results = await asyncio.get_running_loop().run_in_executor(None, _search)

        if not results:
            return []

        self._save_raw(cell.cell_id + "_fallback", results)

        listings = parse_pyairbnb_results(results, cell.cell_id)
        logger.info("Cell %s: %d Airbnb listings via pyairbnb fallback", cell.cell_id, len(listings))
        return listings

    async def _scrape_via_playwright(self, cell: GridCell) -> list[Listing]:
        """Fallback: use Playwright to intercept Airbnb API responses."""
        from playwright.async_api import async_playwright
        from .parser import parse_airbnb_results

        bbox = cell.bbox
        captured_data = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            )
            page = await context.new_page()

            async def handle_response(response):
                if "StaysSearch" in response.url:
                    try:
                        data = await response.json()
                        captured_data.append(data)
                    except Exception:
                        pass

            page.on("response", handle_response)

            url = (
                f"https://www.airbnb.com/s/Bucharest--Romania/homes"
                f"?ne_lat={bbox['ne_lat']}&ne_lng={bbox['ne_lng']}"
                f"&sw_lat={bbox['sw_lat']}&sw_lng={bbox['sw_lng']}"
                f"&zoom_level={_zoom_from_bbox(bbox)}&search_by_map=true"
            )
            if self._checkin and self._checkout:
                url += f"&checkin={self._checkin.isoformat()}&checkout={self._checkout.isoformat()}"

            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(3000)
            except Exception as e:
                logger.warning("Playwright navigation error: %s", e)
            finally:
                page.remove_listener("response", handle_response)
                await browser.close()

        all_listings = []
        for data in captured_data:
            listings = parse_airbnb_results(data, cell.cell_id)
            all_listings.extend(listings)

        logger.info("Cell %s: %d Airbnb listings via Playwright", cell.cell_id, len(all_listings))
        return all_listings

    def _save_raw(self, cell_id: str, data):
        """Save raw API response for debugging."""
        path = RAW_DIR / f"{cell_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    async def close(self) -> None:
        """Clean up resources."""
        logger.info("Airbnb scraper closed")
