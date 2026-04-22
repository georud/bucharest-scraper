from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from datetime import date, timedelta
from pathlib import Path

from tqdm import tqdm

from ...config import AppConfig, DATA_DIR
from ...anti_detect.delays import AdaptiveDelay
from ...anti_detect.proxy import ProxyManager
from ...grid.generator import GridCell
from ...models.listing import Listing
from ..base import BaseScraper
from .parser import parse_pyairbnb_results, parse_raw_api_results, extract_pagination_cursor, parse_detail_response, parse_business_modal, parse_airbnb_business_from_html

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
        except ImportError:
            logger.warning("pyairbnb not installed, will use Playwright fallback only")
            return

        # Try to cache the API key + search hash for search phases. Failures
        # here (rate limits, network) are non-fatal — Phase 3 business
        # enrichment uses Playwright and doesn't need either.
        proxy = self.proxy.get_proxy()
        loop = asyncio.get_running_loop()
        try:
            self._api_key = await loop.run_in_executor(None, self._pyairbnb.api.get, proxy or "")
            logger.info("Cached Airbnb API key")
        except Exception as e:
            logger.warning("Could not fetch pyairbnb API key (non-fatal): %s", e)

        try:
            self._search_hash = await loop.run_in_executor(
                None, self._pyairbnb.search.fetch_stays_search_hash, proxy or ""
            )
            logger.info("Cached fresh StaysSearch hash")
        except Exception as e:
            logger.warning("Could not fetch StaysSearch hash, using default: %s", e)
            self._search_hash = ""

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

    async def enrich_cells(self, cells: list[GridCell], checkin_offset: int = 14, checkout_offset: int = 15) -> list[Listing]:
        """Re-query cells with dates to fill in prices for undated listings."""
        today = date.today()
        orig_checkin, orig_checkout = self._checkin, self._checkout
        self._checkin = today + timedelta(days=checkin_offset)
        self._checkout = today + timedelta(days=checkout_offset)

        all_listings: list[Listing] = []
        sem = asyncio.Semaphore(3)
        pbar = tqdm(total=len(cells), desc="Airbnb enrich", unit="cell")

        async def enrich_one(cell) -> list[Listing]:
            cell_listings: list[Listing] = []
            async with sem:
                try:
                    listings = await self.scrape_cell(cell)
                    for lst in listings:
                        cell_listings.append(lst)
                except Exception as e:
                    logger.warning("Airbnb enrich failed for cell %s: %s", cell.cell_id, e)
            await self.delay.wait("airbnb")
            pbar.update(1)
            return cell_listings

        try:
            results = await asyncio.gather(*[enrich_one(cell) for cell in cells])
            seen_ids: set[str] = set()
            for batch in results:
                for lst in batch:
                    if lst.id not in seen_ids:
                        all_listings.append(lst)
                        seen_ids.add(lst.id)
        finally:
            pbar.close()
            self._checkin, self._checkout = orig_checkin, orig_checkout

        return all_listings

    async def enrich_business_data(self, listings: list[Listing], db=None) -> list[Listing]:
        """Fetch per-listing business disclosure via Playwright 'Learn more' modal.

        One browser is launched for the whole batch; a new page is used per
        listing. Listings without a Learn-more link get `business_type="Individual"`
        so they're not retried on subsequent runs.

        If `db` is provided, each successfully-fetched listing is written back
        to the DB immediately (checkpointing). This way a mid-batch crash
        (e.g. browser connection lost after hours of work) doesn't discard
        everything that succeeded.
        """
        if not self.config.scraping.business_airbnb_enabled:
            logger.info("Airbnb business-data enrichment disabled by config")
            return []
        if not listings:
            return []

        from playwright.async_api import async_playwright

        timeout_ms = max(5, self.config.scraping.business_airbnb_timeout) * 1000
        concurrency = max(1, self.config.scraping.business_airbnb_concurrency)

        # Browser restart cadence — under the ~2,700-listing crash ceiling we've seen.
        BATCH_SIZE = 500

        logger.info(
            "Airbnb business enrichment: %d listings via Playwright (batches of %d)",
            len(listings), BATCH_SIZE,
        )
        enriched: list[Listing] = []
        pbar = tqdm(total=len(listings), desc="Airbnb business", unit="listing")

        try:
            for chunk_start in range(0, len(listings), BATCH_SIZE):
                chunk = listings[chunk_start:chunk_start + BATCH_SIZE]
                batch_num = chunk_start // BATCH_SIZE + 1
                total_batches = (len(listings) + BATCH_SIZE - 1) // BATCH_SIZE
                logger.info(
                    "Airbnb business batch %d/%d: %d listings (fresh browser)",
                    batch_num, total_batches, len(chunk),
                )

                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    try:
                        context = await browser.new_context(
                            viewport={"width": 1440, "height": 900},
                            user_agent=(
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/131.0.0.0 Safari/537.36"
                            ),
                            locale="en-US",
                        )
                        sem = asyncio.Semaphore(concurrency)

                        async def _one(lst: Listing) -> Listing | None:
                            async with sem:
                                try:
                                    fields = await self._fetch_business_data(context, lst.url, timeout_ms)
                                except Exception as e:
                                    logger.debug("Business modal fetch failed for %s: %s", lst.platform_id, e)
                                    fields = None
                                pbar.update(1)

                                if fields is None:
                                    return None

                                updated = False
                                for key in (
                                    "business_name", "business_registration_number", "business_vat",
                                    "business_address", "business_email", "business_phone",
                                    "business_country", "business_trade_register_name",
                                    "host_name", "host_id", "host_response_rate",
                                    "host_response_time", "host_join_date",
                                ):
                                    if fields.get(key) is not None:
                                        setattr(lst, key, fields[key])
                                        updated = True
                                # Always record business_type so the listing isn't re-tried every run
                                lst.business_type = fields.get("business_type") or "Unknown"

                                # Checkpoint to DB immediately (resilient against mid-batch crashes)
                                if db is not None:
                                    try:
                                        db.enrich_listings([lst])
                                    except Exception as e:
                                        logger.warning("DB checkpoint failed for %s: %s", lst.platform_id, e)

                                return lst if (updated or lst.business_type) else None

                        tasks = [_one(lst) for lst in chunk]
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for r in results:
                            if isinstance(r, Listing):
                                enriched.append(r)
                    finally:
                        try:
                            await browser.close()
                        except Exception as e:
                            logger.warning("Playwright browser close failed (non-fatal): %s", e)
        finally:
            pbar.close()

        logger.info(
            "Airbnb business enrichment: updated %d/%d listings with business_type classification",
            len(enriched), len(listings),
        )
        return enriched

    async def _fetch_business_data(self, context, url: str, timeout_ms: int) -> dict | None:
        """Fetch Airbnb listing page + extract DSA `businessDetails` from Apollo state.

        Airbnb embeds the DSA disclosure directly in the page's serialized state
        as `"businessDetails":null` (individual host) or `"businessDetails":{...}`
        (professional). We parse it out of the rendered HTML rather than clicking
        any "Learn more" button — the latter is unreliable (Airbnb has several
        "Learn more" triggers, most of which open the host profile popover,
        not DSA business details).
        """
        page = await context.new_page()
        try:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            except Exception as e:
                logger.debug("Navigation failed for %s: %s", url, e)
                return None

            # Wait for the Apollo state to be serialized into the DOM.
            try:
                await page.wait_for_function(
                    "() => document.documentElement.innerHTML.indexOf('\"businessDetails\"') !== -1",
                    timeout=15_000,
                )
            except Exception:
                # Fall through — we'll still try to parse whatever rendered.
                pass

            html = await page.content()
            return parse_airbnb_business_from_html(html)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def enrich_listings(self, listings: list[Listing]) -> list[Listing]:
        """Fetch detail pages to fill in room data for listings missing it.

        Uses pyairbnb.get_details() with a concurrency semaphore.
        """
        if not self._pyairbnb:
            logger.warning("pyairbnb not available, cannot enrich Airbnb listings")
            return []

        to_enrich = [
            lst for lst in listings
            if lst.beds is None and lst.bathrooms is None and lst.max_guests is None
        ]
        if not to_enrich:
            logger.info("No Airbnb listings need enrichment")
            return []

        logger.info("Enriching %d Airbnb listings with detail data...", len(to_enrich))
        sem = asyncio.Semaphore(5)
        enriched: list[Listing] = []
        pbar = tqdm(total=len(to_enrich), desc="Airbnb details", unit="listing")

        async def _fetch_detail(lst: Listing) -> Listing | None:
            async with sem:
                try:
                    proxy = self.proxy.get_proxy()
                    loop = asyncio.get_running_loop()
                    detail = await loop.run_in_executor(
                        None,
                        lambda: self._pyairbnb.get_details(
                            room_id=int(lst.platform_id),
                            currency="EUR",
                            proxy_url=proxy or "",
                        ),
                    )
                    if not detail:
                        pbar.update(1)
                        return None

                    fields = parse_detail_response(detail)
                    lst.max_guests = fields["max_guests"]
                    lst.is_superhost = fields["is_superhost"]
                    lst.bedrooms = fields["bedrooms"]
                    lst.beds = fields["beds"]
                    lst.bathrooms = fields["bathrooms"]

                    await self.delay.wait("airbnb_enrich")
                    pbar.update(1)
                    return lst
                except Exception as e:
                    logger.debug("Failed to enrich Airbnb listing %s: %s", lst.platform_id, e)
                    pbar.update(1)
                    return None

        tasks = [_fetch_detail(lst) for lst in to_enrich]
        results = await asyncio.gather(*tasks)
        pbar.close()

        for result in results:
            if result is not None:
                enriched.append(result)

        logger.info("Enriched %d/%d Airbnb listings", len(enriched), len(to_enrich))
        return enriched

    def _save_raw(self, cell_id: str, data):
        """Save raw API response for debugging."""
        path = RAW_DIR / f"{cell_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    async def close(self) -> None:
        """Clean up resources."""
        logger.info("Airbnb scraper closed")
