from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from tqdm import tqdm

from curl_cffi.requests import Session

from ...config import AppConfig, DATA_DIR
from ...anti_detect.delays import AdaptiveDelay
from ...anti_detect.proxy import ProxyManager
from ...grid.generator import GridCell
from ...models.listing import Listing
from ..base import BaseScraper
from .graphql import get_dates
from .parser import parse_graphql_results, parse_property_page

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://www.booking.com/dml/graphql"
RAW_DIR = DATA_DIR / "raw" / "booking"
RESULTS_PER_PAGE = 25

# Minimal FullSearch query that returns coordinates, prices, and reviews.
SEARCH_QUERY = """
query FullSearch($input: SearchQueryInput!) {
  searchQueries {
    search(input: $input) {
      ... on SearchQueryOutput {
        pagination {
          nbResultsPerPage
          nbResultsTotal
        }
        results {
          ... on SearchResultProperty {
            basicPropertyData {
              id
              accommodationTypeId
              pageName
              location {
                address
                city
                latitude
                longitude
              }
              starRating { value }
              reviews { totalScore reviewsCount }
              photos {
                main {
                  highResJpegUrl { relativeUrl }
                }
              }
            }
            displayName { text }
            matchingUnitConfigurations {
              commonConfiguration {
                nbBedrooms
                nbBathrooms
                nbAllBeds
              }
            }
            blocks {
              finalPrice { amount currency }
            }
          }
        }
      }
    }
  }
}
"""


class BookingScraper(BaseScraper):
    """Booking.com scraper using direct GraphQL via curl_cffi.

    Strategy: per-cell bounding-box FullSearch queries, paginating within
    each cell.  Dense cells are refined by the orchestrator.
    """

    def __init__(self, config: AppConfig, proxy_manager: ProxyManager | None = None):
        super().__init__(config)
        self.delay = AdaptiveDelay(config.scraping)
        self.proxy = proxy_manager or ProxyManager(config.proxy_urls)
        self._http: Session | None = None
        self._checkin: date | None = None
        self._checkout: date | None = None

    async def init_session(self) -> None:
        """Prepare HTTP client."""
        if self.config.city.booking_use_dates:
            self._checkin, self._checkout = get_dates(
                self.config.city.checkin_offset_days,
                self.config.city.checkout_offset_days,
            )
        else:
            self._checkin = None
            self._checkout = None

        self._http = Session(
            impersonate=self.config.scraping.curl_impersonate,
            timeout=self.config.scraping.curl_timeout,
        )

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(
            "Booking scraper initialized (checkin=%s, checkout=%s)",
            self._checkin, self._checkout,
        )

    async def scrape_cell(self, cell: GridCell) -> list[Listing]:
        """Paginate through FullSearch results for this cell's bounding box."""
        bbox = cell.bbox
        all_listings: list[Listing] = []
        seen_ids: set[str] = set()
        offset = 0
        total_results = None
        consecutive_empty = 0

        while True:
            payload = {
                "operationName": "FullSearch",
                "variables": self._build_variables(bbox, offset),
                "query": SEARCH_QUERY,
            }

            response = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda p=payload: self._http.post(
                    GRAPHQL_URL,
                    json=p,
                    headers=self._headers(),
                    proxies=self.proxy.get_curl_proxy(),
                ),
            )

            if response.status_code == 429:
                self.delay.on_rate_limit()
                logger.warning("Rate limited at offset %d, stopping cell", offset)
                break

            if response.status_code != 200:
                logger.warning("HTTP %d at offset %d", response.status_code, offset)
                self.delay.on_error()
                break

            try:
                data = response.json()
            except (ValueError, Exception) as e:
                logger.warning("Malformed JSON at offset %d: %s", offset, e)
                self.delay.on_error()
                break

            if "errors" in data:
                logger.warning("GraphQL errors at offset %d: %s", offset, data["errors"][0].get("message", ""))
                break

            try:
                search = data["data"]["searchQueries"]["search"]
                results = search.get("results", [])
                pag = search.get("pagination", {})
            except (KeyError, TypeError):
                logger.warning("Unexpected response structure at offset %d", offset)
                break

            if total_results is None:
                total_results = pag.get("nbResultsTotal", 0)

            if not results:
                break

            listings = parse_graphql_results(results, self.config.city.booking_country_code, cell.cell_id)
            new_count = 0
            for lst in listings:
                if lst.id not in seen_ids:
                    all_listings.append(lst)
                    seen_ids.add(lst.id)
                    new_count += 1

            if new_count == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
            else:
                consecutive_empty = 0

            self.delay.on_success()
            offset += RESULTS_PER_PAGE

            if total_results and offset >= total_results:
                break

            await self.delay.wait("booking")

        return all_listings

    async def enrich_cells(self, cells: list[GridCell], checkin_offset: int = 14, checkout_offset: int = 15) -> list[Listing]:
        """Re-query cells with dates to fill in prices and room data.

        Uses a dated search regardless of the original use_dates config,
        since undated searches return empty blocks and null
        matchingUnitConfigurations.
        """
        today = date.today()
        checkin = today + timedelta(days=checkin_offset)
        checkout = today + timedelta(days=checkout_offset)

        all_listings: list[Listing] = []
        sem = asyncio.Semaphore(5)
        pbar = tqdm(total=len(cells), desc="Booking enrich", unit="cell")

        async def enrich_one(cell) -> list[Listing]:
            cell_listings: list[Listing] = []
            async with sem:
                bbox = cell.bbox
                offset = 0
                seen_ids: set[str] = set()
                consecutive_empty = 0

                while True:
                    variables = self._build_variables(bbox, offset)
                    variables["input"]["dates"] = {
                        "checkin": checkin.isoformat(),
                        "checkout": checkout.isoformat(),
                    }

                    payload = {
                        "operationName": "FullSearch",
                        "variables": variables,
                        "query": SEARCH_QUERY,
                    }

                    response = await asyncio.get_running_loop().run_in_executor(
                        None,
                        lambda p=payload: self._http.post(
                            GRAPHQL_URL,
                            json=p,
                            headers=self._headers(),
                            proxies=self.proxy.get_curl_proxy(),
                        ),
                    )

                    if response.status_code == 429:
                        self.delay.on_rate_limit()
                        logger.warning("Rate limited during enrichment, stopping cell %s", cell.cell_id)
                        break

                    if response.status_code != 200:
                        self.delay.on_error()
                        logger.warning("Enrichment HTTP %d for cell %s", response.status_code, cell.cell_id)
                        break

                    try:
                        data = response.json()
                    except (ValueError, Exception) as e:
                        logger.warning("Malformed JSON during enrichment for cell %s: %s", cell.cell_id, e)
                        self.delay.on_error()
                        break

                    if "errors" in data:
                        logger.warning("Enrichment GraphQL errors for cell %s: %s", cell.cell_id, data["errors"][:200] if isinstance(data["errors"], str) else str(data["errors"])[:200])
                        break

                    try:
                        search = data["data"]["searchQueries"]["search"]
                        results = search.get("results", [])
                        pag = search.get("pagination", {})
                    except (KeyError, TypeError):
                        break

                    if not results:
                        break

                    listings = parse_graphql_results(results, self.config.city.booking_country_code, cell.cell_id)
                    new_count = 0
                    for lst in listings:
                        if lst.id not in seen_ids:
                            cell_listings.append(lst)
                            seen_ids.add(lst.id)
                            new_count += 1

                    if new_count == 0:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            break
                    else:
                        consecutive_empty = 0

                    self.delay.on_success()
                    offset += RESULTS_PER_PAGE

                    total = pag.get("nbResultsTotal", 0)
                    if total and offset >= total:
                        break

                    await self.delay.wait("booking_enrich")
            await self.delay.wait("booking_enrich")
            pbar.update(1)
            return cell_listings

        results = await asyncio.gather(*[enrich_one(cell) for cell in cells])
        pbar.close()
        for batch in results:
            all_listings.extend(batch)

        logger.info("Booking enrichment: %d listings with prices/room data", len(all_listings))
        return all_listings

    async def enrich_listings(self, listings: list[Listing], checkin_offset: int = 180, checkout_offset: int = 181, db=None) -> list[Listing]:
        """Fetch individual property pages via Playwright to extract price, room, and business data.

        Booking serves an AWS WAF JS challenge on detail pages — curl_cffi can't execute it
        (every request returns 202 with an interstitial). A real browser is required.
        One headless Chromium is launched for the whole batch; pages are processed concurrently.
        """
        if not listings:
            return []

        from playwright.async_api import async_playwright

        today = date.today()
        checkin = (today + timedelta(days=checkin_offset)).isoformat()
        checkout = (today + timedelta(days=checkout_offset)).isoformat()

        concurrency = 3
        nav_timeout_ms = 25_000
        enriched: list[Listing] = []
        pbar = tqdm(total=len(listings), desc="Booking details", unit="listing")

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
                # Cut bandwidth: drop images / fonts / stylesheets we don't need.
                async def _route(route):
                    if route.request.resource_type in ("image", "media", "font", "stylesheet"):
                        await route.abort()
                    else:
                        await route.continue_()
                await context.route("**/*", _route)

                sem = asyncio.Semaphore(concurrency)

                async def _fetch_one(lst: Listing) -> Listing | None:
                    async with sem:
                        url = f"{lst.url}?checkin={checkin}&checkout={checkout}"
                        page = await context.new_page()
                        try:
                            try:
                                await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout_ms)
                            except Exception as e:
                                logger.debug("Booking detail nav failed for %s: %s", lst.platform_id, e)
                                return None

                            # Wait for the AWS WAF challenge to finish + actual content to render.
                            # The DSA traderInfo block is what we really want; wait for it explicitly.
                            try:
                                await page.wait_for_function(
                                    "() => document.documentElement.innerHTML.indexOf('\"traderInfo\"') !== -1"
                                    " || document.documentElement.innerHTML.indexOf('\"isTrader\"') !== -1",
                                    timeout=20_000,
                                )
                            except Exception:
                                # Fallback: at least wait for property header / JSON-LD
                                try:
                                    await page.wait_for_function(
                                        "() => document.querySelector('script[type=\"application/ld+json\"]') !== null"
                                        " || document.querySelector('[data-testid=\"property-header\"]') !== null",
                                        timeout=8_000,
                                    )
                                except Exception:
                                    pass

                            # Brief settle for JS to inject Apollo state
                            await page.wait_for_timeout(500)
                            html = await page.content()
                            fields = parse_property_page(html)

                            if fields["price_per_night"] is not None and lst.price_per_night is None:
                                lst.price_per_night = fields["price_per_night"]
                                lst.currency = fields["currency"] or "EUR"
                            for room_field in ("bedrooms", "beds", "bathrooms", "max_guests"):
                                if fields[room_field] is not None and getattr(lst, room_field) is None:
                                    setattr(lst, room_field, fields[room_field])

                            business_hit = False
                            for key in (
                                "business_name", "business_registration_number", "business_vat",
                                "business_address", "business_email", "business_phone",
                                "business_country", "business_trade_register_name",
                            ):
                                if fields[key] is not None:
                                    setattr(lst, key, fields[key])
                                    business_hit = True
                            if fields["business_type"] is not None:
                                lst.business_type = fields["business_type"]
                            elif business_hit:
                                lst.business_type = lst.business_type or "Professional"
                            else:
                                # Page fetched successfully but no legal info — mark "Private"
                                # so the row stops being re-checked.
                                lst.business_type = lst.business_type or "Private"

                            # Checkpoint to DB immediately (resilient against mid-batch crashes)
                            if db is not None:
                                try:
                                    db.enrich_listings([lst])
                                except Exception as e:
                                    logger.warning("DB checkpoint failed for %s: %s", lst.platform_id, e)

                            return lst
                        except Exception as e:
                            logger.debug("Failed to enrich Booking listing %s: %s", lst.platform_id, e)
                            return None
                        finally:
                            try:
                                await page.close()
                            except Exception:
                                pass
                            pbar.update(1)

                tasks = [_fetch_one(lst) for lst in listings]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Listing):
                        enriched.append(r)
            finally:
                pbar.close()
                try:
                    await browser.close()
                except Exception as e:
                    logger.warning("Playwright browser close failed (non-fatal): %s", e)

        with_price = sum(1 for l in enriched if l.price_per_night is not None)
        with_biz = sum(1 for l in enriched if l.business_name is not None)
        logger.info(
            "Booking detail enrichment: %d/%d listings parsed (priced=%d, biz_named=%d)",
            len(enriched), len(listings), with_price, with_biz,
        )
        return enriched

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _detail_headers(self) -> dict:
        return {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        }

    def _build_variables(self, bbox: dict, offset: int = 0) -> dict:
        city = self.config.city
        input_vars: dict = {
            "location": {
                "destType": "BOUNDING_BOX",
                "boundingBox": {
                    "neLat": bbox["ne_lat"],
                    "neLon": bbox["ne_lng"],
                    "swLat": bbox["sw_lat"],
                    "swLon": bbox["sw_lng"],
                    "precision": 1,
                },
                "initialDestination": {
                    "destType": city.booking_dest_type.upper(),
                    "destId": int(city.booking_dest_id),
                },
            },
            "nbAdults": city.adults,
            "nbRooms": city.rooms,
            "nbChildren": 0,
            "pagination": {
                "rowsPerPage": RESULTS_PER_PAGE,
                "offset": offset,
            },
            "filters": {},
        }
        if self._checkin and self._checkout:
            input_vars["dates"] = {
                "checkin": self._checkin.isoformat(),
                "checkout": self._checkout.isoformat(),
            }
        return {"input": input_vars}

    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Origin": "https://www.booking.com",
            "Referer": "https://www.booking.com/searchresults.html",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        }

    async def close(self) -> None:
        if self._http:
            self._http.close()
        logger.info("Booking scraper closed")
