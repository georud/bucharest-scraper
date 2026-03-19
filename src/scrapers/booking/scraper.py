from __future__ import annotations

import asyncio
import logging
from datetime import date

from curl_cffi.requests import Session

from ...config import AppConfig, DATA_DIR
from ...anti_detect.delays import AdaptiveDelay
from ...anti_detect.proxy import ProxyManager
from ...grid.generator import GridCell
from ...models.listing import Listing
from ..base import BaseScraper
from .graphql import get_dates
from .parser import parse_graphql_results

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
                lambda: self._http.post(
                    GRAPHQL_URL,
                    json=payload,
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

            data = response.json()

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

            listings = parse_graphql_results(results, self.config.city.booking_country_code)
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

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

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
