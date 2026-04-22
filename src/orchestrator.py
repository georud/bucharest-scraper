from __future__ import annotations

import asyncio
import logging
import random
import sys

from tqdm import tqdm

from .config import AppConfig, load_config
from .anti_detect.delays import AdaptiveDelay
from .anti_detect.proxy import ProxyManager
from .dedup.deduplicator import Deduplicator
from .grid.generator import GridCell, generate_grid, refine_cell, should_refine, _make_grid_cell
from .models.enums import Platform, ScrapeStatus
from .scrapers.booking.scraper import BookingScraper
from .scrapers.airbnb.scraper import AirbnbScraper
from .storage.database import Database
from .storage.exporter import export_csv, export_geojson
from .visualization.map_builder import build_map

logger = logging.getLogger(__name__)


class Orchestrator:
    """Main pipeline controller for the Bucharest scraping project."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.db = Database()
        self.dedup = Deduplicator()
        self.proxy = ProxyManager(config.proxy_urls)

    async def run(
        self,
        platforms: list[Platform] | None = None,
        sample_pct: float = 1.0,
        enrich_only: bool = False,
    ):
        """Run the full scraping pipeline.

        Args:
            platforms: Which platforms to scrape. Default: both.
            sample_pct: Fraction of cells to scrape (0.1 = 10% for testing).
            enrich_only: Skip scraping, only run enrichment passes.
        """
        platforms = platforms or [Platform.BOOKING, Platform.AIRBNB]

        # Step 1: Generate or recover grid
        if enrich_only:
            # Use cells that actually have listings in the DB
            import h3
            cell_ids = self.db.get_distinct_cell_ids()
            cells = [_make_grid_cell(cid, h3.get_resolution(cid)) for cid in cell_ids]
            logger.info("Enrich-only mode: found %d cells with listings in DB", len(cells))
        else:
            logger.info("Generating H3 grid...")
            cells = generate_grid(self.config.city)

            # Optional sampling for test runs
            if sample_pct < 1.0:
                total_cells = len(cells)
                sample_size = max(1, int(total_cells * sample_pct))
                cells = random.sample(cells, sample_size)
                logger.info("Sampling %d/%d cells (%.0f%%)", sample_size, total_cells, sample_pct * 100)

        # Randomize order to avoid geographic scanning patterns
        random.shuffle(cells)

        cell_map = {c.cell_id: c for c in cells}

        if not enrich_only:
            # Initialize progress tracking
            self.db.init_grid_progress([c.cell_id for c in cells], platforms)
            run_id = self.db.start_run(len(cells) * len(platforms))

            # Step 2: Scrape each platform
            for platform in platforms:
                logger.info("=== Scraping %s ===", platform.value.title())

                if platform == Platform.BOOKING:
                    await self._scrape_platform_booking(cells, cell_map, run_id)
                elif platform == Platform.AIRBNB:
                    await self._scrape_platform_airbnb(cells, cell_map, run_id)

            self.db.finish_run(run_id)

        # Step 3: Enrichment passes
        logger.info("=== Running enrichment passes ===")

        if Platform.BOOKING in platforms:
            try:
                await self._enrich_booking(cells)
            except Exception as e:
                logger.error("Booking enrichment failed: %s — continuing to export", e)

        if Platform.AIRBNB in platforms:
            try:
                await self._enrich_airbnb(cells)
            except Exception as e:
                logger.error("Airbnb enrichment failed: %s — continuing to export", e)

        # Step 4: Summary
        total_listings = self.db.get_listing_count()
        booking_count = self.db.get_listing_count(Platform.BOOKING)
        airbnb_count = self.db.get_listing_count(Platform.AIRBNB)

        logger.info("=== Complete ===")
        logger.info("Total listings: %d (Booking: %d, Airbnb: %d)", total_listings, booking_count, airbnb_count)

        # Step 5: Export
        logger.info("Exporting data...")
        csv_path = export_csv(self.db)
        geojson_path = export_geojson(self.db)
        map_path = build_map(self.db)

        logger.info("Exports: CSV=%s, GeoJSON=%s, Map=%s", csv_path, geojson_path, map_path)

        progress = self.db.get_progress_summary()
        logger.info("Progress summary: %s", progress)

    async def _scrape_platform_booking(
        self, cells: list[GridCell], cell_map: dict[str, GridCell], run_id: int
    ):
        """Scrape all cells for Booking.com with per-cell bounding-box queries."""
        scraper = BookingScraper(self.config, self.proxy)

        async with scraper:
            delay = AdaptiveDelay(self.config.scraping)
            pending = self.db.get_pending_cells(Platform.BOOKING)
            cells_to_scrape = [cell_map[cid] for cid in pending if cid in cell_map]

            pbar = tqdm(total=len(cells_to_scrape), desc="Booking.com", unit="cell")
            sem = asyncio.Semaphore(5)

            async def process_cell(cell):
                async with sem:
                    self.db.update_cell_status(cell.cell_id, Platform.BOOKING, ScrapeStatus.IN_PROGRESS)

                    try:
                        listings = await scraper.scrape_cell(cell)
                        raw_count = len(listings)
                        listings = self.dedup.deduplicate(listings)

                        if listings:
                            self.db.upsert_listings(listings)

                        result_count = len(listings)
                        status = ScrapeStatus.COMPLETED

                        if should_refine(
                            raw_count,
                            self.config.city.booking_results_cap,
                            self.config.city.refine_threshold,
                        ):
                            status = ScrapeStatus.NEEDS_REFINEMENT
                            logger.info(
                                "Cell %s hit cap: %d raw, %d after dedup — marking for refinement",
                                cell.cell_id, raw_count, result_count,
                            )

                        self.db.update_cell_status(cell.cell_id, Platform.BOOKING, status, result_count)

                        total = self.db.get_listing_count(Platform.BOOKING)
                        pbar.set_postfix(listings=total, cell_results=result_count)

                    except Exception as e:
                        logger.error("Failed to scrape Booking cell %s: %s", cell.cell_id, e)
                        self.db.update_cell_status(
                            cell.cell_id, Platform.BOOKING, ScrapeStatus.FAILED, error_message=str(e)
                        )

                    pbar.update(1)
                await delay.wait("booking")

            tasks = [process_cell(cell) for cell in cells_to_scrape]
            await asyncio.gather(*tasks)
            pbar.close()

            await self._refine_cells(scraper, Platform.BOOKING, cell_map)

    async def _scrape_platform_airbnb(
        self, cells: list[GridCell], cell_map: dict[str, GridCell], run_id: int
    ):
        """Scrape all cells for Airbnb."""
        scraper = AirbnbScraper(self.config, self.proxy)

        async with scraper:
            delay = AdaptiveDelay(self.config.scraping)
            pending = self.db.get_pending_cells(Platform.AIRBNB)
            cells_to_scrape = [cell_map[cid] for cid in pending if cid in cell_map]

            pbar = tqdm(total=len(cells_to_scrape), desc="Airbnb", unit="cell")
            sem = asyncio.Semaphore(3)

            async def process_cell(cell):
                async with sem:
                    self.db.update_cell_status(cell.cell_id, Platform.AIRBNB, ScrapeStatus.IN_PROGRESS)

                    try:
                        listings = await scraper.scrape_cell(cell)
                        raw_count = len(listings)
                        listings = self.dedup.deduplicate(listings)

                        if listings:
                            self.db.upsert_listings(listings)

                        result_count = len(listings)
                        status = ScrapeStatus.COMPLETED

                        if should_refine(
                            raw_count,
                            self.config.city.airbnb_results_cap,
                            self.config.city.refine_threshold,
                        ):
                            status = ScrapeStatus.NEEDS_REFINEMENT
                            logger.info(
                                "Cell %s hit cap: %d raw, %d after dedup — marking for refinement",
                                cell.cell_id, raw_count, result_count,
                            )

                        self.db.update_cell_status(cell.cell_id, Platform.AIRBNB, status, result_count)

                        total = self.db.get_listing_count(Platform.AIRBNB)
                        pbar.set_postfix(listings=total, cell_results=result_count)

                    except Exception as e:
                        logger.error("Failed to scrape Airbnb cell %s: %s", cell.cell_id, e)
                        self.db.update_cell_status(
                            cell.cell_id, Platform.AIRBNB, ScrapeStatus.FAILED, error_message=str(e)
                        )

                    pbar.update(1)
                await delay.wait("airbnb")

            tasks = [process_cell(cell) for cell in cells_to_scrape]
            await asyncio.gather(*tasks)
            pbar.close()

            await self._refine_cells(scraper, Platform.AIRBNB, cell_map)

    async def _enrich_booking(self, cells: list[GridCell]):
        """Run cascading dated enrichment passes for Booking to fill prices and room data."""
        ENRICHMENT_DATES = [
            # Near-term, dense (consecutive offsets sweep weekdays naturally)
            (3, 4),     # 3 days out
            (7, 8),     # 1 week
            (10, 11),
            (14, 15),   # 2 weeks
            (17, 18),
            (21, 22),   # 3 weeks
            (25, 26),
            (28, 29),
            (35, 36),   # 5 weeks
            (42, 43),   # 6 weeks
            # Mid-term
            (50, 51),
            (60, 61),   # 2 months
            (75, 76),
            (90, 91),   # 3 months
            (105, 106),
            (120, 121), # 4 months
            (135, 136),
            (150, 151), # 5 months
            (165, 166),
            (180, 181), # 6 months
            # Long-tail (catches seasonal / sparsely-listed properties)
            (210, 211), # 7 months
            (240, 241), # 8 months
            (270, 271), # 9 months
            (300, 301), # 10 months
            (330, 331), # 11 months
        ]

        cells_with_data = [c for c in cells if self.db.cell_has_listings(c.cell_id, Platform.BOOKING)]
        logger.info("Booking enrichment: re-querying %d cells (of %d total) with up to %d date windows...", len(cells_with_data), len(cells), len(ENRICHMENT_DATES))
        scraper = BookingScraper(self.config, self.proxy)

        async with scraper:
            for i, (ci, co) in enumerate(ENRICHMENT_DATES):
                enriched = await scraper.enrich_cells(cells_with_data, ci, co)
                if enriched:
                    count = self.db.enrich_listings(enriched)
                    logger.info("Booking enrichment pass %d/%d: updated %d listings", i + 1, len(ENRICHMENT_DATES), count)

                total = self.db.get_listing_count(Platform.BOOKING)
                missing = self.db.count_missing_prices(Platform.BOOKING)
                pct = (missing / total * 100) if total else 0
                logger.info("Pass %d/%d: %d/%d (%.1f%%) still missing prices", i + 1, len(ENRICHMENT_DATES), missing, total, pct)
                if pct < 5:
                    break

        # Phase 2: One Playwright pass over every listing needing detail-page data —
        # either business-disclosure check or a price retry (the detail page can
        # surface prices for listings the GraphQL date-sweep missed). Combine both
        # target sets and dedupe.
        if self.config.scraping.business_booking_enabled:
            biz_target = self.db.get_listings_missing_business_data(Platform.BOOKING)
            price_target = self.db.get_listings_missing_prices(Platform.BOOKING)
            by_id = {lst.id: lst for lst in biz_target}
            for lst in price_target:
                by_id.setdefault(lst.id, lst)
            target = list(by_id.values())
        else:
            target = self.db.get_listings_missing_prices(Platform.BOOKING)

        if target:
            logger.info(
                "Booking Phase 2: %d listings need detail-page enrichment (price + business data)",
                len(target),
            )
            scraper = BookingScraper(self.config, self.proxy)
            async with scraper:
                # Pass self.db for per-listing checkpointing so mid-batch browser
                # crashes don't discard hours of work.
                enriched = await scraper.enrich_listings(target, db=self.db)
                if enriched:
                    count = self.db.enrich_listings(enriched)
                    logger.info("Booking Phase 2: updated %d listings from detail pages", count)
        else:
            logger.info("Booking Phase 2: no listings need detail-page enrichment")

        final_missing = self.db.count_missing_prices(Platform.BOOKING)
        final_biz_missing = self.db.count_missing_business_data(Platform.BOOKING)
        final_total = self.db.get_listing_count(Platform.BOOKING)
        logger.info(
            "Booking enrichment complete: %d/%d missing price (%.1f%%), %d/%d missing business (%.1f%%)",
            final_missing, final_total, (final_missing / final_total * 100) if final_total else 0,
            final_biz_missing, final_total, (final_biz_missing / final_total * 100) if final_total else 0,
        )

    async def _enrich_airbnb(self, cells: list[GridCell]):
        """Run cascading dated passes for prices, then detail-page enrichment for room data."""
        ENRICHMENT_DATES = [
            (7, 8),     # 1 week out
            (14, 15),   # 2 weeks
            (21, 22),
            (28, 29),
            (35, 36),
            (45, 46),   # 6 weeks
            (60, 61),   # 2 months
            (75, 76),
            (90, 91),   # 3 months
            (120, 121), # 4 months
            (150, 151), # 5 months
            (180, 181), # 6 months
            (240, 241), # 8 months
            (300, 301), # 10 months
            (330, 331), # 11 months
        ]

        # Phase 1: Cascading date passes for prices
        missing = self.db.count_missing_prices(Platform.AIRBNB)
        total = self.db.get_listing_count(Platform.AIRBNB)
        if missing and total:
            pct = missing / total * 100
            logger.info("Airbnb price enrichment: %d/%d (%.1f%%) missing prices", missing, total, pct)

            if pct >= 5:
                cells_with_data = [c for c in cells if self.db.cell_has_listings(c.cell_id, Platform.AIRBNB)]
                logger.info("Airbnb enrichment: %d cells with data (of %d total)", len(cells_with_data), len(cells))
                scraper = AirbnbScraper(self.config, self.proxy)
                async with scraper:
                    for i, (ci, co) in enumerate(ENRICHMENT_DATES):
                        enriched = await scraper.enrich_cells(cells_with_data, ci, co)
                        if enriched:
                            count = self.db.enrich_listings(enriched)
                            logger.info("Airbnb price pass %d/%d: updated %d listings", i + 1, len(ENRICHMENT_DATES), count)

                        missing = self.db.count_missing_prices(Platform.AIRBNB)
                        pct = (missing / total * 100) if total else 0
                        logger.info("Pass %d/%d: %d/%d (%.1f%%) still missing prices", i + 1, len(ENRICHMENT_DATES), missing, total, pct)
                        if pct < 5:
                            break

        # Phase 2: Detail-page enrichment for room data
        missing_data = self.db.get_listings_missing_data(Platform.AIRBNB)
        if missing_data:
            logger.info("Airbnb enrichment: %d listings missing room data", len(missing_data))
            scraper = AirbnbScraper(self.config, self.proxy)

            async with scraper:
                enriched = await scraper.enrich_listings(missing_data)
                if enriched:
                    count = self.db.enrich_listings(enriched)
                    logger.info("Airbnb enrichment: updated %d listings", count)
                else:
                    logger.info("Airbnb enrichment: no listings enriched")
        else:
            logger.info("Airbnb enrichment: all listings already have room data")

        # Phase 3: Business / host disclosure via Playwright 'Learn more' modal.
        if self.config.scraping.business_airbnb_enabled:
            biz_missing = self.db.get_listings_missing_business_data(Platform.AIRBNB)
            if biz_missing:
                logger.info(
                    "Airbnb Phase 3: %d listings missing business data — opening Learn more modals",
                    len(biz_missing),
                )
                scraper = AirbnbScraper(self.config, self.proxy)
                async with scraper:
                    # Pass self.db for per-listing checkpointing — guards against
                    # mid-batch browser crashes discarding hours of work.
                    enriched = await scraper.enrich_business_data(biz_missing, db=self.db)
                    if enriched:
                        # Final catch-up write (checkpointing already persisted most).
                        count = self.db.enrich_listings(enriched)
                        logger.info("Airbnb Phase 3: updated %d listings with business data", count)

    async def _refine_cells(self, scraper, platform: Platform, cell_map: dict[str, GridCell]):
        """Recursively subdivide and re-scrape cells that hit the result cap.

        Uses BFS queue so all res-N cells are processed before res-(N+1).
        """
        initial_ids = self.db.get_cells_needing_refinement(platform)
        if not initial_ids:
            return

        cap = (
            self.config.city.booking_results_cap
            if platform == Platform.BOOKING
            else self.config.city.airbnb_results_cap
        )
        max_res = self.config.city.max_refine_resolution

        # Build initial queue from cells needing refinement
        from collections import deque
        queue: deque[GridCell] = deque()
        for cell_id in initial_ids:
            if cell_id in cell_map:
                queue.append(cell_map[cell_id])

        logger.info(
            "Refining %d dense %s cells (max resolution %d)...",
            len(queue), platform.value, max_res,
        )
        delay = AdaptiveDelay(self.config.scraping)
        sem = asyncio.Semaphore(5)

        while queue:
            parent_cell = queue.popleft()
            next_res = parent_cell.resolution + 1

            if next_res > max_res:
                logger.warning(
                    "Cell %s at res %d hit cap but max_refine_resolution (%d) reached — skipping",
                    parent_cell.cell_id, parent_cell.resolution, max_res,
                )
                self.db.update_cell_status(parent_cell.cell_id, platform, ScrapeStatus.COMPLETED)
                continue

            sub_cells = refine_cell(parent_cell, next_res)

            sub_ok = 0

            async def scrape_sub(sub_cell):
                nonlocal sub_ok
                async with sem:
                    try:
                        listings = await scraper.scrape_cell(sub_cell)
                        raw_count = len(listings)
                        listings = self.dedup.deduplicate(listings)
                        if listings:
                            self.db.upsert_listings(listings)

                        logger.info(
                            "Refined sub-cell %s (res %d): %d raw, %d after dedup",
                            sub_cell.cell_id, sub_cell.resolution, raw_count, len(listings),
                        )
                        sub_ok += 1

                        if should_refine(raw_count, cap, self.config.city.refine_threshold):
                            cell_map[sub_cell.cell_id] = sub_cell
                            queue.append(sub_cell)
                            logger.info(
                                "Sub-cell %s also hit cap (%d) — queued for refinement to res %d",
                                sub_cell.cell_id, raw_count, sub_cell.resolution + 1,
                            )

                    except Exception as e:
                        logger.error("Failed to scrape refined cell %s: %s", sub_cell.cell_id, e)
                await delay.wait(platform.value)

            tasks = [scrape_sub(sc) for sc in sub_cells]
            await asyncio.gather(*tasks)

            # Mark parent based on sub-cell outcomes
            if sub_ok > 0:
                self.db.update_cell_status(parent_cell.cell_id, platform, ScrapeStatus.COMPLETED)
            else:
                self.db.update_cell_status(
                    parent_cell.cell_id, platform, ScrapeStatus.FAILED,
                    error_message="All sub-cells failed during refinement",
                )


def main():
    """Entry point for the scraper."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("scraper.log", encoding="utf-8"),
        ],
    )

    config = load_config()
    orchestrator = Orchestrator(config)

    # Parse CLI args
    sample_pct = 1.0
    platforms = None
    enrich_only = False

    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--sample" and i + 1 < len(args):
            sample_pct = float(args[i + 1])
        elif arg == "--booking-only":
            platforms = [Platform.BOOKING]
        elif arg == "--airbnb-only":
            platforms = [Platform.AIRBNB]
        elif arg == "--enrich-only":
            enrich_only = True

    try:
        asyncio.run(orchestrator.run(platforms=platforms, sample_pct=sample_pct, enrich_only=enrich_only))
    finally:
        orchestrator.db.close()


if __name__ == "__main__":
    main()
