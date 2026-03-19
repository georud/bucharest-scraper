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
from .grid.generator import GridCell, generate_grid, refine_cell, should_refine
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

    async def run(self, platforms: list[Platform] | None = None, sample_pct: float = 1.0):
        """Run the full scraping pipeline.

        Args:
            platforms: Which platforms to scrape. Default: both.
            sample_pct: Fraction of cells to scrape (0.1 = 10% for testing).
        """
        platforms = platforms or [Platform.BOOKING, Platform.AIRBNB]

        # Step 1: Generate grid
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

        # Initialize progress tracking
        self.db.init_grid_progress([c.cell_id for c in cells], platforms)
        run_id = self.db.start_run(len(cells) * len(platforms))

        cell_map = {c.cell_id: c for c in cells}
        completed_cells = 0
        total_listings = 0

        # Step 2: Scrape each platform
        for platform in platforms:
            logger.info("=== Scraping %s ===", platform.value.title())

            if platform == Platform.BOOKING:
                await self._scrape_platform_booking(cells, cell_map, run_id)
            elif platform == Platform.AIRBNB:
                await self._scrape_platform_airbnb(cells, cell_map, run_id)

        # Step 3: Summary
        total_listings = self.db.get_listing_count()
        booking_count = self.db.get_listing_count(Platform.BOOKING)
        airbnb_count = self.db.get_listing_count(Platform.AIRBNB)

        logger.info("=== Scraping Complete ===")
        logger.info("Total listings: %d (Booking: %d, Airbnb: %d)", total_listings, booking_count, airbnb_count)

        # Step 4: Export
        logger.info("Exporting data...")
        csv_path = export_csv(self.db)
        geojson_path = export_geojson(self.db)
        map_path = build_map(self.db)

        logger.info("Exports: CSV=%s, GeoJSON=%s, Map=%s", csv_path, geojson_path, map_path)

        self.db.finish_run(run_id)
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

            pbar = tqdm(cells_to_scrape, desc="Booking.com", unit="cell")
            for cell in pbar:
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

                await delay.wait("booking")

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

            pbar = tqdm(cells_to_scrape, desc="Airbnb", unit="cell")
            for cell in pbar:
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

                await delay.wait("airbnb")

            await self._refine_cells(scraper, Platform.AIRBNB, cell_map)

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

            for sub_cell in sub_cells:
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

                    # If sub-cell also hits cap, add to queue for further refinement
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

            # Mark parent as completed after all sub-cells done
            self.db.update_cell_status(parent_cell.cell_id, platform, ScrapeStatus.COMPLETED)


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

    # Parse CLI args for sample percentage
    sample_pct = 1.0
    platforms = None

    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg == "--sample" and i + 1 < len(args):
            sample_pct = float(args[i + 1])
        elif arg == "--booking-only":
            platforms = [Platform.BOOKING]
        elif arg == "--airbnb-only":
            platforms = [Platform.AIRBNB]

    try:
        asyncio.run(orchestrator.run(platforms=platforms, sample_pct=sample_pct))
    finally:
        orchestrator.db.close()


if __name__ == "__main__":
    main()
