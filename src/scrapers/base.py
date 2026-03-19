from __future__ import annotations

import abc
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config import AppConfig
    from ..grid.generator import GridCell
    from ..models.listing import Listing

logger = logging.getLogger(__name__)


class BaseScraper(abc.ABC):
    """Abstract base class for platform scrapers."""

    def __init__(self, config: AppConfig):
        self.config = config

    @abc.abstractmethod
    async def init_session(self) -> None:
        """Initialize scraping session (browser, cookies, tokens)."""

    @abc.abstractmethod
    async def scrape_cell(self, cell: GridCell) -> list[Listing]:
        """Scrape all listings within a grid cell."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Clean up resources."""

    async def __aenter__(self):
        await self.init_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
