from __future__ import annotations

import asyncio
import logging
import random

from ..config import ScrapingConfig

logger = logging.getLogger(__name__)


class AdaptiveDelay:
    """Manages delays between requests with exponential backoff and human-like pauses."""

    def __init__(self, config: ScrapingConfig):
        self.config = config
        self._consecutive_errors = 0

    async def wait(self, platform: str = "booking"):
        """Wait an appropriate delay before the next request."""
        if platform == "booking":
            base = random.uniform(self.config.booking_delay_min, self.config.booking_delay_max)
        else:
            base = random.uniform(self.config.airbnb_delay_min, self.config.airbnb_delay_max)

        # Apply backoff if we've had errors
        if self._consecutive_errors > 0:
            backoff = min(
                self.config.backoff_base ** self._consecutive_errors,
                self.config.backoff_max,
            )
            delay = base + backoff
            logger.debug("Backoff delay: %.1fs (errors: %d)", delay, self._consecutive_errors)
        else:
            delay = base

        # Random chance of a longer "human break"
        if random.random() < self.config.human_break_chance:
            pause = random.uniform(self.config.human_break_min, self.config.human_break_max)
            delay += pause
            logger.info("Human break: %.1fs", pause)

        # Add small jitter
        delay += random.uniform(0.1, 0.5)

        await asyncio.sleep(delay)

    def on_success(self):
        """Reset error counter on successful request."""
        self._consecutive_errors = 0

    def on_error(self):
        """Increment error counter for backoff calculation."""
        self._consecutive_errors += 1

    def on_rate_limit(self):
        """Significant backoff on rate limit."""
        self._consecutive_errors += 2

    @property
    def current_backoff(self) -> float:
        if self._consecutive_errors == 0:
            return 0
        return min(
            self.config.backoff_base ** self._consecutive_errors,
            self.config.backoff_max,
        )
