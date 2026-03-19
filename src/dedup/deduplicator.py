from __future__ import annotations

import logging
import math

from rapidfuzz import fuzz

from ..models.listing import Listing

logger = logging.getLogger(__name__)

# Earth radius in meters
EARTH_RADIUS_M = 6_371_000


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in meters using Haversine formula."""
    lat1_r, lat2_r = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return EARTH_RADIUS_M * c


class Deduplicator:
    """Deduplicates listings by platform ID and spatial proximity."""

    def __init__(self, distance_threshold_m: float = 50.0, name_similarity_threshold: float = 70.0):
        self.distance_threshold = distance_threshold_m
        self.name_threshold = name_similarity_threshold
        self._seen_ids: set[str] = set()

    def deduplicate(self, listings: list[Listing]) -> list[Listing]:
        """Remove duplicates from a batch of listings.

        Primary: exact platform ID match.
        Secondary: spatial proximity + fuzzy name match (for cross-cell duplicates).
        """
        unique = []

        for listing in listings:
            # Primary dedup: exact ID
            if listing.id in self._seen_ids:
                continue

            # Secondary dedup: spatial + name
            is_dup = False
            for existing in unique:
                if existing.platform == listing.platform:
                    continue

                dist = haversine_distance(
                    existing.latitude, existing.longitude,
                    listing.latitude, listing.longitude,
                )

                if dist < self.distance_threshold:
                    name_sim = fuzz.ratio(existing.name.lower(), listing.name.lower())
                    if name_sim >= self.name_threshold:
                        is_dup = True
                        break

            if not is_dup:
                unique.append(listing)
                self._seen_ids.add(listing.id)

        removed = len(listings) - len(unique)
        if removed > 0:
            logger.debug("Deduplication removed %d/%d listings", removed, len(listings))

        return unique

    def find_cross_platform_matches(
        self, listings: list[Listing], distance_m: float = 100.0, name_threshold: float = 60.0
    ) -> list[tuple[Listing, Listing]]:
        """Find listings that appear on both Booking and Airbnb."""
        from ..models.enums import Platform

        booking = [l for l in listings if l.platform == Platform.BOOKING]
        airbnb = [l for l in listings if l.platform == Platform.AIRBNB]

        matches = []
        for b in booking:
            for a in airbnb:
                dist = haversine_distance(b.latitude, b.longitude, a.latitude, a.longitude)
                if dist < distance_m:
                    name_sim = fuzz.ratio(b.name.lower(), a.name.lower())
                    if name_sim >= name_threshold:
                        matches.append((b, a))

        logger.info("Found %d cross-platform matches", len(matches))
        return matches

    def reset(self):
        """Reset seen IDs for a fresh dedup pass."""
        self._seen_ids.clear()
