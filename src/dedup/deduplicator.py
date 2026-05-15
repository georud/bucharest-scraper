from __future__ import annotations

import hashlib
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
                if existing.platform != listing.platform:
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
        self, listings: list[Listing], distance_m: float = 100.0, name_threshold: float = 72.0
    ) -> list[tuple[Listing, Listing, float]]:
        """Find candidate Booking↔Airbnb pairs that may be the same property.

        Returns `(booking_listing, airbnb_listing, name_similarity)` tuples — a
        *candidate* list, not a final grouping. A single listing may appear in
        several tuples; `assign_cross_platform_groups` resolves that into clean
        1:1 pairs.

        Best-effort only: Airbnb deliberately fuzzes listing coordinates (~150 m),
        so this misses pairs whose Airbnb point drifted beyond `distance_m` and
        can mis-suggest two different units in the same building. Name similarity
        is the corroborating signal. See METHODOLOGY.md → Unit of analysis.

        Coordinate-bucketed (~110 m cells, 3×3 neighbourhood scan) rather than the
        O(booking×airbnb) brute force, so it runs in roughly linear time.
        """
        from ..models.enums import Platform

        booking = [l for l in listings if l.platform == Platform.BOOKING]
        airbnb = [l for l in listings if l.platform == Platform.AIRBNB]

        # Bucket Airbnb listings by ~110 m grid cell (round to 3 decimal degrees).
        buckets: dict[tuple[float, float], list[Listing]] = {}
        for a in airbnb:
            key = (round(a.latitude, 3), round(a.longitude, 3))
            buckets.setdefault(key, []).append(a)

        matches: list[tuple[Listing, Listing, float]] = []
        for b in booking:
            blat, blng = round(b.latitude, 3), round(b.longitude, 3)
            seen_for_b: set[str] = set()
            for dlat in (-0.001, 0.0, 0.001):
                for dlng in (-0.001, 0.0, 0.001):
                    key = (round(blat + dlat, 3), round(blng + dlng, 3))
                    for a in buckets.get(key, ()):
                        if a.id in seen_for_b:
                            continue
                        dist = haversine_distance(
                            b.latitude, b.longitude, a.latitude, a.longitude
                        )
                        if dist < distance_m:
                            name_sim = fuzz.ratio(b.name.lower(), a.name.lower())
                            if name_sim >= name_threshold:
                                matches.append((b, a, name_sim))
                                seen_for_b.add(a.id)

        logger.info("Found %d cross-platform candidate pairs", len(matches))
        return matches

    def assign_cross_platform_groups(
        self, listings: list[Listing], distance_m: float = 100.0, name_threshold: float = 72.0
    ) -> dict[str, str]:
        """Assign a shared `cross_platform_group_id` to a Booking listing and the
        single Airbnb listing that is most likely the same physical property.

        Uses **greedy 1:1 matching**, NOT transitive grouping: candidate pairs
        are sorted by name similarity (best first) and a pair is accepted only
        if neither listing is already matched. Every resulting group is therefore
        exactly one Booking + one Airbnb listing — no chains. This deliberately
        under-merges (a flat listed twice on one platform only gets its single
        best cross-platform pair linked) rather than risk the transitive
        over-merge that union-find produced. The group id is a stable hash of the
        two member ids.

        Returns {listing_id: group_id}; only matched listings appear in the map.
        """
        candidates = self.find_cross_platform_matches(listings, distance_m, name_threshold)
        if not candidates:
            return {}

        # Greedy: best name match first, accept only if both ends are still free.
        candidates.sort(key=lambda t: t[2], reverse=True)
        used: set[str] = set()
        mapping: dict[str, str] = {}
        pairs = 0
        for b, a, _sim in candidates:
            if b.id in used or a.id in used:
                continue
            digest = hashlib.md5("|".join(sorted((b.id, a.id))).encode()).hexdigest()[:16]
            group_id = f"xpg_{digest}"
            mapping[b.id] = group_id
            mapping[a.id] = group_id
            used.add(b.id)
            used.add(a.id)
            pairs += 1

        logger.info(
            "Cross-platform linking: %d listings linked as %d 1:1 pairs",
            len(mapping), pairs,
        )
        return mapping

    def reset(self):
        """Reset seen IDs for a fresh dedup pass."""
        self._seen_ids.clear()
