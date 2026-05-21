from __future__ import annotations

import math
from dataclasses import dataclass

# Local equirectangular projection anchored on central Bucharest.
_LAT0, _LNG0 = 44.4325, 26.1000
_DEG_LAT_M = 111_320.0
_DEG_LNG_M = 111_320.0 * math.cos(math.radians(_LAT0))  # ~79,545 m

_OUTLIER_M = 1000.0


@dataclass
class Observation:
    listing_id: str
    latitude: float
    longitude: float
    sigma_m: float
    source: str  # 'scraped' | 'geocoded'


@dataclass
class FusedPosition:
    latitude: float
    longitude: float
    sigma_m: float
    dominant_listing_id: str
    dominant_source: str


def _to_local(lat: float, lng: float) -> tuple[float, float]:
    return ((lng - _LNG0) * _DEG_LNG_M, (lat - _LAT0) * _DEG_LAT_M)


def _to_geo(x: float, y: float) -> tuple[float, float]:
    return (_LAT0 + y / _DEG_LAT_M, _LNG0 + x / _DEG_LNG_M)


def _weighted_mean(obs: list[Observation]) -> tuple[float, float, float]:
    sw = sum(1.0 / (o.sigma_m ** 2) for o in obs)
    x = sum((1.0 / o.sigma_m ** 2) * _to_local(o.latitude, o.longitude)[0] for o in obs) / sw
    y = sum((1.0 / o.sigma_m ** 2) * _to_local(o.latitude, o.longitude)[1] for o in obs) / sw
    sigma = 1.0 / math.sqrt(sw)
    return x, y, sigma


def fuse_observations(observations: list[Observation], outlier_m: float = _OUTLIER_M) -> FusedPosition:
    """Inverse-variance weighted fusion with >1 km outlier rejection.

    Outlier filter anchors on the highest-precision (smallest sigma) point so
    that a single distant observation cannot drag the initial mean far enough
    to exclude the genuine cluster.
    """
    obs = [o for o in observations if o.sigma_m and o.sigma_m > 0]
    if not obs:
        raise ValueError("no usable observations")

    anchor = min(obs, key=lambda o: o.sigma_m)
    ax, ay = _to_local(anchor.latitude, anchor.longitude)
    kept = [o for o in obs
            if math.dist((ax, ay), _to_local(o.latitude, o.longitude)) <= outlier_m]
    if not kept:
        kept = obs
    x, y, sigma = _weighted_mean(kept)
    lat, lng = _to_geo(x, y)

    dominant = min(kept, key=lambda o: o.sigma_m)  # smallest sigma == largest weight
    return FusedPosition(lat, lng, sigma, dominant.listing_id, dominant.source)


def position_confidence(sigma_m: float) -> float:
    """Map a fused sigma to a 0-1 confidence (1 at 0 m, 0 at >=150 m)."""
    return max(0.0, min(1.0, (150.0 - sigma_m) / 150.0))
