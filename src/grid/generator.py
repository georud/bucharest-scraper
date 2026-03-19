from __future__ import annotations

import logging
from dataclasses import dataclass

import h3
from shapely.geometry import Polygon

from ..config import CityConfig

logger = logging.getLogger(__name__)


@dataclass
class GridCell:
    cell_id: str
    resolution: int
    boundary: list[tuple[float, float]]  # List of (lat, lng) vertices
    bbox: dict  # {ne_lat, ne_lng, sw_lat, sw_lng}
    center_lat: float
    center_lng: float


def generate_grid(config: CityConfig) -> list[GridCell]:
    """Generate H3 hexagonal grid cells covering the city bounds."""
    bounds = config.bounds

    # Create polygon from bounds (H3 expects lat/lng tuples)
    polygon = h3.LatLngPoly(
        [
            (bounds.north, bounds.west),
            (bounds.north, bounds.east),
            (bounds.south, bounds.east),
            (bounds.south, bounds.west),
        ]
    )

    cell_ids = h3.polygon_to_cells(polygon, config.h3_resolution)
    cells = [_make_grid_cell(cid, config.h3_resolution) for cid in cell_ids]

    logger.info(
        "Generated %d grid cells at resolution %d",
        len(cells),
        config.h3_resolution,
    )
    return cells


def refine_cell(cell: GridCell, target_resolution: int) -> list[GridCell]:
    """Subdivide a cell into higher-resolution children for dense areas."""
    children = h3.cell_to_children(cell.cell_id, target_resolution)
    refined = [_make_grid_cell(cid, target_resolution) for cid in children]
    logger.info(
        "Refined cell %s into %d sub-cells at resolution %d",
        cell.cell_id,
        len(refined),
        target_resolution,
    )
    return refined


def should_refine(result_count: int, cap: int, threshold: float) -> bool:
    """Check if a cell needs refinement based on result count vs cap."""
    return result_count >= cap * threshold


def _make_grid_cell(cell_id: str, resolution: int) -> GridCell:
    """Convert an H3 cell ID into a GridCell with bounding box."""
    boundary = h3.cell_to_boundary(cell_id)  # List of (lat, lng)
    center = h3.cell_to_latlng(cell_id)

    lats = [p[0] for p in boundary]
    lngs = [p[1] for p in boundary]

    bbox = {
        "ne_lat": max(lats),
        "ne_lng": max(lngs),
        "sw_lat": min(lats),
        "sw_lng": min(lngs),
    }

    return GridCell(
        cell_id=cell_id,
        resolution=resolution,
        boundary=list(boundary),
        bbox=bbox,
        center_lat=center[0],
        center_lng=center[1],
    )
