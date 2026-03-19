from shapely.geometry import Polygon

# Bucharest bounding polygon (slightly larger than strict city limits)
BUCHAREST_BOUNDS = {
    "north": 44.55,
    "south": 44.35,
    "east": 26.22,
    "west": 25.92,
}

# Approximate Bucharest polygon (rectangular for simplicity)
BUCHAREST_POLYGON = Polygon([
    (BUCHAREST_BOUNDS["west"], BUCHAREST_BOUNDS["south"]),
    (BUCHAREST_BOUNDS["east"], BUCHAREST_BOUNDS["south"]),
    (BUCHAREST_BOUNDS["east"], BUCHAREST_BOUNDS["north"]),
    (BUCHAREST_BOUNDS["west"], BUCHAREST_BOUNDS["north"]),
])

# Known dense areas that may need resolution 8 refinement
DENSE_AREAS = [
    {"name": "Centru Vechi", "lat": 44.4325, "lng": 26.1005},
    {"name": "Unirii", "lat": 44.4268, "lng": 26.1025},
    {"name": "Universitate", "lat": 44.4356, "lng": 26.1001},
    {"name": "Victoriei", "lat": 44.4530, "lng": 26.0857},
]
