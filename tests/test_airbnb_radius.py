from src.scrapers.airbnb.parser import extract_map_radius


def test_extract_map_radius():
    assert extract_map_radius('foo "mapMarkerRadiusInMeters":0,"bar":1') == 0.0
    assert extract_map_radius('x "mapMarkerRadiusInMeters": 152 y') == 152.0
    assert extract_map_radius('"mapMarkerRadiusInMeters":500.0,') == 500.0
    assert extract_map_radius("no such tag here") is None
    assert extract_map_radius(None) is None
    assert extract_map_radius("") is None
