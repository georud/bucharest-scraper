from src.scrapers.airbnb.parser import extract_amenities, extract_map_radius


def test_extract_map_radius():
    assert extract_map_radius('foo "mapMarkerRadiusInMeters":0,"bar":1') == 0.0
    assert extract_map_radius('x "mapMarkerRadiusInMeters": 152 y') == 152.0
    assert extract_map_radius('"mapMarkerRadiusInMeters":500.0,') == 500.0
    assert extract_map_radius("no such tag here") is None
    assert extract_map_radius(None) is None
    assert extract_map_radius("") is None


def test_extract_amenities():
    html = ('x{"__typename":"AmenityItem","available":true,"title":"Wifi","icon":"A"}'
            '{"__typename":"AmenityItem","available":true,"title":"Kitchen","icon":"B"}'
            '{"__typename":"AmenityItem","available":false,"title":"Pool","icon":"C"}y')
    assert extract_amenities(html) == ["kitchen", "wifi"]   # available only, normalized, sorted
    assert extract_amenities("no amenities here") == []
    assert extract_amenities(None) == []
