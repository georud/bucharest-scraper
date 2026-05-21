from src.config import load_config


def test_curation_config_loads_with_defaults():
    cfg = load_config()
    assert cfg.geocoding.enabled in (True, False)
    assert cfg.geocoding.rate_limit_s >= 0
    assert cfg.dedup.strict_distance_m == 100
    assert cfg.fusion.sigma_geocoded_m == 25
