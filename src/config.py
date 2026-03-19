from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data"


@dataclass
class Bounds:
    north: float
    south: float
    east: float
    west: float


@dataclass
class CityConfig:
    city: str
    country: str
    bounds: Bounds
    h3_resolution: int
    refine_resolution: int
    refine_threshold: float
    booking_country_code: str
    booking_dest_id: str
    booking_dest_type: str
    checkin_offset_days: int
    checkout_offset_days: int
    adults: int
    rooms: int
    booking_results_cap: int
    booking_use_dates: bool
    airbnb_results_cap: int
    airbnb_adults: int
    airbnb_use_dates: bool


@dataclass
class ScrapingConfig:
    booking_delay_min: float
    booking_delay_max: float
    airbnb_delay_min: float
    airbnb_delay_max: float
    human_break_chance: float
    human_break_min: float
    human_break_max: float
    max_retries: int
    backoff_base: float
    backoff_max: float
    csrf_refresh_interval: int
    full_refresh_interval: int
    proxy_enabled: bool
    proxy_rotate_on_error: bool
    curl_impersonate: str
    curl_timeout: int


@dataclass
class AppConfig:
    city: CityConfig
    scraping: ScrapingConfig
    proxy_urls: list[str]


def load_config() -> AppConfig:
    load_dotenv(ROOT_DIR / ".env")

    with open(CONFIG_DIR / "bucharest.yaml") as f:
        city_raw = yaml.safe_load(f)

    with open(CONFIG_DIR / "scraping.yaml") as f:
        scraping_raw = yaml.safe_load(f)

    bounds = Bounds(**city_raw["bounds"])
    grid = city_raw["grid"]
    booking = city_raw["booking"]
    airbnb = city_raw["airbnb"]

    city = CityConfig(
        city=city_raw["city"],
        country=city_raw["country"],
        bounds=bounds,
        h3_resolution=grid["h3_resolution"],
        refine_resolution=grid["refine_resolution"],
        refine_threshold=grid["refine_threshold"],
        booking_country_code=booking["country_code"],
        booking_dest_id=booking["dest_id"],
        booking_dest_type=booking["dest_type"],
        checkin_offset_days=booking["checkin_offset_days"],
        checkout_offset_days=booking["checkout_offset_days"],
        adults=booking["adults"],
        rooms=booking["rooms"],
        booking_results_cap=booking["results_cap"],
        booking_use_dates=booking.get("use_dates", False),
        airbnb_results_cap=airbnb["results_cap"],
        airbnb_adults=airbnb["adults"],
        airbnb_use_dates=airbnb.get("use_dates", False),
    )

    delays = scraping_raw["delays"]
    retry = scraping_raw["retry"]
    session = scraping_raw["session"]
    proxy = scraping_raw["proxy"]
    curl = scraping_raw["curl_cffi"]

    scraping = ScrapingConfig(
        booking_delay_min=delays["booking"]["base_min"],
        booking_delay_max=delays["booking"]["base_max"],
        airbnb_delay_min=delays["airbnb"]["base_min"],
        airbnb_delay_max=delays["airbnb"]["base_max"],
        human_break_chance=delays["human_break_chance"],
        human_break_min=delays["human_break_min"],
        human_break_max=delays["human_break_max"],
        max_retries=retry["max_retries"],
        backoff_base=retry["backoff_base"],
        backoff_max=retry["backoff_max"],
        csrf_refresh_interval=session["csrf_refresh_interval"],
        full_refresh_interval=session["full_refresh_interval"],
        proxy_enabled=proxy["enabled"],
        proxy_rotate_on_error=proxy["rotate_on_error"],
        curl_impersonate=curl["impersonate"],
        curl_timeout=curl["timeout"],
    )

    proxy_urls = []
    for i in range(1, 10):
        url = os.getenv(f"PROXY_URL_{i}")
        if url:
            proxy_urls.append(url)

    return AppConfig(city=city, scraping=scraping, proxy_urls=proxy_urls)
