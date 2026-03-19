from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from ..config import DATA_DIR
from ..models.enums import Platform, ScrapeStatus
from ..models.listing import Listing

logger = logging.getLogger(__name__)

DB_PATH = DATA_DIR / "bucharest.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    platform_id TEXT NOT NULL,
    name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    property_type TEXT,
    star_rating REAL,
    review_score REAL,
    review_count INTEGER,
    price_per_night REAL,
    currency TEXT DEFAULT 'EUR',
    url TEXT,
    thumbnail_url TEXT,
    is_superhost INTEGER,
    scraped_at TEXT NOT NULL,
    grid_cell_id TEXT,
    raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_listings_platform ON listings(platform);
CREATE INDEX IF NOT EXISTS idx_listings_platform_id ON listings(platform, platform_id);
CREATE INDEX IF NOT EXISTS idx_listings_coords ON listings(latitude, longitude);

CREATE TABLE IF NOT EXISTS grid_progress (
    cell_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    result_count INTEGER DEFAULT 0,
    started_at TEXT,
    completed_at TEXT,
    error_message TEXT,
    PRIMARY KEY (cell_id, platform)
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    total_cells INTEGER,
    completed_cells INTEGER DEFAULT 0,
    total_listings INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'
);
"""


class Database:
    def __init__(self, db_path: Path | None = None):
        self.db_path = db_path or DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def upsert_listing(self, listing: Listing) -> bool:
        """Insert or update a listing. Returns True if new, False if updated."""
        sql = """
        INSERT INTO listings (
            id, platform, platform_id, name, latitude, longitude,
            property_type, star_rating, review_score, review_count,
            price_per_night, currency, url, thumbnail_url, is_superhost,
            scraped_at, grid_cell_id, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            price_per_night=excluded.price_per_night,
            review_score=excluded.review_score,
            review_count=excluded.review_count,
            scraped_at=excluded.scraped_at,
            raw_json=excluded.raw_json
        """
        cursor = self.conn.execute(sql, listing.to_row())
        self.conn.commit()
        return cursor.rowcount > 0

    def upsert_listings(self, listings: list[Listing]) -> int:
        """Bulk upsert listings. Returns count of rows affected."""
        sql = """
        INSERT INTO listings (
            id, platform, platform_id, name, latitude, longitude,
            property_type, star_rating, review_score, review_count,
            price_per_night, currency, url, thumbnail_url, is_superhost,
            scraped_at, grid_cell_id, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            price_per_night=excluded.price_per_night,
            review_score=excluded.review_score,
            review_count=excluded.review_count,
            scraped_at=excluded.scraped_at,
            raw_json=excluded.raw_json
        """
        rows = [l.to_row() for l in listings]
        self.conn.executemany(sql, rows)
        self.conn.commit()
        return len(rows)

    def get_listing_count(self, platform: Platform | None = None) -> int:
        if platform:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM listings WHERE platform=?",
                (platform.value,),
            ).fetchone()
        else:
            row = self.conn.execute("SELECT COUNT(*) FROM listings").fetchone()
        return row[0]

    # -- Grid progress --

    def init_grid_progress(self, cell_ids: list[str], platforms: list[Platform]):
        """Initialize progress tracking for all grid cells."""
        sql = """
        INSERT OR IGNORE INTO grid_progress (cell_id, platform, status)
        VALUES (?, ?, 'pending')
        """
        rows = [
            (cid, p.value) for cid in cell_ids for p in platforms
        ]
        self.conn.executemany(sql, rows)
        self.conn.commit()

    def update_cell_status(
        self,
        cell_id: str,
        platform: Platform,
        status: ScrapeStatus,
        result_count: int = 0,
        error_message: str | None = None,
    ):
        now = datetime.utcnow().isoformat()
        if status == ScrapeStatus.IN_PROGRESS:
            self.conn.execute(
                "UPDATE grid_progress SET status=?, started_at=? WHERE cell_id=? AND platform=?",
                (status.value, now, cell_id, platform.value),
            )
        elif status in (ScrapeStatus.COMPLETED, ScrapeStatus.FAILED, ScrapeStatus.NEEDS_REFINEMENT):
            self.conn.execute(
                "UPDATE grid_progress SET status=?, result_count=?, completed_at=?, error_message=? "
                "WHERE cell_id=? AND platform=?",
                (status.value, result_count, now, error_message, cell_id, platform.value),
            )
        self.conn.commit()

    def get_pending_cells(self, platform: Platform) -> list[str]:
        rows = self.conn.execute(
            "SELECT cell_id FROM grid_progress WHERE platform=? AND status='pending'",
            (platform.value,),
        ).fetchall()
        return [r[0] for r in rows]

    def get_cells_needing_refinement(self, platform: Platform) -> list[str]:
        rows = self.conn.execute(
            "SELECT cell_id FROM grid_progress WHERE platform=? AND status='needs_refinement'",
            (platform.value,),
        ).fetchall()
        return [r[0] for r in rows]

    def get_progress_summary(self) -> dict:
        rows = self.conn.execute("""
            SELECT platform, status, COUNT(*), SUM(result_count)
            FROM grid_progress
            GROUP BY platform, status
        """).fetchall()

        summary = {}
        for platform, status, count, total in rows:
            if platform not in summary:
                summary[platform] = {}
            summary[platform][status] = {"cells": count, "results": total or 0}
        return summary

    # -- Scrape runs --

    def start_run(self, total_cells: int) -> int:
        cursor = self.conn.execute(
            "INSERT INTO scrape_runs (started_at, total_cells) VALUES (?, ?)",
            (datetime.utcnow().isoformat(), total_cells),
        )
        self.conn.commit()
        return cursor.lastrowid

    def update_run(self, run_id: int, completed_cells: int, total_listings: int):
        self.conn.execute(
            "UPDATE scrape_runs SET completed_cells=?, total_listings=? WHERE id=?",
            (completed_cells, total_listings, run_id),
        )
        self.conn.commit()

    def finish_run(self, run_id: int):
        self.conn.execute(
            "UPDATE scrape_runs SET completed_at=?, status='completed' WHERE id=?",
            (datetime.utcnow().isoformat(), run_id),
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
