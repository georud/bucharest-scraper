# Canonical map position columns — design

*2026-05-23*

## Problem

The CSV/GeoJSON exports carry three latitude columns (`latitude`,
`latitude_geocoded`, `latitude_best`) and the matching longitudes. Anyone
building a map from the export has to know to pick `latitude_best` (and fall back
to `latitude` when it is null). We want **one obvious, always-populated position
pair plus its source/trust**, so a future map tool can be pointed at a single
column set for every entry.

Current state (already in place): `latitude_best`/`longitude_best`,
`location_source`, `location_precision` exist and are populated for all curated
listings, and `map_builder.py` already plots `COALESCE(latitude_best, latitude)`.
This change just **surfaces that canonical position in the exports**.

## Decision

Add four **derived** (computed at export time, *not* stored) columns to both
exporters:

| Column | Definition | Notes |
|---|---|---|
| `map_latitude` | `COALESCE(latitude_best, latitude)` | always populated (a scraped coord is effectively never null) |
| `map_longitude` | `COALESCE(longitude_best, longitude)` | " |
| `map_source` | `COALESCE(location_source, 'platform_coord')` | `geocoded_address` / `transferred_from_twin` / `platform_coord` — where the point came from |
| `map_precision` | `COALESCE(location_precision, 'approximate')` | `exact` / `approximate` — how much to trust the point |

**Derived, not a stored DB column:** the values are fully computable from existing
columns; a stored column would duplicate `latitude_best` and need re-syncing on
every curation (DRY/YAGNI). The exports are the consumption point, so the
canonical pair lives there — and it reuses the exact `COALESCE(latitude_best,
latitude)` rule `map_builder.py` already uses, so internal map and exports agree.

## Components / changes

- **`src/storage/exporter.py`**
  - One shared definition of the four SQL expressions (a `_MAP_SELECT` fragment)
    used by both `export_csv` and `export_geojson` — single source of truth.
  - `export_csv`: select + emit the four `map_*` columns, placed immediately
    after `name` so they are the obvious ones to map from; the existing raw /
    `_geocoded` / `_best` columns are retained (further right) for provenance.
  - `export_geojson`: the geometry already uses best-then-scraped (unchanged);
    add `map_source`, `map_precision`, `map_latitude`, `map_longitude` to each
    feature's `properties` so the point can be coloured/filtered.
- **No DB schema change. No renames** of existing columns. `map_builder.py`
  unchanged (it already COALESCEs the same way).

## Edge cases

- A listing with no coordinate at all → `map_latitude`/`map_longitude` null. The
  GeoJSON already filters `latitude IS NOT NULL`; the CSV includes such a row with
  a null map position (acceptable and rare — scraped coords are effectively never
  null in this dataset).
- Uncurated listing (no `latitude_best`/`location_source`) → falls back to the
  scraped coord, `map_source = 'platform_coord'`, `map_precision = 'approximate'`.

## Testing

- Export a small fixture DB and assert the CSV header contains the four `map_*`
  columns and every row's `map_latitude`/`map_longitude` is non-null.
- A row with `latitude_best` set → `map_latitude == latitude_best`; a row with
  only a scraped coord → `map_latitude == latitude`.
- `map_precision` defaults to `approximate` and `map_source` to `platform_coord`
  when the row is uncurated.
- GeoJSON: each feature's `properties` includes `map_source`/`map_precision`, and
  the geometry equals (`map_longitude`, `map_latitude`).

## Non-goals

- No stored DB columns; no separate map-only export file; no renaming or removing
  existing columns.

## Usage

Future map tool: latitude → `map_latitude`, longitude → `map_longitude`,
colour/legend → `map_precision` (or `map_source`). One pair, every entry.
