# Canonical map_* Position Columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add four derived, always-populated columns — `map_latitude`, `map_longitude`, `map_source`, `map_precision` — to the CSV and GeoJSON exports so a future map tool can be pointed at one canonical position + source for every listing.

**Architecture:** The columns are computed at export time via `COALESCE(latitude_best, latitude)` etc. (no DB schema change), defined once in `src/storage/exporter.py` and shared by both exporters. They are inserted right after `name`; existing raw / `_geocoded` / `_best` columns stay for provenance.

**Tech Stack:** Python 3.13, sqlite3, stdlib `csv`/`json`, pytest.

---

### Task 1: Shared `map_*` definition + wire into CSV export

**Files:**
- Modify: `src/storage/exporter.py` (add `_MAP_COLUMNS` + `_select_and_columns()` after the `_EXPORT_COLUMNS` block at lines 18–42; rewrite `export_csv` at lines 45–60; remove the now-unused `_EXPORT_SELECT` at line 42)
- Test: `tests/test_exporter.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_exporter.py` (the file already has `_mk`, `export_csv`, `export_geojson` imports and a `db`/`tmp_path` fixture):

```python
def test_csv_map_columns_present_and_populated(db, tmp_path):
    # One curated row (best + source + precision), one uncurated (scraped only).
    _mk(db, "booking_1", Platform.BOOKING, 44.43, 26.10, best=(44.4301, 26.1001))
    db.conn.execute("UPDATE listings SET location_source='geocoded_address', "
                    "location_precision='exact' WHERE id='booking_1'")
    _mk(db, "airbnb_2", Platform.AIRBNB, 44.4330, 26.10)  # no best, uncurated
    db.conn.commit()

    path = export_csv(db, output_path=tmp_path / "l.csv")
    with open(path, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    by_id = {r["id"]: r for r in rows}

    assert {"map_latitude", "map_longitude", "map_source", "map_precision"} <= set(rows[0].keys())
    # curated row uses the fused best position + its source/precision
    assert float(by_id["booking_1"]["map_latitude"]) == 44.4301
    assert float(by_id["booking_1"]["map_longitude"]) == 26.1001
    assert by_id["booking_1"]["map_source"] == "geocoded_address"
    assert by_id["booking_1"]["map_precision"] == "exact"
    # uncurated row falls back to the scraped coord + defaults
    assert float(by_id["airbnb_2"]["map_latitude"]) == 44.4330
    assert by_id["airbnb_2"]["map_source"] == "platform_coord"
    assert by_id["airbnb_2"]["map_precision"] == "approximate"
    # always populated
    assert all(r["map_latitude"] and r["map_longitude"] for r in rows)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_exporter.py::test_csv_map_columns_present_and_populated -q`
Expected: FAIL — `KeyError: 'map_latitude'` (column not in header / DictReader row).

- [ ] **Step 3: Add the shared definition**

In `src/storage/exporter.py`, **replace** the line `_EXPORT_SELECT = ", ".join(_EXPORT_COLUMNS)` (line 42) with:

```python
# Derived "use this for a map" columns: one always-populated position + its
# source/precision, from the curated best with a fallback to the raw scraped
# coord. Reuses the COALESCE(latitude_best, latitude) rule map_builder.py plots,
# so internal map and exports agree. Each entry is (column_name, sql_expression).
_MAP_COLUMNS = [
    ("map_latitude", "COALESCE(latitude_best, latitude)"),
    ("map_longitude", "COALESCE(longitude_best, longitude)"),
    ("map_source", "COALESCE(location_source, 'platform_coord')"),
    ("map_precision", "COALESCE(location_precision, 'approximate')"),
]


def _select_and_columns() -> tuple[str, list[str]]:
    """Return (sql_select_expr, column_names) with the derived map_* columns
    inserted right after 'name'. One definition shared by both exporters."""
    names: list[str] = []
    exprs: list[str] = []
    for col in _EXPORT_COLUMNS:
        names.append(col)
        exprs.append(col)
        if col == "name":
            for mname, mexpr in _MAP_COLUMNS:
                names.append(mname)
                exprs.append(f"{mexpr} AS {mname}")
    return ", ".join(exprs), names
```

- [ ] **Step 4: Wire it into `export_csv`**

Rewrite `export_csv` (lines 45–60) to use the shared helper:

```python
def export_csv(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings to CSV with every captured field."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.csv"

    select_sql, columns = _select_and_columns()
    rows = db.conn.execute(
        f"SELECT {select_sql} FROM listings ORDER BY platform, name"
    ).fetchall()

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    logger.info("Exported %d listings to %s", len(rows), path)
    return path
```

- [ ] **Step 5: Confirm `_EXPORT_SELECT` has no other users**

Run: `grep -rn "_EXPORT_SELECT" src/ tests/`
Expected: only the (now-rewritten) `export_geojson` at `src/storage/exporter.py` still references it — that is fixed in Task 2. If anything else references it, leave the constant defined; otherwise it is removed here.

- [ ] **Step 6: Run the test to verify it passes**

Run: `python -m pytest tests/test_exporter.py::test_csv_map_columns_present_and_populated -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/storage/exporter.py tests/test_exporter.py
git commit -m "feat(exporter): canonical map_* columns in CSV (derived, always populated)"
```

---

### Task 2: Wire into GeoJSON export + properties

**Files:**
- Modify: `src/storage/exporter.py` (`export_geojson` at lines 63–99)
- Test: `tests/test_exporter.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_exporter.py`:

```python
def test_geojson_has_map_source_and_geometry_from_map_coords(db, tmp_path):
    import json
    _mk(db, "airbnb_2", Platform.AIRBNB, 44.4330, 26.10, best=(44.4301, 26.1001))
    db.conn.execute("UPDATE listings SET location_source='transferred_from_twin', "
                    "location_precision='exact' WHERE id='airbnb_2'")
    db.conn.commit()

    path = export_geojson(db, output_path=tmp_path / "l.geojson")
    feat = json.load(open(path, encoding="utf-8"))["features"][0]
    assert feat["geometry"]["coordinates"] == [26.1001, 44.4301]   # [map_longitude, map_latitude]
    assert feat["properties"]["map_source"] == "transferred_from_twin"
    assert feat["properties"]["map_precision"] == "exact"
    assert feat["properties"]["map_latitude"] == 44.4301
    assert "latitude" not in feat["properties"]                    # raw lat/lng still popped
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_exporter.py::test_geojson_has_map_source_and_geometry_from_map_coords -q`
Expected: FAIL — `KeyError: 'map_source'` in properties (export_geojson still uses the old `_EXPORT_SELECT`/`_EXPORT_COLUMNS`).

- [ ] **Step 3: Rewrite `export_geojson` to use the shared helper**

Replace `export_geojson` (lines 63–99) with:

```python
def export_geojson(db: Database, output_path: Path | None = None) -> Path:
    """Export all listings as a GeoJSON FeatureCollection, including every captured field."""
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = output_path or EXPORTS_DIR / "listings.geojson"

    select_sql, columns = _select_and_columns()
    rows = db.conn.execute(
        f"SELECT {select_sql} FROM listings "
        "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
        "ORDER BY platform, name"
    ).fetchall()

    features = []
    for row in rows:
        props = dict(zip(columns, row))
        lng = props["map_longitude"]
        lat = props["map_latitude"]
        props.pop("latitude", None)
        props.pop("longitude", None)
        # Coerce is_superhost to a proper boolean / None
        sh = props.get("is_superhost")
        if sh is not None:
            props["is_superhost"] = bool(sh)
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lng, lat]},
            "properties": props,
        })

    geojson = {"type": "FeatureCollection", "features": features}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    logger.info("Exported %d features to %s", len(features), path)
    return path
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `python -m pytest tests/test_exporter.py::test_geojson_has_map_source_and_geometry_from_map_coords -q`
Expected: PASS.

- [ ] **Step 5: Run the full suite (no regressions)**

Run: `python -m pytest tests/ -q`
Expected: all pass (the existing `test_csv_has_new_columns` and `test_geojson_uses_best_coords_when_present` still pass — geometry is now `[map_longitude, map_latitude]` which equals the prior best-then-scraped value).

- [ ] **Step 6: Commit**

```bash
git add src/storage/exporter.py tests/test_exporter.py
git commit -m "feat(exporter): map_* columns in GeoJSON properties; geometry from map coords"
```

---

### Task 3: Regenerate the live exports + spot-check

**Files:** none (run only)

- [ ] **Step 1: Regenerate exports from the live DB**

Run:
```bash
python -c "from src.storage.database import Database; from src.storage.exporter import export_csv, export_geojson; db=Database(); export_csv(db); export_geojson(db); db.close()"
```
Expected: logs "Exported 10982 listings ..." and "Exported ... features ...".

- [ ] **Step 2: Spot-check the CSV header + a row**

Run:
```bash
python -c "import csv; r=next(csv.DictReader(open('data/exports/listings.csv',encoding='utf-8-sig'))); print('map cols:', [k for k in r if k.startswith('map_')]); print(r['map_latitude'], r['map_longitude'], r['map_source'], r['map_precision'])"
```
Expected: `map cols: ['map_latitude', 'map_longitude', 'map_source', 'map_precision']` and a populated lat/lng + a source + a precision.

- [ ] **Step 3: Confirm map_latitude is populated for every row**

Run:
```bash
python -c "import csv; rows=list(csv.DictReader(open('data/exports/listings.csv',encoding='utf-8-sig'))); print('rows:',len(rows),'missing map_latitude:',sum(1 for x in rows if not x['map_latitude']))"
```
Expected: `missing map_latitude: 0`.

(No commit — `data/exports/` is gitignored.)

---

## Self-review

- **Spec coverage:** four `map_*` columns (Task 1 def) ✓; derived/COALESCE not stored (Task 1 `_MAP_COLUMNS`) ✓; shared single definition (`_select_and_columns`, used by both — Tasks 1 & 2) ✓; CSV placement after `name` (helper insertion) ✓; GeoJSON properties carry `map_source`/`map_precision` + geometry from map coords (Task 2) ✓; always-populated + fallback + defaults tested (Task 1 test) ✓; no schema change / no renames (only exporter touched) ✓; regenerate + verify (Task 3) ✓.
- **Placeholders:** none — every step has full code/commands.
- **Type consistency:** `_select_and_columns()` returns `(str, list[str])`, used identically in both exporters; `map_*` names match across `_MAP_COLUMNS`, tests, and Task 3 checks.
