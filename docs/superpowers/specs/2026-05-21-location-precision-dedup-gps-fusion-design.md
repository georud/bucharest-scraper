# Design — Location precision, identity-based dedup, and GPS fusion

**Date:** 2026-05-21
**Status:** Approved design, ready for implementation plan

## Context

The dataset's two weakest dimensions are **position** and **duplicate identity**:

- **Position.** Airbnb fuzzes every unbooked listing inside a ~150 m circle
  (confirmed: PDP carries `mapMarkerRadius` + `locationDisclaimer`, and
  `addressTitle`/`exactAddress` are `null` for both individual *and* professional
  listings — Airbnb publishes **no** property street address). Booking, by
  contrast, carries a street-level address (number / *strada* / *bloc* /
  *apartament*) for **4,694 of 4,783 listings (98%)** plus 6–7-decimal coords.
  So one platform is precise and the other is deliberately vague, and today every
  coordinate is treated as equally trustworthy.
- **Duplicates.** Cross-platform linking currently uses only proximity + fuzzy
  name (greedy 1:1, see `src/dedup/deduplicator.py`). But both platforms publish,
  for professional listings, `business_registration_number` / `business_phone` /
  `business_email`, and these overlap across platforms (167 shared phones, 158
  emails, 49 registration numbers). These are *operator-level* identity keys (one
  phone → many flats) that today go unused for linking.

**Outcome:** every listing carries an honest, sourced precision tag; duplicates
are found with the identity keys (correctly, without re-introducing operator
over-merge); and matched duplicates plus geocoded addresses are *fused* into a
calculated best position that improves over time. All original coordinates are
preserved — nothing is overwritten.

This is a new **post-enrichment curation stage** that runs after Phase 3 and
before export. It is fully **re-runnable against the existing DB** (no re-scrape).

---

## Components (dependency order)

```
operator layer ─▶ property dedup ─▶ precision tag ─▶ geocode + ledger ─▶ fusion ─▶ verification ─▶ export
```

### 1. Operator layer — `src/dedup/operators.py` (new)

- `normalize_registration(s)` (strip to `[a-z0-9]`, drop leading `ro`), `normalize_phone(s)` (digits only, strip country prefix), `normalize_email(s)` (lower/trim).
- `assign_operator_ids(listings) -> dict[listing_id, operator_id]`: **union-find** over listings that share *any* normalized identity key. Safe here — a shared registration/phone genuinely is one operator (unlike GPS+name). `operator_id = "op_" + md5(sorted(canonical_keys))[:16]`.
- Re-key `Database.get_operator_summary()` on `operator_id` (fixes the "STR / STRE Asset Management" split that share a registration number).

### 2. Layered property dedup — extend `src/dedup/deduplicator.py`

New `assign_property_groups(listings, operator_map) -> dict[listing_id, group_id]` replacing the call site of `assign_cross_platform_groups` (keep the old method for now, mark superseded):

- **Candidate generation, two sources merged:**
  - *Within a shared `operator_id` block:* relaxed GPS ≤ 250 m **and** (name ≥ 70% **or** room-config agreement) — the small candidate pool lets us pin the unit.
  - *Outside operator blocks (current spatial bucket):* tightened — GPS < 100 m **and** name ≥ 80% (up from 72), room-config as tiebreak.
- **Room-config helper** `room_config_matches(a, b)`: compare `bedrooms`/`beds`/`bathrooms`, treating `None` as wildcard; agreement = all present fields equal.
- **Greedy + clique check:** sort candidates by a composite confidence (identity-shared > name_sim > inverse-distance); accept a pair/member only if it matches **every** existing member of the target group. This structurally prevents the transitive over-merge that union-find on GPS+name produced.
- Covers **within-platform and cross-platform** dupes ("among or across").
- `group_id = "pg_" + md5(sorted(member_ids))[:16]`. `cross_platform_group_id` is **derived**: set to the group id only when the group spans both platforms (preserves its original cross-platform-only meaning), else `NULL`.

### 3. Precision tagging — `src/geo/precision.py` (new)

`classify_scraped_precision(listing, raw_json, stack_count) -> (provisional_precision, sigma_m)` — no network. Its job is to set the **σ of the scraped observation** that seeds the ledger; the returned label is provisional and the listing's authoritative `location_precision` is decided later by fusion (§5).

- **Booking:** street-level address in `raw_json` (regex for number/`strada`/`calea`/`bulevardul`/`soseaua`/`aleea`/`bloc`/`apartament`) and not stacked → `approximate`, σ ≈ 50 m (the scraped point; geocoding will tighten it). Vague address or coordinate stacked with ≥ 3 others → `approximate`, σ ≈ 150 m.
- **Airbnb:** always `approximate`, σ ≈ 100 m (policy fuzz; `mapMarkerRadius` confirms). Stacked coords confirm centroids.

`stack_count` comes from a one-pass `Counter` over `round(lat,6),round(lng,6)` per platform.

### 4. Geocoding + observation ledger — `src/geo/geocode.py` (new)

- **Nominatim client** (small direct client over the project's HTTP lib — no new heavy `geopy` dependency): descriptive `User-Agent`, **≤ 1 req/s** throttle, on-disk cache.
- **Cache + persistent retry** — new table `geocode_cache(address_norm PK, status, latitude, longitude, quality, attempts, last_tried_at, raw_json)`. Successes cached forever; transient failures (timeout/429/5xx) store `attempts`+`last_tried_at` and are **re-queued on every subsequent run** with exponential backoff until resolved or `max_retries` (default 5). This is the "keep retrying every entry" requirement.
- **Geocode every geocodable address:** all ~4,700 Booking street addresses (one-time ≈ 80 min, cached after). Airbnb has no property address (confirmed) — its entries are positioned by fusion/transfer, not geocoding. `business_address` is the operator HQ and is **never** used as a property position.
- A confident geocode → `latitude_geocoded`/`longitude_geocoded`, `geocoded_address`, σ ≈ 25 m.

**Observation ledger** — new append-only table
`position_observations(id PK, listing_id, property_group_id, capture_date, platform, source, latitude, longitude, sigma_m, created_at)`. It keeps **every** coordinate ever seen and is the substrate for fusion:
- Per run, append the scraped observation (σ from §3) and the geocoded observation (if any) for each listing.
- **One-time temporal backfill:** ingest coordinates from `data/bucharest.db.backup-20260515-091053` (April capture) as additional observations keyed by listing/property — immediately giving no-twin Airbnb listings a second independent jitter sample.

### 5. Position fusion — `src/geo/fusion.py` (new)

`fuse_group(observations) -> FusedPosition`:

- Project lat/lng to local metres around Bucharest (equirectangular: 1° lat ≈ 111,320 m; 1° lng ≈ 111,320·cos 44.43° ≈ 79,500 m).
- **Inverse-variance weighted mean:** `wᵢ = 1/σᵢ²`; `x̂ = Σwᵢxᵢ / Σwᵢ`; fused `σ = 1/√Σwᵢ`. A precise Booking-geocoded point dominates; an Airbnb point barely nudges it; two approximate points still reduce error; N captures shrink Airbnb jitter ≈ ÷√N.
- **Outlier rejection:** drop observations > 1,000 m from the weighted median before the final fuse. If a Booking and an Airbnb observation in the same group disagree by > `disagreement_km` (default 1.0), **flag the property group as a probable false-positive dedup** (feeds §6) and fuse only the within-platform-consistent subset.
- Emit `latitude_best`/`longitude_best`, `est_accuracy_m` (fused σ), `position_confidence` (monotonic map of fused σ → [0,1], e.g. `clamp((150 − σ)/150, 0, 1)`; exact form tunable), `location_source` (`geocoded_address` / `transferred_from_twin` / `platform_coord`), `location_precision` (`exact` if fused σ ≤ ~40 m else `approximate`).
- **`location_source` is the dominant contributor** to the fused point: `geocoded_address` if the listing's own geocoded observation carried the most weight; `transferred_from_twin` if the deciding weight came from another group member's precise/geocoded point (the listing itself had only a fuzzed coordinate); `platform_coord` otherwise.
- Listings not in any multi-member group still fuse their own observations (scraped + temporal), so every listing gets a best position.

### 6. Verification — `src/dedup/validate.py` (new) + review exports

- **Auto metric** (identity keys as ground truth): for property groups whose members carry identity keys on ≥ 2 sides, report **precision proxy** (groups whose identities agree ÷ groups with comparable identities), **recall proxy** (identity-confirmed co-located twins we grouped ÷ all such candidates), and a **conflict list** (grouped members whose identities disagree — false-positive suspects, union of §5's distance flags). Printed to the run log + `data/exports/dedup_metrics.json`.
- **Sample export** `data/exports/dedup_review.csv`: N matched pairs + N near-misses with names, addresses, both coordinates, distance, identity keys, photo URLs — for eyeballing and threshold tuning.
- **Reverse-geocode QA** `data/exports/geo_review.csv`: reverse-geocode each fused best point, compare its city/sector to the scraped Booking address; list mismatches.

---

## Schema additions

`listings` (via the existing idempotent `PRAGMA table_info` migration in `Database._migrate`):

| Column | Meaning |
|---|---|
| `operator_id` | shared id for one operator (identity-key union-find) |
| `property_group_id` | same physical flat, within or across platforms |
| `latitude` / `longitude` | **unchanged** — as-scraped, never overwritten |
| `latitude_geocoded` / `longitude_geocoded` | from geocoding (NULL if none) |
| `latitude_best` / `longitude_best` | fused calculated position (map/exports use these) |
| `geocoded_address` | the address string geocoded (audit) |
| `location_precision` | `exact` / `approximate` (quality of *best*) |
| `location_source` | `geocoded_address` / `transferred_from_twin` / `platform_coord` |
| `est_accuracy_m` | fused σ in metres |
| `position_confidence` | 0–1, for "map-grade" filtering |

`cross_platform_group_id` is retained but now **derived** from `property_group_id`.

New tables: `position_observations` (append-only ledger, §4) and `geocode_cache` (§4).

---

## Pipeline integration — `src/orchestrator.py`

New stage `_curate_geo_and_dedup(listings)` after enrichment, before export, in order:
operator IDs → property groups → scraped-precision observations → geocode (+ one-time April backfill) → fusion → verification. Exposed via a `--curate-only` flag so it can be re-run on the existing DB without scraping. The existing `_link_cross_platform()` is replaced by this stage.

## Config — `config/scraping.yaml` + `src/config.py`

```yaml
geocoding:
  enabled: true
  nominatim_url: "https://nominatim.openstreetmap.org/search"
  user_agent: "bucharest-str-research/1.0 (contact email)"
  rate_limit_s: 1.0
  max_retries: 5
  cache_path: "data/geocode_cache.sqlite"   # or a table in the main DB
dedup:
  operator_relaxed_distance_m: 250
  strict_distance_m: 100
  strict_name_threshold: 80
fusion:
  sigma_geocoded_m: 25
  sigma_booking_address_m: 50
  sigma_vague_m: 150
  sigma_airbnb_m: 100
  disagreement_km: 1.0
  exact_max_sigma_m: 40
```

## Exports / docs — `src/storage/exporter.py`, `METHODOLOGY.md`, `CAPTURE_COMPARISON.md`

- Add the new columns to `_EXPORT_COLUMNS`; GeoJSON emits `latitude_best`/`longitude_best` (fallback to scraped when NULL) and carries precision/source/accuracy/confidence as properties.
- `operators.csv` re-keyed on `operator_id`; new `dedup_review.csv`, `dedup_metrics.json`, `geo_review.csv`.
- Map popups (`src/visualization/map_builder.py`) show precision + confidence and plot best coords.
- `METHODOLOGY.md` §7 (unit of analysis) and §8 (GPS precision) updated to describe operator/identity linking, fusion, and the precision tag; `CAPTURE_COMPARISON.md` gets a short note that May positions are now fused.

## Error handling

- Geocoder unreachable → skip geocoding for the run; everything stays `platform_coord`; pipeline continues (degrades, never crashes).
- Cache row corrupt → treat as a miss and re-geocode.
- Listing missing coords → excluded from fusion, `location_source` left NULL.
- Backfill DB absent → skip temporal backfill with a logged warning.

## Testing / acceptance

- **Unit:** `normalize_*`; `room_config_matches`; fusion math (inverse-variance correctness on hand-computed cases, outlier rejection, two-approximate-points variance drop, N-sample √N shrink); precision classification on Booking-detailed / Booking-vague / Airbnb fixtures.
- **Integration:** run the stage on a small fixture DB; assert operator grouping merges the STR/STRE example, property groups stay 1:1 where expected, and best coords move toward the geocoded point.
- **Acceptance on the real DB:** `--curate-only` run; `dedup_metrics.json` precision proxy ≥ ~0.95 with no high-distance conflicts unexplained; spot-check `dedup_review.csv`; confirm ~4,700 Booking addresses geocoded (cached) and Airbnb twins inherit tightened positions.

## Risks / caveats

- Nominatim ToS: ≤ 1 req/s, attribution required; self-hosted Nominatim (Docker) is the scale option if rate limits bite.
- `business_address` ≠ property location — must never be geocoded as the property point.
- Airbnb σ (~100 m) is a modelling assumption; documented as such.
- Temporal fusion assumes the unit didn't physically move between captures (true for STRs); a re-listed flat at a new address would be a new property group.
- Reverse-geocode is a QA signal only, never overwrites a position.

## Out of scope (YAGNI)

No new UI, no live/continuous geocoding service, no ML matcher, no address parsing beyond the precision regex, no changes to the scrapers themselves.
