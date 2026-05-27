# Cross-platform offset column + σ calibration check — Design

**Status:** approved design (brainstorming)
**Date:** 2026-05-27

## Context

We dedupe Airbnb↔Booking listings into cross-platform property groups and fuse
their coordinates with per-source σ priors (`SIGMA_AIRBNB_EXACT=15`,
`SIGMA_GEOCODED=25`, `SIGMA_BOOKING_ADDRESS=50`, `SIGMA_AIRBNB=100`,
`SIGMA_VAGUE=150`; fuzzed Airbnb = `max(100, radius×0.7)`). The Airbnb σ was a
**modelling assumption**, never calibrated against ground truth.

For a matched twin we have two independent estimates of the same property: the
Airbnb scraped pin and the Booking **geocoded** street address. The distance
between them ("cross-platform offset") is an empirical read on positional error.
A one-off analysis over 1,547 twin groups confirmed the priors are close to
reality:

| Airbnb radius | pairs | median offset | predicted (RMS) `√(σ_airbnb² + σ_geocoded²)` | ratio |
|---|---|---|---|---|
| 0 (exact pin) | 1,003 | **26 m** | √(15²+25²)=29 | 0.90 |
| 152 (std fuzz) | 678 | **99 m** | √(106²+25²)=109 | 0.91 |

(The ~0.9 ratios are the *calibrated* expectation — a median compared to an RMS — not a 10% miss; see ②.)

This design **persists that offset as per-twin columns** (so it's usable later)
and **formalizes the calibration as a per-run check** that flags drift.

## Design

### Shared core — compute the offset (in `src/geo/curate.py`, post-geocode)

For each **cross-platform** property group (gid ∈ `cross_groups` from
`assign_property_groups`; members via `members_by_key`; positions from `by_id`
scraped lat/lng + `airbnb_location_radius_m` and `geocoded_map` for Booking),
split members into Airbnb (use scraped pin) and Booking (use geocoded coord if
present in `geocoded_map`, else scraped coord). Form the set of **per-pair
records** — one per Airbnb×Booking pair — each carrying `(distance_m,
airbnb_sigma, airbnb_radius, booking_source)`. `airbnb_sigma` is the σ the fusion
ledger assigned that Airbnb point — **captured from the
`classify_scraped_precision` call already made in the observation loop**
(`curate.py:75`; record it into a `{lid: scraped_sigma}` map — no recomputation,
so the calibration tests the *literal* σ used). For Airbnb that σ is a pure
function of radius (0→15, fuzzed→`max(σ_airbnb, radius×0.7)`, NULL→`σ_airbnb`).
This pairwise set is the single primitive both outputs derive from, so
multi-member groups are unambiguous:

- **Column** (per group): `offset_m` = **median** of the group's pair distances
  (robust; most groups are 1×1); `source` = `"geocoded"` if ≥1 pair used a
  geocoded Booking coord else `"scraped"`. Written to **every member** of the group.
- **Calibration** (per σ bucket, see ②): the pair records with
  `booking_source == "geocoded"` (passed to `sigma_calibration`, which drops the
  `> 1 km` tail itself).

Cheap: a haversine over ~2,000 pairs (~1,700 of them geocoded-Booking → the
calibration input). Non-cross-platform listings: both columns `NULL`.

### ① Per-twin columns (DB + export)

- New columns on `listings`: `cross_platform_offset_m REAL`,
  `cross_platform_offset_source TEXT`. Added to `_migrate`'s `new_columns`.
- New writer `Database.set_cross_platform_offsets({lid: (offset_m, source)})`.
- **Curation-derived**, like `est_accuracy_m`: cleared (set NULL) in
  `reset_curation_columns` and recomputed every `--curate-only`.
- Added to `_EXPORT_COLUMNS` so both land in `listings.csv` / `listings.geojson`.
  (Not in `_CURATION_COLS` — they're outputs, not inputs to dedup.)

### ② σ calibration metric (`dedup_metrics.json`)

- New module `src/geo/calibration.py`, function
  `sigma_calibration(pair_records, *, geo_sigma, max_dist_m, warn_band, min_n=30)`
  → dict. `pair_records` are the geocoded-Booking pairs from the shared core; it
  drops pairs with `distance_m > max_dist_m` (curate passes
  `fusion.disagreement_km × 1000`, default 1 km — the **same cut curation already
  uses** to flag false cross-platform links), then **buckets by `airbnb_sigma`** —
  the exact σ the ledger assigned — which cleanly separates the distinct radii
  (radius 0→σ 15, 152→σ 106, 500→σ 350, NULL→σ 100; **one bucket per σ value, not a
  single lumped ">0"**). `geo_sigma` is the pipeline's `fusion.sigma_geocoded_m`
  (default 25).
- Per bucket it reports: `n`, `airbnb_sigma`, `measured_median_m`, `predicted_m`
  (`√(airbnb_sigma² + geo_sigma²)`), and `ratio = measured_median / predicted_m`.
- **Predicted is an RMS; measured is a median — so a calibrated ladder yields
  `ratio ≈ 0.9, not 1.0`.** `predicted_m` is the RMS displacement of two
  independent radial-σ estimates; the reported statistic is a *median*, which for
  a 2-D displacement runs ~0.85–0.95× the RMS even when the priors are exactly
  right. (This is exactly the ~0.9 seen in the one-off analysis — correctness, not
  drift.) Read the band relative to ~0.9: below ~0.6 ⇒ priors too pessimistic;
  above ~1.4 ⇒ measured error exceeds even the RMS prediction ⇒ priors too
  optimistic. It is a sanity check, not a rigorous estimator.
- curate.py adds the returned dict to `metrics` under `position_calibration`
  (built before the existing `export_dedup_metrics(metrics)` call, so it flows to
  `dedup_metrics.json` + the logged "Curation metrics") and **soft-warns** via
  `logger.warning` only for buckets with **`n ≥ min_n`** whose `ratio` falls
  outside `warn_band` (small-`n` buckets — e.g. the ~22 wide-fuzz pairs — are
  reported but never warned, to avoid noise). `warn_band` defaults to a module
  constant `(0.6, 1.4)` in `calibration.py`, resolved by curate via
  `getattr(fusion_cfg, "calibration_warn_band", …)` so it becomes yaml-tunable if
  that field is later added to `FusionConfig`. It **never raises / never fails the run**.

### ③ Documentation (`METHODOLOGY.md` — the tracked deliverable)

- **§8 "Position hierarchy" (the σ ladder, ~line 414):** add a short note that the
  σ priors are now **corroborated per run** by the cross-platform calibration —
  `position_calibration` in `dedup_metrics.json` compares the measured
  Airbnb-pin↔Booking-geocoded displacement (per σ bucket) against
  `√(σ² + σ_geocoded²)`; the May 2026 read is `ratio ≈ 0.9` in both populated
  buckets (a median-vs-RMS artefact ⇒ in-band). Note that the **exact-pin bucket
  cross-checks the geocoder** (σ_geocoded≈25, measured ~26 m) and the **fuzz bucket
  the Airbnb obfuscation** (σ≈106 for the standard 152 m circle, measured ~99 m) —
  the empirical answer to
  "what is the Airbnb σ based on": a modelling assumption, now confirmed against
  geocoded twins.
- **§11 Data dictionary (~line 533):** add rows for `cross_platform_offset_m`
  (Airbnb-pin↔Booking-geocoded distance for a twinned property, in metres, on every
  member of a cross-platform group; `NULL` for non-twins) and
  `cross_platform_offset_source` (`geocoded` / `scraped` — which Booking anchor the
  offset used).
- Generated reports (`NEW_VS_OLD.md`, `CAPTURE_COMPARISON.md`) stay **untracked** —
  refresh locally if desired; not part of the committed deliverable.

### Where it runs

In `curate.py` after the observation loop (so the `{lid: scraped_sigma}` map +
`geocoded_map` are populated) and after `members_by_key` is built (`curate.py:139`;
`cross_groups` comes from `assign_property_groups` at `:42`). Offsets are computed
once, written via the new DB writer, and the geocoded pairs passed to
`sigma_calibration`; its result is added to `metrics` before the existing
`export_dedup_metrics(metrics)` call.

## Edge cases

- Group with no geocoded Booking → offset uses Booking scraped coords, `source="scraped"`.
- Group with multiple Airbnb and/or Booking → median over all cross pairs.
- A `cross_groups` gid whose members are all one platform → cannot happen by
  definition (cross = spans both), but guard: skip if either side empty.
- `>1 km` outlier pairs → kept in the **column** (it's the raw measured offset, useful
  for the review tail) but **excluded from the calibration metric** (biases the ladder check).
- No cross-platform groups at all → calibration returns empty buckets; no warning.
- Geocoding disabled (`geocoding.enabled = false`; default is **true**) → no
  geocoded pairs → empty calibration buckets, reported empty, no warning (graceful).

## Testing

Unit-test `src/geo/calibration.py` on **synthetic** inputs (never the live DB):
- Distinct σ land in distinct buckets (a `σ=106` pair and a `σ=350` pair do **not**
  share a bucket); each bucket's `predicted_m = √(airbnb_sigma² + geo_sigma²)`.
- Known offsets per bucket → expected `measured_median_m`, `predicted_m`, `ratio`.
- A bucket with `n ≥ min_n` and offsets far above predicted → out-of-band ratio
  flagged; the **same wild ratio in a bucket with `n < min_n` → reported but NOT warned**.
- `> max_dist_m` pairs are excluded from the metric.
- Offset computation: a 1×1 twin with known coords → expected `offset_m` and
  `source`; a geocoded-absent twin → `source="scraped"`.

## Non-goals

- **No auto-tuning** of the σ priors — report + warn only; recalibration stays a
  human decision.
- No UI/map changes beyond the new export columns.
- Not a dedup-quality review queue (the >500 m tail) — separate idea, out of scope.

## Rollout

`--curate-only` (cache-fast): migration adds the columns, curation computes
offsets + calibration, exports regenerate with the two new columns,
`dedup_metrics.json` gains `position_calibration`. Verify the columns populate for
twins (NULL otherwise) and the calibration block matches the one-off analysis —
both buckets `ratio ≈ 0.9` (the calibrated expectation per the median-vs-RMS note
in ②), well in-band. Then update + **commit `METHODOLOGY.md`** (§8 + §11) — the
tracked doc deliverable; the untracked reports refresh locally only.
