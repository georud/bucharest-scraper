# Methodology & Data Notes

A guide for anyone analysing or publishing from this dataset. It explains where
the data comes from, how it was collected, how complete it is, what each field
means, and — most importantly — **what you can and cannot safely claim from it**.

The scale tables in §1 and §5 are the **April 2026 baseline**; the later sections
(positions §8, dedup/operators §6–§7) cite the **current May 2026 capture**. Both
are dated snapshots — re-running the scraper produces a fresh one.

---

## 1. What this dataset is

Short-term accommodation listings in Bucharest, Romania, scraped from **Booking.com**
and **Airbnb**, with — this is the point of the project — the **EU Digital Services
Act (DSA) trader-disclosure data** attached to each listing wherever the platforms
publish it.

The DSA requires online marketplaces to collect and display the identity of
professional "traders" — businesses that let accommodation as a trade. Both
platforms surface this as a per-listing business panel (company legal name,
registration number, registered address, contact details). This project
aggregates those per-listing public disclosures into one queryable dataset so
the professional-operator layer of the short-term rental market becomes visible.

Beyond the trader disclosure, a curation stage adds two further layers (the rest
of this document explains both):

- **Precise positions** — the raw platform coordinate (Airbnb fuzzes ~half of
  them by ~150 m; the rest expose an exact point) is improved by geocoding
  Booking addresses, reading Airbnb's own exact-location tag, and fusing matched
  listings across platforms and captures, with a per-listing `location_precision`
  / `est_accuracy_m` so you know which points are map-grade (§8).
- **Operator & property identity** — listings are resolved to real operators
  (`operator_id`, by shared registration/phone/email) and to the same physical
  flat across or within platforms (`property_group_id`), so "who runs what" and
  "how many distinct properties" are answerable (§6, §7).

**Scale of the April 2026 capture:**

| | Booking | Airbnb | Combined |
|---|---:|---:|---:|
| Listing rows | 4,796 | 6,105 | 10,901 |
| Distinct properties after cross-platform de-duplication | — | — | **9,363** |
| Classified "Professional" (trader) | 1,992 | 2,554 | 4,546 |
| With full company disclosure captured | 1,932 | 2,547 | 4,479 |
| Distinct operators identified | — | — | 3,151 |

The **current** capture — 21–22 May 2026, the figures used in §6–§8 and §12 — lands days after [EU Regulation 2024/1028](https://eur-lex.europa.eu/eli/reg/2024/1028/oj/eng) on short-term-rental data transparency became applicable (20 May 2026). That timing, with Romania's ongoing registration crackdown, makes the trader and registration layer especially timely.

One **row = one platform listing**. A row is *not* a property and *not* an
operator — see §6, the most important section for anyone counting things.

---

## 2. Provenance & audit trail

Every row carries its own provenance:

- **`first_seen_at`** — when the scraper first discovered the listing. Written
  once, never overwritten.
- **`scraped_at`** — when the row's data was last touched (including enrichment
  passes). So `first_seen_at` answers "when did this enter the dataset" and
  `scraped_at` answers "how fresh is this row".
- **`raw_json`** — the original API payload for the listing, stored verbatim, so
  any parsing decision can be re-checked against source.
- **`grid_cell_id`** — which geographic search tile returned the listing (see §8).

Run-level and curation audit lives in these tables:

- **`grid_progress`** — one row per (search tile, platform): status, result
  count, timestamps, error message. This is the cell-by-cell completion record.
- **`scrape_runs`** — one row per run: start/finish time, cell counts, and
  **`listings_dropped`** (see §5).
- **`position_observations`** — append-only ledger of every coordinate ever seen
  for a property (each platform, each capture, scraped + geocoded, with an
  uncertainty estimate). It is the substrate the position fusion reads (§8), and
  it lets repeat captures tighten a fuzzed point over time.
- **`geocode_cache`** — address → coordinate cache (`status` ok/not_found/failed,
  `attempts`, last-tried), so geocoding is rate-respectful and failures can be
  re-attempted across runs (`--regeocode`, §15).

The curation-derived columns (`operator_id`, `property_group_id`,
`latitude_best`/`longitude_best`, `location_precision`, `location_source`,
`est_accuracy_m`, `position_confidence`) are **recomputed** by the curation stage
(§4), so they reflect the latest curation rather than first capture and can be
regenerated on the existing DB with `--curate-only`.

> **Coverage caveat.** `first_seen_at`, `price_original` and `currency_original`
> are populated for listings captured from April 2026 onward; some earlier rows
> carry NULL in these columns rather than a guessed value.

---

## 3. Legal & ethical basis

- **The business data is public regulatory disclosure.** [DSA Article 30](https://www.eu-digital-services-act.com/Digital_Services_Act_Article_30.html)
  requires online marketplaces to collect and display — "in a clear, easily
  accessible and comprehensible manner" — on each trader's listing: name,
  address, phone, email, identification number, and trade-register number.
  This project aggregates what Booking and Airbnb already display under that
  obligation; it does not obtain anything the public cannot see on the live
  site. Art. 30(2) requires platforms only to make "best efforts" to verify
  what traders submit, which is why some rows are incomplete.
- **EU Reg 2024/1028** (applicable 20 May 2026) adds a per-unit
  registration-number requirement and a monthly data-sharing regime for
  short-term rental platforms — see [EUR-Lex](https://eur-lex.europa.eu/eli/reg/2024/1028/oj/eng).
  Romania separately requires a Ministry-of-Tourism classification certificate
  for STR units, with ANAF scrutinising approximately 23,000 hosts —
  [Romania Insider](https://www.romania-insider.com/apartment-rent-airbnb-romania-registration).
- **No private personal data is collected.** For non-professional ("individual"
  / "private") hosts the platforms disclose only a first name; that is all this
  dataset holds. No emails, phones or addresses are captured for individuals —
  only for registered businesses, and only because the platforms publish them.
- **Terms-of-service tension.** Both platforms' ToS discourage automated access.
  The collection method (internal-API access, browser automation) is described
  openly in §4–§5; a publisher should make its own call on this and may want to
  characterise the dataset as "aggregated public disclosures" rather than
  "scraped".
- **Verification is the journalist's job.** See §9 — company data is taken from
  the platforms' disclosures and is *not* independently verified.

---

## 4. How the data was collected

### Geographic coverage — H3 grid + recursive refinement

Both platforms' search APIs are bounding-box queries with a hard results cap
(~250 Booking, ~280 Airbnb). One city-wide query would silently truncate. So the
city is tiled with **H3 hexagons at resolution 7** (~5 km² cells); any cell that
hits the cap is **recursively subdivided** to resolution 8, 9, 10 until no
sub-cell is truncated (`src/grid/generator.py`). Dense neighbourhoods (Old Town,
Universitate) end up finely tiled; the rest of the city does not pay for it.

### Platform access

Neither platform has a public listings API, so both are reached through their
own **internal web APIs**, reverse-engineered from the site's own traffic:

- **Booking** — the `FullSearch` GraphQL endpoint, bounding-box queries, accessed
  with a real-Chrome TLS fingerprint (curl_cffi `chrome131` impersonation).
- **Airbnb** — the internal `StaysSearch` API via the `pyairbnb` library, parsed
  directly (the library's own parser has a bug that silently drops some
  listings), with a browser-automation fallback.

### Enrichment — cheap passes first, expensive passes last

The initial search is undated, so listings arrive without prices. Enrichment
fills the gaps in stages:

- **Booking** — up to 25 dated GraphQL re-queries spanning 3 days to 11 months
  out (a listing booked solid on one date may be free on another), then a
  browser pass over the listing's detail page for price + business disclosure.
- **Airbnb** — dated re-queries, then `pyairbnb.get_details()` for room data,
  then a browser pass over each listing page for the business + host disclosure.

Targeted re-fetch queries mean a re-run only touches listings that are still
incomplete — but re-fetching already-enriched listings recovers only
genuinely-missing data. Fields the platform simply never exposes (some Airbnb
host stats / room counts, Booking `max_guests` / VAT) are **not** recovered by
re-scraping; see §14.

### Where the business-disclosure data lives

The DSA trader data is not in the visible page markup — it is buried in each
page's serialized application state, in platform-specific places:

- **Booking** — the trader block is a structured `traderInfo` JSON object
  embedded in the page's serialized GraphQL state; it is extracted with a
  balanced-brace JSON parser. Booking gates its detail pages behind a JavaScript
  bot challenge, so they are fetched with a real headless browser rather than a
  plain HTTP client.
- **Airbnb** — the obvious `businessDetails` field in page state is empty; the
  actual trader disclosure text sits in a separate page-state section,
  `PROFESSIONAL_HOST_DETAILS_MODAL`. Whether a listing is Professional or
  Individual is read from `businessDetailsItem.action.screenId`
  (`PROFESSIONAL_HOST_DETAILS` vs `INDIVIDUAL_HOST_PROMPT`) — see §13.

This is internal-API extraction: the platforms can change these structures,
field names and locations at any time without notice, which is the standing
reason the scraper is fragile and every figure here is tied to a capture date.

### Curation — dedup, geocoding, position fusion

A final stage runs over the whole DB after scraping + enrichment (and is
re-runnable on its own, §15). It does not fetch listing content; it derives:

- **Identity dedup** — operators are union-found by shared registration / phone /
  email (`operator_id`); listings are grouped into the same physical flat,
  within or across platforms, by a layered matcher (`property_group_id`) — §6, §7.
- **Geocoding** — Booking street addresses are cleaned (apartment/floor noise
  stripped, ranges collapsed) and geocoded via OpenStreetMap/Nominatim
  (rate-limited, cached, drift-guarded so a mis-resolution > 2 km is discarded;
  failures re-tried with `--regeocode`). Airbnb, which exposes no address, is
  de-fuzzed by transferring its matched Booking twin's position.
- **Position fusion** — every coordinate for a property (both platforms, scraped
  + geocoded, and prior captures from `position_observations`) is fused by
  inverse-variance weighting into `latitude_best`/`longitude_best`, tagged with
  `location_precision` / `est_accuracy_m` / `position_confidence` — §8.
- **Verification** — identity keys cross-check the dedup, and cross-platform
  position disagreements > 1 km are flagged (`data/exports/dedup_metrics.json`).

---

## 5. Coverage & completeness

The grid + refinement design is built for completeness *within Bucharest's
bounding box*, but two honest caveats apply:

- **The denominator is unknown.** Neither platform publishes a total count of
  Bucharest listings, and listings churn daily. This dataset is "as complete as
  the platforms' own search would return on the capture dates" — it is **not**
  provably "every listing".
- **Some listings are dropped during parsing.** A listing is skipped if it has
  no usable id, has `(0,0)` coordinates, or raises an error mid-parse. Every
  drop is counted: each run records the totals in `scrape_runs.listings_dropped`
  and the parsers log a per-platform breakdown
  (`parsed=…, dropped=… (zero_coords=…, parse_error=…, missing_id=…)`). The
  drop counters are populated for captures from April 2026 onward — quote the
  actual `scrape_runs` figure for the run you are citing.

**Enrichment coverage in the April 2026 capture:**

| | Booking | Airbnb |
|---|---:|---:|
| Has a price | 3,300 / 4,796 (69%) | 6,105 / 6,105 (100%) |
| Business-type classified | 4,796 / 4,796 (100%) | 6,076 / 6,105 (99.5%) |
| Full company disclosure captured | 1,932 | 2,547 |

Booking's ~one-third price gap is real, not a bug: those listings have **no bookable
night** across any of the 25 tested date windows (booked solid, minimum-stay
rules, seasonal closure). 29 Airbnb listings (April figure; 2 in the current
capture after retry passes) never rendered their page state and stayed `Unknown`.

---

## 6. Unit of analysis — read this before counting anything

A **listing ≠ a property ≠ a host ≠ an operator.** Conflating these is the
single biggest way to get a number wrong.

- **Listing** — one row. The same flat can be listed more than once, on one
  platform or both.
- **Property** — one physical place. The May 2026 capture has 10,982 listing
  rows but an estimated **~8,036 distinct properties**, because many rows are the
  same flat appearing twice (cross-platform or within a platform) — 2,094
  property groups, see §7.
- **Host** — the account doing the letting (`host_id`, `host_name` — Airbnb).
- **Operator / trader** — the registered business behind a professional listing.
  One operator routinely runs many properties across many listings: the largest
  in this capture, **STRE Asset Management SRL**, is attached to **321 listings**
  across both platforms (the operator layer now merges its "STR"/"STRE" variants
  — see below). 103 operators have 10+ listings each.

The dataset gives you two tools for correct counting:

- **`property_group_id`** — listings judged to be the same physical flat (within
  *or* across platforms) share this id. Count distinct properties with
  `COUNT(DISTINCT COALESCE(property_group_id, id))`. (`cross_platform_group_id`
  is the subset of those groups that span both platforms.)
- **`operator_id`** / **`operators.csv`** — one row per operator. Operators are
  now keyed by a **normalised identity union-find** (registration / phone /
  email), so whitespace/prefix variants of one registration no longer split it.
  *Residual caveat:* Booking and Airbnb sometimes disclose the **same** operator
  in **different formats** (Booking the CUI, Airbnb the trade-register J-number),
  with no deterministic mapping between them — so one real operator can still
  appear as two `operator_id`s across platforms. The operator count is therefore
  a mild **over**-count; spot-check the big ones.

---

## 7. Deduplication — in full

Deduplication runs at two levels — **operator** and **property** — written to
`operator_id` and `property_group_id`. **Nothing is ever deleted**; both platform
rows are kept with their own price/host/business data, and only shared ids are
written so correct counts are possible.

**Operator layer (`operator_id`).** Listings that share a *normalised* identity
key — registration number, phone, or email — are unioned into one operator
(union-find; safe, because a shared registration/phone genuinely is one operator,
unlike GPS+name). This **fixes** the old un-normalised-key problem: the "STR" /
"STRE Asset Management" variants now collapse into a single operator of **321
listings** across both platforms. May 2026: 859 operators carry an `operator_id`,
103 of them with 10+ listings.

**Property layer (`property_group_id`)** groups listings that are the same
physical flat, within *or* across platforms, by three confidence tiers:

1. **Tier 0 — singleton identity.** A contact (phone/email/registration) mapping
   to exactly one Booking *and* one Airbnb listing links that pair directly — a
   single-property host — even when GPS/name disagree. Catches twins that
   proximity+name miss (Airbnb fuzzed far, different titles).
2. **Tier 1 — within an operator block.** Among one operator's listings, pairs
   within 250 m with matching name *or* room configuration are grouped.
3. **Tier 2 — spatial + name.** Outside operator blocks, pairs under 100 m with
   ≥80% name similarity.

Matching is greedy with a **clique check** (a listing joins a group only if
compatible with *every* member), which structurally prevents one operator's
shared phone from chaining its distant flats together. May 2026: 2,094 property
groups (1,495 spanning both platforms), giving an
estimated **~8,036 distinct properties** from 10,982 rows.

**Treat the property count as an estimate, bracketed on both sides.** Airbnb's
coordinate fuzz (§8) means a true twin can be missed; conversely Tier 1 can
*over-merge* two genuinely different units of one operator if they sit within
250 m with similar names. The earlier strict-1:1 method under-merged (~9,363
distinct); this layered method merges more aggressively (~8,000). The truth lies
between. **Verification:** for groups carrying identity keys on both sides, a
recall proxy confirms **100% (66/66)** of identity-confirmed cross-platform twins
were grouped; the precision proxy reads 0% only because the *same operator is
often disclosed with different identity formats on each platform* (Booking gives
the CUI, Airbnb the trade-register J-number) — so the "conflicts" it flags are
actually correct matches the identity check can't confirm, not bad merges. See
`data/exports/dedup_metrics.json` and `dedup_review.csv`.

Unlike [Inside Airbnb](https://insideairbnb.com/data-assumptions/), which links records by Airbnb listing id alone, this pipeline also resolves operators (by shared registration/phone/email) and groups the same physical flat across platforms — see above.

---

## 8. Geographic precision — what the coordinates actually mean

The **as-scraped** `latitude`/`longitude` are **not** a precise address:

- **Airbnb obfuscates *some* listings, not all.** The listing page carries
  `mapMarkerRadiusInMeters`, which directly reflects the host's
  [**"Precise location" vs "Approximate location"**](https://www.airbnb.com/help/article/2141)
  privacy choice in their settings. `0` = **Precise**: the coordinate is the
  true point — accurate to building level; only the street number is withheld
  until booking. `~152` (or `500`) = **Approximate**: the unit sits somewhere within a shaded circle of
  that radius — Airbnb jitters the point within ~150 m of the real address.
  [Inside Airbnb documents the same ~150 m fuzz plus individual-building
  scatter](https://insideairbnb.com/data-assumptions/). A Harvard study
  ([Szanto & Mehta 2018](https://techscience.org/a/2018100902/)) re-identified
  hosts from their fuzzed Airbnb coordinates 94% of the time — context for why
  cross-platform de-fuzzing works in this pipeline — and a reminder that
  `approximate` rows still carry meaningful location signal. In this capture
  Airbnb's radius was retrieved for **6,122 of 6,185 listings (~99%)**; **63
  blocked**. Of those captured, **2,978 (~49%) are radius-0 (exact)**. The tag
  is on the detail page only — captured by the radius pass below.
- **Booking is mixed.** Hotels carry a genuine geocoded location; apartments are
  often geocoded to a street, neighbourhood centroid or building cluster. But
  Booking's `raw_json` *does* carry a full street address (number / *strada* /
  *bloc* / *apartament*) for ~98% of listings — which the pipeline uses (below).
- **Trader address ≠ map pin.** `business_address` is the operator's *registered
  company address*, frequently nowhere near the flats it runs. Never plot it.

### What the curation stage does about it

A post-enrichment stage computes an **improved** position and **tags how much to
trust it**, while **preserving the originals** (`latitude`/`longitude` are never
overwritten). It works as follows:

1. **Geocode Booking street addresses** via OpenStreetMap/Nominatim (rate-limited,
   cached, persistent retry). The address is cleaned to street + number first
   (apartment-level noise stripped), giving a ~74% resolve rate. A geocode is
   **discarded if it lands > 2 km from the scraped point** (a sanity guard
   against mis-resolutions). → `latitude_geocoded`/`longitude_geocoded`.
2. **Use Airbnb's own exactness, then twins.** The capture pass records each
   Airbnb listing's `mapMarkerRadiusInMeters` (`--capture-airbnb-radius`):
   radius-0 listings keep their **own coordinate as precise** (it *is* the true
   point — verified equal to the PDP coordinate). A *fuzzed* Airbnb listing
   matched to a Booking twin (§7) instead inherits the twin's precise position.
3. **Fuse** all of a property's coordinates (both platforms, scraped + geocoded,
   *and* prior captures held in `position_observations`) by **inverse-variance
   weighting** — a precise point dominates a fuzzed one, and independent samples
   reduce error. → `latitude_best`/`longitude_best`, `est_accuracy_m` (the fused
   σ, in metres), and **`position_confidence` = clamp((150 − `est_accuracy_m`) /
   150, 0, 1)** — a 0–1 rescaling of that σ where **1 ≈ pinpoint** and **0 ≈ the
   ~150 m Airbnb fuzz** (so 0.78 ≈ ±33 m, 0.54 ≈ ±69 m). It carries no extra
   information beyond `est_accuracy_m` — it's just a friendlier handle; filter on
   either (`confidence ≥ 0.7` ≈ accurate to ~45 m or better).
4. **Tag** each position twice. The **curated** precision —
   `location_precision` (`exact` ≤ ~40 m σ, else `approximate`) and
   `location_source` (`geocoded_address` / `transferred_from_twin` /
   `platform_coord`) — describes the fused best position. The **platform-native**
   `platform_precision` records what the *platform itself* disclosed about the
   raw coordinate (Booking from address detail; Airbnb from the radius tag).
   They answer different questions — what curation derived vs what the platform
   said — so a fuzzed Airbnb listing fused to `exact` via a twin reads
   `location_precision=exact`, `platform_precision=approximate`.

**Result (May 2026 capture):** of 10,982 listings, **8,576 (78%) are `exact`**
(median accuracy ~21 m): on Booking, ~2,800 from geocoded street addresses plus
~1,700 already on a precise platform coordinate; on Airbnb, **~3,000 expose their
own exact location** (radius-0) and **~1,000** fuzzed ones are de-fuzzed via a
Booking twin. The remaining ~22% stay `approximate` (fuzzed
Airbnb with no twin / radius not captured, or un-geocodable Booking). Map/exports
use `latitude_best`/`longitude_best`. Address cleaning resolves ~78% of Booking
addresses (`--regeocode` re-tries failures); the Airbnb radius is captured for
**~99%** of listings (6,122/6,185; `--capture-airbnb-radius`, re-run to retry blocks),
**63 persistent blocks**.

### Position hierarchy — how sources are ranked and fused

Each candidate coordinate carries an uncertainty (σ); within a property group they
are **inverse-variance fused** (smaller σ = more weight), not picked by strict
priority. The σ ladder, most → least trusted:

| Source | σ | Notes |
|---|---:|---|
| Airbnb radius-0 own coordinate | **15 m** | host exposed the exact location (`mapMarkerRadiusInMeters = 0`) — **pinned** |
| Geocoded Booking address | **25 m** | Nominatim on the street address, drift-guarded ≤ 2 km |
| Booking own coordinate, street-level | **50 m** | has a precise address, not a stacked centroid |
| Fuzzed Airbnb | **~100 m** | radius not captured, or radius 152 → max(100, radius × 0.7) |
| Vague Booking / ≥3-stacked centroid / radius-500 Airbnb | **150 m+** | "Sector 3"-type address, shared coordinate, or radius 500 (~350 m) |

**Fusion** (`fuse_observations`): pool all of a group's observations (both
platforms, scraped + geocoded, *and* prior captures), reject any > 1 km from the
smallest-σ anchor, then inverse-variance average. The fused σ sets
`location_precision` (`exact` ≤ 40 m, else `approximate`), `est_accuracy_m` and
`position_confidence`. `location_source` is the dominant (smallest-σ)
observation's origin: `geocoded_address`, `transferred_from_twin` (a twin's
position won), or `platform_coord` (the listing's own coordinate won).

**Two rules override the pure fusion:**
- **radius-0 pin** — a radius-0 Airbnb fuses from its *own* observations only; the
  host-exposed coordinate is ground truth and is never moved by a group/twin.
- **> 1 km disagreement** — if a cross-platform group's Booking and Airbnb points
  disagree by more than 1 km the link is treated as suspect: each member keeps its
  own position (no transfer) and the group is flagged in `dedup_metrics.json`.

Two consequences worth noting: a **Booking listing reaches `exact` only** when its
address geocodes (σ 25) or a precise twin pulls the fused σ < 40 — on its own
coordinate (σ 50) it stays `approximate`; and **Booking exposes no per-listing
precision flag** (confirmed across the search payload and the detail page — unlike
Airbnb's radius), so Booking coordinate precision is *inferred* from address detail
+ stacking rather than read from the platform.

**How to use it:**
- **Map / cite a point only where `location_precision = 'exact'`** (optionally
  filter `position_confidence ≥ 0.7`). For `approximate` rows, fall back to
  neighbourhood-level reasoning and `grid_cell_id` (the H3 search tile, reliable
  to a few hundred metres even when the point is fuzzed).
- **A few links are wrong.** ~28 cross-platform groups whose Booking and Airbnb
  points disagree by > 1 km are flagged in `dedup_metrics.json →
  geo_conflict_groups`; the pipeline does **not** transfer positions across them,
  but treat those groups' positions with suspicion.

---

## 9. Business data is self-reported and unverified

The `business_*` fields are transcribed from the platforms' own DSA disclosure
panels. **Nothing in this pipeline cross-checks them against an external
register.** In particular:

- Company names, registration numbers and trade-register authorities are
  **whatever the host entered** and the platform displayed. They are not
  validated against Romania's trade register (ONRC) or any VAT registry.
- A registration number that looks well-formed may still be wrong, stale, or
  belong to a dissolved company.

**Before publishing anything about a specific company, verify it independently**
— ONRC (`portal.onrc.ro`) for Romanian companies, VIES for VAT numbers. Treat
this dataset as a *lead generator*, not a *source of record*, for company
identity.

---

## 10. Prices

- **Prices are snapshots, not "the" price.** Each price is for one arbitrary
  future night picked by the enrichment pass (3 days to 11 months out). It is
  indicative of asking price, not a booked transaction or an average.
- **Currency is normalised, transparently.** Booking returns Romanian listings
  in RON; the pipeline converts to EUR at a **fixed, dated reference rate**
  (`config/scraping.yaml` → `currency.ron_to_eur_rate`, with
  `ron_to_eur_rate_date`). It is not a daily FX feed.
- **The original value is preserved.** `price_original` + `currency_original`
  hold the as-scraped figure, so the conversion is transparent and can be redone
  with a better rate. (Populated for listings captured from April 2026 onward;
  some earlier rows carry NULL here.)

---

## 11. Data dictionary

| Column | Meaning | NULL means |
|---|---|---|
| `id` | Primary key, `"{platform}_{platform_id}"` | never NULL |
| `platform` | `booking` or `airbnb` | never NULL |
| `platform_id` | The platform's own listing id | never NULL |
| `name` | Listing title | never NULL |
| `latitude`, `longitude` | **As-scraped** coordinates, precision-limited (§8); for mapping use `*_best` | never NULL |
| `property_type` | e.g. apartment, hotel, guest house | not stated by platform |
| `star_rating` | Hotel star rating (Booking) | not a rated hotel |
| `review_score` | Guest review score, 0–10 normalised | no reviews yet |
| `review_count` | Number of reviews | no reviews yet |
| `price_per_night` | Indicative nightly price, EUR (see §10) | no bookable night found |
| `currency` | Always `EUR` after normalisation | — |
| `price_original` | As-scraped price before conversion | not captured for this row |
| `currency_original` | As-scraped currency (e.g. `RON`) | not captured for this row |
| `url` | Live listing URL | — |
| `thumbnail_url` | Listing photo URL | no photo found |
| `bedrooms`, `beds`, `bathrooms`, `max_guests` | Room configuration | not disclosed / not enriched |
| `is_superhost` | Airbnb Superhost flag | Booking, or not determined |
| `business_type` | `Professional` / `Private` / `Individual` / `Unknown` | not yet classified |
| `business_name` | Trader's legal/company name | not a trader, or not disclosed |
| `business_registration_number` | Trade-register / company number | not a trader, or not disclosed |
| `business_trade_register_name` | Issuing authority (e.g. ONRC) | as above |
| `business_vat` | VAT id | rarely disclosed |
| `business_address` | Trader's **registered** address — **not the property** (§8) | not a trader, or not disclosed |
| `business_email`, `business_phone` | Trader contact | not a trader, or not disclosed |
| `business_country` | Trader country | as above |
| `host_name` | Host display name (Airbnb) | Booking, or not captured |
| `host_id` | Platform user id of the host | as above |
| `host_response_rate`, `host_response_time`, `host_join_date` | Airbnb host profile stats | not captured / not on page |
| `operator_id` | Operator id — normalised identity union-find (§6, §7) | not a trader / no identity key |
| `property_group_id` | Same physical flat, within or across platforms (§7) | not matched to another listing |
| `cross_platform_group_id` | The subset of property groups that span both platforms (§7) | no cross-platform match |
| `latitude_best`, `longitude_best` | **Fused best position — use these for mapping** (§8) | no coordinate at all |
| `latitude_geocoded`, `longitude_geocoded` | Geocoded Booking street address (§8) | not geocoded / not Booking |
| `geocoded_address` | The cleaned address string that was geocoded (street + number, city) | not geocoded |
| `address_raw` | The full raw Booking property address (with bloc/scara/etaj), from `raw_json` — export-only | not Booking / no address |
| `location_precision` | `exact` (≤ ~40 m σ) / `approximate` (§8) | not curated |
| `location_source` | `geocoded_address` / `transferred_from_twin` / `platform_coord` | not curated |
| `est_accuracy_m` | Estimated position error in metres (fused σ) | not curated |
| `position_confidence` | 0–1 trust score = `clamp((150 − est_accuracy_m)/150, 0, 1)`; 1 ≈ pinpoint, 0 ≈ ~150 m (§8) | not curated |
| `platform_precision` | The platform's OWN `exact`/`approximate` for the raw coordinate (§8) — distinct from `location_precision` | radius not captured (Airbnb) |
| `airbnb_location_radius_m` | Airbnb `mapMarkerRadiusInMeters` — 0 = exact, ~152 = fuzzed (§8) | not Airbnb / not captured |
| `grid_cell_id` | H3 search tile that returned the listing | — |
| `first_seen_at` | First discovery time (immutable) | predates the column |
| `scraped_at` | Last-touched time | never NULL |
| `raw_json` | Original API payload, verbatim | — |

---

## 12. What you can — and cannot — claim

**Reasonable claims** (with the right hedging):

- ✅ "Around **4,600 listings** in Bucharest across Booking and Airbnb are
  operated by parties the platforms classify as professional businesses." —
  classification has residual error (§13) and unclassified rows exist.
- ✅ "Of the **~8,036 distinct properties** identified, a substantial share are
  run by professional operators rather than individual hosts." — "~" and
  "identified" are doing real work, and the estimate is method-dependent (§6, §7).
- ✅ "One operator, STRE Asset Management, is attached to around 320 listings." —
  round it, say "around", and verify the company independently (§9).
- ✅ "Professional operators with 10+ listings number in the low hundreds." —
  ~100 by the normalised operator key (§6).

**Claims to avoid:**

- ❌ "There are exactly N short-term rentals in Bucharest." — the denominator is
  unknown (§5) and rows ≠ properties (§6).
- ❌ "Company X owns these N flats." — the data shows X is the *disclosed trader*
  for N *listings*; ownership, and even the company's identity, must be verified
  (§9), and listing count ≠ property count.
- ❌ "This flat is at [exact address/point]." — Airbnb coordinates are fuzzed by
  ~150 m (§8).
- ❌ "Average nightly price in Bucharest is €X." — prices are arbitrary-date
  snapshots, ~a third of Booking listings have none, and the set is not a
  probability sample (§5, §10).
- ❌ Treating `business_address` as the property's location (§8).

When in doubt: **a row is a platform listing on a capture date — nothing more
until you've done the cross-checks this document points to.**

---

## 13. Classification: Professional vs Individual

`business_type` is derived from explicit platform signals:

- **Booking** — the listing's `traderInfo` block carries `isTrader` /
  `regulatorySubjectType`; `BUSINESS` → Professional, private individual →
  Private.
- **Airbnb** — the `businessDetailsItem.action.screenId`:
  `PROFESSIONAL_HOST_DETAILS` → Professional, `INDIVIDUAL_HOST_PROMPT` →
  Individual; corroborated by the disclosure-panel title text.

Residual uncertainty: classification depends entirely on the platforms' own
labelling — the signals above — which they can restructure or rename without
notice. It is only as accurate as what Booking and Airbnb themselves publish.
`Unknown` (just 2 Airbnb rows in this capture) means the page state never
rendered the signal — Airbnb anti-bot blocking, **not** a synonym for
"individual". Retry passes recover almost all of them (this capture went
1,771 → 301 → 2 over two passes).

---

## 14. Known limitations

- **Denominator unknown** — cannot prove the dataset is every Bucharest listing (§5).
- **Price gaps** — ~35% of Booking listings have no price; genuinely unbookable on tested dates (§5, §10).
- **Coordinates** — mixed as-scraped: Booking is street-or-better, ~49% of Airbnb expose an exact location (`mapMarkerRadiusInMeters=0`), the rest are ~150 m fuzz. Curation lifts **~78% to `exact`** (~21 m median) via geocoding + Airbnb's radius tag + cross-platform/temporal fusion; ~22% stay `approximate`. Map only `exact` rows; `platform_precision` says what the platform itself disclosed (§8).
- **Some gaps are genuinely unrecoverable, not extraction misses** — re-fetching already-enriched listings yields ~nothing: Airbnb partial-room counts (~354 missing bathrooms) and host stats (`host_response_rate` ~657, `host_join_date` ~885) simply aren't on those pages; Booking `max_guests`/`business_vat` are never exposed. Don't re-scrape to chase them.
- **Dedup is an estimate, bracketed both ways** — the layered method merges more aggressively than the old strict-1:1 (~8,000 vs ~9,363 distinct properties); Tier-1 can over-merge an operator's similar nearby units, Airbnb fuzz can miss twins (§7).
- **Business data is unverified** — not checked against ONRC/VIES (§9).
- **Operator linking** — normalised via identity union-find, but the same operator can still split across platforms when Booking and Airbnb disclose different ID formats (CUI vs J-number) (§6).
- **Airbnb `Unknown` (2)** — anti-bot blocking; recovered to near-zero by retry passes (1,771 → 301 → 2); not "individual" (§13).
- **~28 cross-platform groups disagree > 1 km** on position — flagged in `dedup_metrics.json`; positions not transferred across them (§8).
- **Self-reported everything** — names, prices, registration numbers, host stats are all what the host entered and the platform displayed.

---

## Sources & related work

- [DSA Article 30](https://www.eu-digital-services-act.com/Digital_Services_Act_Article_30.html) — the EU provision that requires online marketplaces to collect and display trader identity on each listing; grounds the legal basis for capturing business-disclosure fields.
- [EU Regulation 2024/1028](https://eur-lex.europa.eu/eli/reg/2024/1028/oj/eng) — short-term rental data-transparency regulation applicable from 20 May 2026; adds per-unit registration number and monthly platform data-sharing requirements.
- [Romania STR registration (Romania Insider)](https://www.romania-insider.com/apartment-rent-airbnb-romania-registration) — summary of Romania's Ministry-of-Tourism classification certificate requirement and ANAF scrutiny of approximately 23,000 hosts.
- [Airbnb "Precise location" vs "Approximate location" help page](https://www.airbnb.com/help/article/2141) — Airbnb's own documentation of the host privacy choice reflected in `mapMarkerRadiusInMeters`; grounds the radius-0 = exact interpretation in §8.
- [Inside Airbnb data assumptions](https://insideairbnb.com/data-assumptions/) — documents Airbnb's ~150 m fuzz and individual-building coordinate scatter; grounds the comparison of deduplication approaches in §7 and the fuzz discussion in §8.
- [Szanto & Mehta 2018 — "A Host of Troubles"](https://techscience.org/a/2018100902/) — Harvard study finding 94% re-identification of Airbnb hosts from their fuzzed coordinates; context for why cross-platform de-fuzzing is feasible and a privacy caveat on `approximate` rows (§8).

---

## 15. Reproducing a run

```bash
# Full pipeline: grid → scrape → refine → enrich → curate (dedup + geo) → export
python -m src.orchestrator

# Enrichment only (re-uses listings already in the DB; also re-curates + exports)
python -m src.orchestrator --enrich-only

# Curation only — re-run operator/property dedup + geocode + position fusion on
# the existing DB, no scraping (geocodes are cached, so this is fast)
python -m src.orchestrator --curate-only

# Re-attempt cached geocode failures (e.g. after improving address cleaning),
# then re-curate + re-export
python -m src.orchestrator --regeocode --curate-only

# Scope to one platform
python -m src.orchestrator --airbnb-only
python -m src.orchestrator --booking-only
```

Outputs land in `data/exports/`: `listings.csv`, `listings.geojson` (both carry
every field in §11), `operators.csv` (one row per operator, §6), and
`bucharest_map.html` (interactive map). Configuration: `config/bucharest.yaml`
(city bounds, grid resolutions, results caps) and `config/scraping.yaml`
(delays, the dated FX rate, the business-data toggles).
