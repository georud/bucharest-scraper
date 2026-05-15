# Methodology & Data Notes

A guide for anyone analysing or publishing from this dataset. It explains where
the data comes from, how it was collected, how complete it is, what each field
means, and — most importantly — **what you can and cannot safely claim from it**.

Figures below are from the **April 2026 capture** of Bucharest. They are a
snapshot; re-running the scraper produces a fresh one.

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

**Scale of the April 2026 capture:**

| | Booking | Airbnb | Combined |
|---|---:|---:|---:|
| Listing rows | 4,796 | 6,105 | 10,901 |
| Distinct properties after cross-platform de-duplication | — | — | **9,363** |
| Classified "Professional" (trader) | 1,992 | 2,554 | 4,546 |
| With full company disclosure captured | 1,932 | 2,547 | 4,479 |
| Distinct operators identified | — | — | 3,151 |

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

Run-level audit lives in two tables:

- **`grid_progress`** — one row per (search tile, platform): status, result
  count, timestamps, error message. This is the cell-by-cell completion record.
- **`scrape_runs`** — one row per run: start/finish time, cell counts, and
  **`listings_dropped`** (see §5).

> **Coverage caveat.** `first_seen_at`, `price_original` and `currency_original`
> are populated for listings captured from April 2026 onward; some earlier rows
> carry NULL in these columns rather than a guessed value.

---

## 3. Legal & ethical basis

- **The business data is public regulatory disclosure.** Under the DSA the
  platforms themselves publish trader identity on the listing page. This project
  aggregates what Booking and Airbnb already display — it does not obtain
  anything the public cannot see on the live site.
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
incomplete.

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

Booking's 31% price gap is real, not a bug: those listings have **no bookable
night** across any of the 25 tested date windows (booked solid, minimum-stay
rules, seasonal closure). 29 Airbnb listings never rendered their page state
even after retries and stayed `Unknown`.

---

## 6. Unit of analysis — read this before counting anything

A **listing ≠ a property ≠ a host ≠ an operator.** Conflating these is the
single biggest way to get a number wrong.

- **Listing** — one row. The same flat can be listed more than once, on one
  platform or both.
- **Property** — one physical place. The April 2026 capture has 10,901 listing
  rows but an estimated **9,363 distinct properties**, because 3,076 rows are
  the same flat appearing on both Booking and Airbnb (1,538 cross-platform
  pairs — see §7).
- **Host** — the account doing the letting (`host_id`, `host_name` — Airbnb).
- **Operator / trader** — the registered business behind a professional listing
  (`business_registration_number`, `business_name`). One operator routinely runs
  many properties across many listings: the largest in this capture,
  **STR Asset Management** (reg. RO41137103), is attached to **198 listings**
  across both platforms. 121 operators have 10+ listings each.

The dataset gives you two tools for correct counting:

- **`cross_platform_group_id`** — listings judged to be the same physical flat
  across Booking and Airbnb share this id. Count distinct properties with
  `COUNT(DISTINCT COALESCE(cross_platform_group_id, id))`.
- **`operators.csv`** / `get_operator_summary()` — one row per operator, with
  listing count and platforms. *Caveat:* operators are keyed on
  `business_registration_number` and that field is **not normalised** — e.g.
  `RO41137103` and `RO 41137103` (a stray space) are currently treated as two
  operators. Treat the operator count as a slight over-count and spot-check the
  big ones.

---

## 7. Deduplication — in full

Three layers, and what each does *not* catch:

1. **Exact platform-id** — a listing returned twice by overlapping search tiles
   is collapsed by its platform id. Reliable and global across a run.
2. **Spatial + fuzzy-name, within platform** — within a search tile, listings
   under 50 m apart with >70% name similarity are treated as one. Catches a
   property that the platform returns under variant ids. Runs **per tile**, so a
   variant-id duplicate split across two tiles can survive.
3. **Cross-platform linking** (`cross_platform_group_id`) — after all scraping,
   each Booking listing is paired with the *single* Airbnb listing most likely
   to be the same flat: candidate pairs (within 100 m, >72% name similarity) are
   ranked by name similarity and accepted greedily, **1:1** — a listing joins at
   most one pair, and every group is exactly one Booking + one Airbnb row. No
   transitive chaining. April 2026: 1,538 cross-platform pairs linking 3,076
   listings. **Nothing is deleted** — both platform rows are kept, on their own
   platforms, with their own price/host/business data; only a shared id is
   written so distinct-property counts are possible.

**Cross-platform linking is best-effort, and you must treat it as such.**
Because Airbnb fuzzes coordinates (§8), a true Booking↔Airbnb pair whose Airbnb
point drifted beyond 100 m is *missed*, and two genuinely different flats in the
same building can be *wrongly linked*. The 1:1 rule deliberately **under-merges**
— a flat listed twice on one platform plus once on the other gets only its best
cross-platform pair linked — because that is far safer than over-merging. Name
similarity is the corroborating signal but it is not proof. The 9,363
distinct-property figure is therefore an **estimate** — directionally a much
better count than 10,901, but not exact.

---

## 8. Geographic precision — what the coordinates actually mean

A listing's latitude/longitude is **not** a precise address.

- **Airbnb deliberately obfuscates location.** For an unbooked listing the API
  returns an *approximate* coordinate — Airbnb jitters the true point within a
  ~150 m circle and only reveals the exact address after booking. Every Airbnb
  coordinate in this dataset is that obfuscated point. An Airbnb pin can be
  100–200 m off; a cluster of Airbnb pins on one building is an artefact.
- **Booking is mixed.** Hotels carry a genuine geocoded location. Apartments and
  vacation rentals are often geocoded to a street, a neighbourhood centroid or a
  building cluster — and the API does not flag which.
- **The H3 cell is more reliable than the point.** `grid_cell_id` places a
  listing inside a hexagon of a few hundred metres *even when the lat/lng is
  fuzzed*, because it comes from the search tile that returned the listing.
  **Aggregate / heat-map analysis at cell resolution is sound; point-level "this
  flat is at this spot" analysis is not** — least of all for Airbnb.
- **Trader address ≠ map pin.** `business_address` is the operator's *registered
  company address*. A property manager's registered office is frequently nowhere
  near the flats it runs. Do not plot `business_address` as the listing location.

**Recommendation:** treat coordinates as neighbourhood-level for Airbnb and
property-or-better for Booking hotels; use `grid_cell_id` for spatial
aggregation.

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
| `latitude`, `longitude` | Coordinates — **see §8, precision-limited** | never NULL |
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
| `cross_platform_group_id` | Shared id for the same flat across platforms (§7) | no cross-platform match found |
| `grid_cell_id` | H3 search tile that returned the listing | — |
| `first_seen_at` | First discovery time (immutable) | predates the column |
| `scraped_at` | Last-touched time | never NULL |
| `raw_json` | Original API payload, verbatim | — |

---

## 12. What you can — and cannot — claim

**Reasonable claims** (with the right hedging):

- ✅ "At least **4,546 listings** in Bucharest across Booking and Airbnb are
  operated by parties the platforms classify as professional businesses." —
  it's a floor; classification has residual error (§13) and unclassified rows
  exist.
- ✅ "Of the ~9,363 distinct properties identified, a substantial share are run
  by professional operators rather than individual hosts." — "~" and
  "identified" are doing real work; see §6, §7.
- ✅ "One operator, STR Asset Management, is attached to ~200 listings." — round
  it, say "around", and verify the company independently (§9).
- ✅ "Professional operators with 10+ listings number in the low hundreds." —
  121 by the raw key, slightly fewer after de-duplicating registration-number
  variants.

**Claims to avoid:**

- ❌ "There are exactly N short-term rentals in Bucharest." — the denominator is
  unknown (§5) and rows ≠ properties (§6).
- ❌ "Company X owns these N flats." — the data shows X is the *disclosed trader*
  for N *listings*; ownership, and even the company's identity, must be verified
  (§9), and listing count ≠ property count.
- ❌ "This flat is at [exact address/point]." — Airbnb coordinates are fuzzed by
  ~150 m (§8).
- ❌ "Average nightly price in Bucharest is €X." — prices are arbitrary-date
  snapshots, 31% of Booking listings have none, and the set is not a
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
`Unknown` (29 Airbnb rows) means the page state never rendered the signal — it
is not a synonym for "individual".

---

## 14. Known limitations

- **Denominator unknown** — cannot prove the dataset is every Bucharest listing (§5).
- **Price gaps** — 31% of Booking listings have no price; genuinely unbookable on tested dates (§5, §10).
- **Coordinates are imprecise** — Airbnb ~150 m fuzz, Booking mixed (§8).
- **Cross-platform linking is best-effort** — strict 1:1 pairs, deliberately under-merging; false negatives and positives both possible; 9,363 distinct properties is an estimate (§7).
- **Business data is unverified** — not checked against ONRC/VIES (§9).
- **Operator keys not normalised** — registration-number whitespace/prefix variants split one operator into several (§6).
- **`first_seen_at` / `price_original` / `currency_original`** — populated from the April 2026 capture onward; NULL on older rows.
- **~62 Booking "Professional" listings** carry `contactDetails: null` upstream — flagged as traders but with no disclosed company data.
- **Self-reported everything** — names, prices, registration numbers, host stats are all what the host entered and the platform displayed.

---

## 15. Reproducing a run

```bash
# Full pipeline: grid → scrape → refine → enrich → cross-platform link → export
python -m src.orchestrator

# Enrichment only (re-uses listings already in the DB)
python -m src.orchestrator --enrich-only

# Scope to one platform
python -m src.orchestrator --airbnb-only
python -m src.orchestrator --booking-only
```

Outputs land in `data/exports/`: `listings.csv`, `listings.geojson` (both carry
every field in §11), `operators.csv` (one row per operator, §6), and
`bucharest_map.html` (interactive map). Configuration: `config/bucharest.yaml`
(city bounds, grid resolutions, results caps) and `config/scraping.yaml`
(delays, the dated FX rate, the business-data toggles).
