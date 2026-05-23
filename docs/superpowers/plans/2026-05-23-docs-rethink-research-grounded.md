# Research-Grounded Docs Rethink — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rethink and update `METHODOLOGY.md` (tracked) and `NEW_VS_OLD.md` (untracked) — ground the trader/legal framing in DSA + EU/Romania law, reframe the Airbnb position story around Airbnb's documented host privacy setting, add the regulatory-moment context, and refresh every figure to one canonical set.

**Architecture:** Pure documentation edits. No code, no tests. "Verification" = figure consistency, every external claim carries a cited source, and Markdown renders. Source of truth for numbers + citations is the spec (`docs/superpowers/specs/2026-05-23-docs-rethink-research-grounded.md`) — repeated below so the executor needs no other file.

**Tech Stack:** GitHub-flavoured Markdown; `grep` for verification.

---

## Canonical figures (use verbatim in BOTH docs)

- Listings **10,982** (Booking 4,797 / Airbnb 6,185).
- `exact` positions **8,576 (78%)**, `approximate` 2,406, median accuracy **~21 m**.
- Airbnb radius captured **6,122/6,185 (~99%)**, **63 blocked**; **radius-0 = 2,978 (~49% of captured)**.
- Exact-position sources: Booking geocoded address **~2,800** + Booking precise own coord **~1,700**; Airbnb own coord (radius-0 Precise + group fusion) **~3,000** + Airbnb via Booking twin **~1,000**.
- Operators **859** (103 with ≥10 listings); distinct properties **~8,036**; property groups **2,094** (1,495 cross-platform).
- Trader: Professional **4,622** (Booking 1,983 / Airbnb 2,639); Booking Private 2,811; Airbnb Individual 3,544; **Unknown 2**. `business_name` 4,472; `host_name` 6,183.
- OLD baseline (April, `backup-20260515-091053`): 10,901 listings; 0 exact; ~9,363 distinct (strict 1:1); 3,151 crude-key operators; Unknown 29.

## Sources (cite as Markdown links; never assert beyond what each supports)

- DSA Art. 30: `https://www.eu-digital-services-act.com/Digital_Services_Act_Article_30.html`
- Airbnb Precise/Approximate location: `https://www.airbnb.com/help/article/2141`
- Inside Airbnb data assumptions (150 m, individual-building scatter): `https://insideairbnb.com/data-assumptions/`
- "A Host of Troubles" 94% re-identification: `https://techscience.org/a/2018100902/`
- EU Reg 2024/1028 (applicable 20 May 2026): `https://eur-lex.europa.eu/eli/reg/2024/1028/oj/eng`
- Romania STR registration regime: `https://www.romania-insider.com/apartment-rent-airbnb-romania-registration`

---

### Task 1: METHODOLOGY.md (tracked — commit at end)

**Files:** Modify `METHODOLOGY.md` (§1 intro, §3 legal basis, §7 dedup, §8 geo, + new "Sources & related work" before §15/reproducing).

- [ ] **Step 1 — §1 regulatory moment.** After the §1 capabilities paragraph, add one sentence: this capture (21–22 May 2026) is a snapshot taken days after **EU Reg 2024/1028** on short-term-rental data became applicable (20 May 2026) and amid Romania's registration crackdown — so the trader/registration layer is timely. Cite the EUR-Lex link.

- [ ] **Step 2 — §3 legal basis: ground in DSA Art. 30.** Rewrite the first bullet to state that **DSA Article 30** requires marketplaces to collect and display on the listing, "in a clear, easily accessible manner," the trader's name, address, phone, email, ID and **trade-register number** — i.e. our captured business fields *are* the Art-30 disclosure — and that Art. 30(2) requires only "best efforts" to verify, which is why some rows are incomplete. Cite the DSA Art. 30 link. Add a bullet: **EU Reg 2024/1028** (applicable 20 May 2026) adds a per-unit registration-number + monthly data-sharing regime (cite EUR-Lex); and Romania requires a Ministry-of-Tourism classification certificate, with ANAF scrutinising ~23,000 hosts (cite Romania Insider). Keep the existing "verification is the journalist's job / unverified" caveat.

- [ ] **Step 3 — §7 dedup benchmark.** Add one sentence near the end of §7: unlike Inside Airbnb (which links only by Airbnb listing id), this pipeline adds operator union-find over shared identity keys and cross-platform property grouping. Cite Inside Airbnb link.

- [ ] **Step 4 — §8 geo: ground the radius in Airbnb's own setting.** In the §8 Airbnb bullet and the curation step, state that `mapMarkerRadiusInMeters` *is* Airbnb's host **"Precise location" vs "Approximate location"** setting: **0 = Precise** (the listed coordinate is the true point, only the street number withheld until booking), **~152/500 = Approximate** (a shaded circle the unit sits within). Cite Airbnb Help 2141. Add: Inside Airbnb documents the same ~150 m fuzz and individual-building scatter (cite); and a Harvard study found the fuzz weak enough to re-identify the host **94%** of the time (cite "A Host of Troubles") — context for why cross-platform de-fuzzing works **and** a privacy caveat. Refresh figures to **8,576 (78%) / ~21 m**, radius captured **~99% (63 blocked)**, radius-0 **~49%**, and the 4-source breakdown above.

- [ ] **Step 5 — new "Sources & related work" section.** Add a short section (before §15 "Reproducing a run") listing the six sources above as Markdown links with a one-line note each (what it grounds).

- [ ] **Step 6 — figure sweep.** `grep -nE "8,582|8,501|~?77%|~?98%|6,635|2,914|~400 persistent" METHODOLOGY.md` and reconcile any stale figures to the canonical set (e.g. 78%, ~99%, 63 blocked). Re-read §1/§3/§7/§8 for internal consistency.

- [ ] **Step 7 — verify + commit.**
  Run: `grep -nE "Article 30|2024/1028|Precise location|94%|8,576" METHODOLOGY.md` → expect hits in §3/§8/Sources.
  Run: `python -c "import markdown" 2>/dev/null` is not required; just confirm tables/headings look intact by eye.
  Then: `git add METHODOLOGY.md && git commit -m "docs: ground methodology in DSA/EU/Romania law + Airbnb's Precise/Approximate model; refresh figures"`

---

### Task 2: NEW_VS_OLD.md (untracked — DO NOT commit)

**Files:** Modify `NEW_VS_OLD.md` (§1 headline, §2 at-a-glance, §3 positions, new "Regulatory moment" section, §4/§5/§6 figures).

- [ ] **Step 1 — §1 + §3 reframe positions around the privacy setting.** Lead the position story with: Airbnb's obfuscation is a host **privacy setting** (Precise vs Approximate location), and **~49% of captured Bucharest Airbnb listings are "Precise" (radius-0)** — so the exact pin was *published, not inferred*. The new dataset reads that signal, geocodes Booking street addresses, and de-fuzzes the rest via cross-platform twins → **8,576 (78%) exact, ~21 m median**. Cite Airbnb Help 2141 and the Inside Airbnb 150 m / 94% context. Use the 4-source breakdown above.

- [ ] **Step 2 — new "Regulatory moment" section** (after §3 or as a new §). Three sentences: the capture lands 3 days after **EU Reg 2024/1028** became applicable (20 May 2026; per-unit registration numbers + monthly data-sharing), **DSA Art. 30** is why the trader data is published at all, and Romania's registration crackdown (ANAF, ~23,000 hosts) is the local backdrop — so this snapshot documents the market at a regulatory inflection point. Cite EUR-Lex, DSA Art. 30, Romania Insider.

- [ ] **Step 3 — refresh all figures** in §2 (at a glance), §4 (operators 859 / distinct ~8,036), §5, §6 to the canonical set (78% exact / ~21 m; keep the 3,151→859 "different metric" caveat). Update the "Generated" date to 2026-05-23.

- [ ] **Step 4 — verify (no commit).**
  Run: `grep -nE "Precise location|2024/1028|8,576|78%|~21 m" NEW_VS_OLD.md` → expect hits.
  Run: `git status --short` → `NEW_VS_OLD.md` must be **absent** (gitignored/untracked); do **not** add or commit it.

---

## Self-review

- **Spec coverage:** §1 regulatory moment (T1S1) ✓; §3 DSA/EU/Romania (T1S2) ✓; §7 Inside Airbnb benchmark (T1S3) ✓; §8 Precise/Approximate + 150 m + 94% + figures (T1S4) ✓; Sources section (T1S5) ✓; NEW_VS_OLD reframe + regulatory moment + figures (T2) ✓; canonical figures + sources repeated above ✓; METHODOLOGY committed / NEW_VS_OLD untracked ✓.
- **Placeholder scan:** none — each step names exact sections, facts, figures, sources.
- **Consistency:** the same canonical figures + six sources drive both docs; the exact-source breakdown sums to 8,576.
