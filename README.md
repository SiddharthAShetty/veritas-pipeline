# Veritas Claims — Medical Data Standardisation Pipeline

A config-driven prototype that ingests messy, clinic-specific JSON medical reports,
standardises them into a canonical schema, validates results against reference ranges,
loads them into a database, and surfaces everything through a small operational UI.

See `docs/architecture.md` for the solution architecture and `docs/assumptions.md` for the reasoning behind every non-obvious design decision.

## Live Demo

🌐 **https://veritas-pipeline.onrender.com/**

## Setup

```bash
python -m venv .venv 
venv\Scripts\activate
pip install -r requirements.txt
```

## Run the pipeline

```bash
python src/run_pipeline.py
```

This reads every `sample-data/{clinic_id}/{date}/*.json` file, standardises and validates
each record, and loads the result into `output/veritas.db` (SQLite). It's idempotent —
run it as many times as you like; re-processing the same files overwrites rows rather than
duplicating them. Errors go to `output/errors/dead_letter.jsonl`; raw JSON + transformed
output for every document is retained under `output/audit/`.

**Note on removing/changing sample data:** the DB persists between runs and only ever
inserts/updates rows — it never deletes a clinic's or document's rows just because that
file disappeared from `sample-data/` (by design, matching FR-4.3's audit-trail intent: in
production you don't want yesterday's claims data to vanish because a file got deleted from
the bucket). If you're iterating locally and want the DB/dashboard to reflect *exactly* what's
currently in `sample-data/` — e.g. after removing a clinic folder — use:

```bash
python src/run_pipeline.py --reset
```

which clears the previous DB/audit/error output before running, rather than manually
deleting `output/`.

```
python src/run_pipeline.py
...
Files seen: 10 | processed: 7 | failed: 1 | duplicates suppressed: 2
Rows loaded: 474 | flagged (outlier/invalid/out-of-range): 123
Unresolved test names this run: 43
DB: output/veritas.db | Errors: output/errors/dead_letter.jsonl | Audit trail: output/audit
```

`sample-data/apollo_diagnostics/` now contains **all 5 of the real JSON files** provided
(1 lab-report-only, 2 discharge-summary-only — one a genuine duplicate of the other under
different document/claim ids, and 2 mixed lab+discharge). The other four clinic folders
(`medplus_labs`, `citycare_hospital`, `wellness_diagnostics`, `sunrise_clinic`) remain
synthetic, since the real samples all share one envelope format and don't exercise the
schema-flexibility requirement (FR-1.3) on their own — see `docs/assumptions.md`.

Most of the 43 unresolved names aren't lab tests at all — the real files mix clinical-exam
narrative ("Fever", "Headache", "P/A", "CVS"), panel headers ("LIVER FUNCTION TEST(LFT)"),
and lab-method annotations into the same `report_details` rows as actual test results.
That's genuine source-data messiness (see `docs/assumptions.md`), not a matcher bug — and
it dropped from 106 unresolved on first pass over the real data to 43 after two rounds of
dictionary curation, which is exactly the workflow NFR-4.1 assumes exists in production.

## Run the UI

```bash
python ui/app.py
```

Then open `http://localhost:5000`. Four screens:
- **Dashboard** — last run's file/row/error counts, result-status breakdown
- **Flagged Queue** — every Outlier / Invalid / Above-Range / Below-Range row, filterable by clinic and flag type
- **Clinics** — per-clinic ingestion and data-quality stats (failure rate, dedup rate, unresolved-name count)
- **Record Inspector** — search by document/UHID/patient name, see raw JSON next to the standardised rows for that document

## FR-2.2 — the fixed 5-column schema

`src/db_loader.py` stores results in the working `clinical_records` table: one row per
(report, test) pair, ~50 columns total covering demographics, discharge details, medicines,
and lineage. But FR-2.2 specifically asks for a **fixed 5-column schema per test** —
`Test_Name`, `Test_Name_Result`, `Test_Name_Range`, `Test_Name_Unit`, `Test_Name_Analytics`,
the same 5 column names reused for every test, blank where a value is missing.

That's exposed as its own SQL view, `vw_test_result`:
```bash
python src/run_pipeline.py --reset
sqlite3 output/veritas.db "SELECT Test_Name, Test_Name_Result, Test_Name_Range, Test_Name_Unit, Test_Name_Analytics FROM vw_test_result LIMIT 10"
```
`document_id`/`clinic_id`/`patient_name`/`uhid` are also included in the view so you can tell
whose test and which report a row belongs to — those identify the row, they aren't part of
the test's own 5-column tuple. See `docs/assumptions.md` for why the richer table is still
the primary operational store the rest of the UI queries against.

## Checking for duplicates yourself

Two different kinds of duplication can show up, and the pipeline checks for both:

**Across files** (the same report resubmitted as a new file) — already suppressed before
loading; check the pipeline run summary's "duplicates suppressed" count, or
`Ingestion.duplicates_suppressed` in code.

**Within one file** (the same result restated on a different page of one report — a real
pattern found in the Apollo sample data) — flagged, not suppressed, since a genuinely
repeated measurement is rare but possible:
```bash
sqlite3 output/veritas.db "SELECT document_id, test_name_canonical, result_text, page_number, flag_reason FROM clinical_records WHERE duplicate_within_report = 1"
```
or in the UI: Flagged Queue → filter by "Within-report duplicate".

To audit an already-exported CSV for either kind yourself (e.g. after pulling data out of
the DB), the check is: group by `(document_id, test_name_canonical, result_text)` and look
for more than one distinct `page_number` — that's what
`_flag_within_document_value_repeats()` in `src/pipeline.py` does at load time.

## Run the tests

```bash
python -m pytest tests/ -v
```

41 tests across two files: `test_standardisation.py` (unit tests for FR-2.1–2.6 and
FR-3.1–3.4 in isolation) and `test_pipeline_integration.py` (end-to-end against the real
`sample-data/` fixtures — including a regression test for a genuine data-quality defect
found in the provided sample JSON, see `docs/assumptions.md`).

## Project layout

```
config/
  test_dictionary.json      # canonical test names + aliases (FR-2.1)
  reference_ranges.json     # medically-informed ranges + outlier bounds (FR-3.1/3.2)
  unit_conversions.json     # unit spelling -> canonical unit + factor (FR-2.4)
  medicine_mapping.json     # brand -> generic (FR-2.6)
  clinics/*.json            # one file per clinic: field paths, date format, envelope shape
sample-data/
  {clinic_id}/{date}/*.json # 5 clinics, 6 files, deliberately varied formats + 1 malformed
src/
  ingestion.py               # FR-1.1/1.2/1.3
  standardisation.py         # FR-2.1-2.6
  validation.py              # FR-3.1-3.4
  pipeline.py                # orchestrates the above per record, config-driven throughout
  db_loader.py                # FR-4.1/4.2/4.3, idempotent upsert (NFR-3.2)
  run_pipeline.py             # entry point
ui/
  app.py + templates/         # FR-5.1-5.4
tests/
docs/
  architecture.md + architecture_diagram.svg
  assumptions.md
```

## Key design decisions

- **Clinic identity comes from the GCS-style folder path** (`{clinic_id}/{date}/{file}.json`),
  not from sniffing the JSON's shape. Two clinics can share an upstream OCR vendor's exact
  envelope format (this repo's `apollo_diagnostics` and `wellness_diagnostics` do), so shape
  alone can't reliably identify a clinic. Shape-checking is kept as a secondary sanity check
  that logs a warning without blocking.
- **Every field lookup goes through a config-declared dotted path** (`pathutil.get_path`),
  never a clinic-specific `if clinic_id == "apollo": ...` branch. Onboarding a clinic with a
  new field layout is adding a JSON file under `config/clinics/`, full stop.
- **Rows use a deterministic id** (hash of `document_id + record_type + test_name_canonical + result_value + unit_canonical`)
  instead of a random UUID, so `INSERT OR REPLACE` makes re-running the pipeline idempotent
  (NFR-3.2) without a separate "have I seen this before" lookup pass.
- **Test-name matching has four tiers, in order: exact alias → fuzzy (`difflib`) → suffix
  match → prefix match.** The real sample data revealed a systematic OCR defect — leading
  characters dropped from test names (`aemoglobin`, `tal WBC Count`, `sophils` for
  Hemoglobin/Total WBC Count/Basophils) — that plain fuzzy matching under-serves on short
  words. The suffix/prefix tiers exist because real data showed they were needed.
- **Deduplication checks an identity key (document_id/correlation_id) *and* a content hash
  of the clinical payload, independently.** The real sample set includes two files with
  byte-identical clinical content submitted under different document_id, correlation_id,
  *and* claim_no — an identity-key-only dedup would have loaded it twice.
- **`claim_no`, `nt_code`, `consumer_client_id`, and `destination_identifier` are captured
  from `metaDetails` on every row.** For a claims company, the foreign key back to the
  insurance claim matters as much as the clinical data itself. 
- **Unresolved test names and mapping failures are logged, never silently dropped.** A row
  with a test name the dictionary can't match still loads, tagged `test_name_canonical =
  UNRESOLVED`, and the name is queued for dictionary curation — matching how NFR-4.1's
  "98% resolved within 30 days" target implies an ongoing curation process exists.
- **Failure isolation is per-row and per-file, not per-batch.** One malformed file doesn't
  block the other five; one row with a non-numeric result where a number was expected
  doesn't block the rest of that file's rows — see `validation.py`'s `Invalid` classification
  vs. `ingestion.py`'s file-level dead-letter path.

## Known limitations

- Reference ranges are simplified adult-general-population values, not clinically validated
  or age/sex-stratified (see `docs/assumptions.md`).
- Composite-field splitting (`"Neutrophil - 72.4, Lymphocyte - 23.5"` → separate rows) uses a
  regex heuristic tuned to the patterns in the provided sample data; a sufficiently different
  delimiter convention from a new clinic would need a small config extension.
- If a composite field's value and a standalone row's value for the same test *disagree*,
  neither is currently flagged as contradictory — only *identical* within-document repeats
  are caught (see "within-document duplicates" above). Worth hardening before production
  (see Assumptions).
- No real GCS/Pub-Sub/Cloud Run wiring — the ingestion module reads a local folder using the
  same path convention GCS would use, so this is a contained swap, not a redesign (see
  `docs/architecture.md`).
- PII masking (NFR-4.3) isn't implemented since the provided sample data arrives pre-redacted;
  there was nothing to mask against.
