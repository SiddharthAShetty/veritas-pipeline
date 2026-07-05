# Assumptions Document — Veritas Medical Data Standardisation Pipeline

## Business Assumptions

- **Consumers of this data are claims analysts and adjudication rules, not clinicians.**
  The canonical output optimises for "can I compare Hemoglobin across clinics and flag
  outliers programmatically," not for reproducing a clinically complete medical record.
  A production clinical-decision-support use case would need a stricter, licensed
  reference-range source (see Data Assumptions).
- **"Duplicate report" means the same document submitted twice, not a genuine repeat test.**
  A patient legitimately getting the same test twice (e.g. a follow-up Hemoglobin a week
  later) is a *different* document with a different `document_id`/`correlation_id` and
  should **not** be deduplicated. The dedup logic keys on document identity, not on
  patient+test+value, specifically to avoid this false-positive.
- **Zero manual intervention (stated in 1.3) is a steady-state goal, not a day-one one.**
  New clinics will always produce some unresolved test names / unmapped units on first
  onboarding; the design assumes a curation workflow (dictionary edits) exists, and
  measures against it (NFR-4.1's 98%-in-30-days), rather than assuming perfect coverage
  from file one.
- **FR-2.2's "exactly 5 columns" is read as 5 fixed, reusable column names — one row per
  test — not a dedicated 5-column block repeated per test name.** The literal column names
  in the requirement (`Test_Name`, `Test_Name_Result`, `Test_Name_Range`, `Test_Name_Unit`,
  `Test_Name_Analytics`) are used exactly as given, as the actual column headers, reused for
  every test's row. This also matches the ideal-schema CSV's shape: generic, reusable
  columns (`test_name_canonical`, `result_value`, `range_low`, `unit_canonical`,
  `test_analytics` — all literally present in that CSV, verbatim), one row per
  (document, test) pair, no test name ever baked into a column name.The
  `vw_test_result` SQL view in `src/db_loader.py` is the single answer: one row per
  test, the 5 fixed columns named exactly as the requirement states, blank where a value is
  missing for that test.
- **500+ clinics does not mean 500+ genuinely distinct schemas.** In practice, many
  clinics likely share an upstream OCR/extraction vendor (the sample JSON's
  `traceId`/`documentId`/`classifier` envelope looks like a document-AI output format,
  not something a clinic's own EMR would hand-roll). The config design assumes clusters
  of clinics share an envelope shape, which is why clinic identity comes from the GCS
  folder path rather than content sniffing (see Architecture §2). **Confirmed by the full
  5-file real sample set**: all 5 files share the identical envelope shape, but
  `metaDetails.source_system` reports two different backend values across them (`ARTEMIS`
  and `FASTTRACK`) — the same platform clearly serves multiple upstream systems into one
  staging pipeline (`metaDetails.DestinationIdentifier = "stg-datalake"` in every file).
  That's the concrete evidence behind the folder-path-over-shape-sniffing decision.

## Technical Assumptions

- **SQLite instead of BigQuery/Postgres for this prototype.** Same canonical column set,
  same upsert-on-deterministic-id idempotency pattern, zero external dependencies to run
  the take-home. `db_loader.py` isolates all DB-specific SQL in one module — swapping the
  backend means replacing `SQLiteLoader`, not touching the standardisation/validation
  modules.
- **Flask over a JS framework for the UI.** Server-rendered HTML with no build step, given
  FR-5's evaluation criterion is "functional... not production-grade." A production version
  behind IAP would likely still be server-rendered (analysts, not consumers) — the
  framework choice would change less than the auth/deployment story would.
- **`difflib` over a fuzzy-matching library (e.g. `rapidfuzz`) for test-name matching.**
  Zero extra dependency, adequate accuracy for typo-level variance (tested against
  "HAEMAGLOBIN" → Hemoglobin). Production at 200k files/day would likely want a faster,
  more tunable library and possibly a learned embedding-based matcher for genuinely novel
  clinic phrasing that isn't a simple edit-distance typo.
- **Composite-field splitting uses a regex heuristic** (`Name - value, Name - value, ...`),
  not a general parser. It's good enough for the patterns seen in the sample data
  (differential counts, bilirubin panels) but would misfire on a sufficiently different
  delimiter convention from a new clinic — another config-driven extension point worth
  building if this pattern turns out to be common (e.g. a per-clinic `composite_delimiter`
  config field).
- **Age/date parsing uses a fixed list of format strings, not a general NLP date parser**
  (e.g. `dateutil`). Kept dependency-free and predictable; a library like `dateutil` would
  handle more formats automatically but can also silently misparse ambiguous dates
  (`01/02/2026` = Jan 2 or Feb 1?) — the explicit format list, seeded from the clinic
  config's declared `date_format`, is a deliberate trade of coverage for correctness.

## Data Assumptions

- **Reference ranges are simplified, general-adult, non-stratified.** Real ranges vary by
  age, sex, and sometimes pregnancy status/lab methodology; `config/reference_ranges.json`
  is a reasonable-adult-range approximation sufficient to demonstrate range/outlier
  classification logic, explicitly **not** clinically validated. Production would need a
  licensed reference source (e.g. institution-specific ranges tied to the reporting lab,
  since two labs' "normal" for the same analyte can differ by methodology).
- **The sample JSON's PII is already redacted** (`[PATIENT NAME REDACTED]` etc.), so the
  pipeline's PII-handling logic (NFR-4.3) is designed *against* the shape of unredacted
  fields but couldn't be tested against real values in this exercise. Production would add
  a masking/tokenisation step before any field reaches long-term storage or the UI — this
  prototype's UI shows patient name/UHID directly, which is acceptable only because the
  sample data is pre-redacted.
- **The sample Apollo file itself contains a genuine data-quality defect** — a row
  literally named `"Haemoglobin (whole blood/photometric method)"` carries a value
  (`8200 cells/cumm`) that's clearly a mislabeled Total Leucocyte Count from an adjacent
  row, not an actual hemoglobin reading. Rather than trying to "fix" this (which would
  require guessing at upstream extraction logic Veritas doesn't own), the pipeline
  classifies it as an **Outlier** and surfaces it in the flagged queue — the correct
  behaviour is to make the defect visible, not silently correct or silently accept it.
  (`tests/test_pipeline_integration.py::test_mislabeled_haemoglobin_row_flagged_outlier`
  encodes this as a regression test.)
- **The same result is frequently restated across multiple pages of one report — confirmed
  by diffing an actual exported `clinical_records.csv`, not just theorised.** Real Apollo
  documents repeat whole panels of results across several `page_no` values (a detail page,
  then a later summary/consolidated page repeating the same figures) — one document in the
  real sample set has the identical Alkaline Phosphatase value appear on 4 different pages.
  The pipeline's deterministic-id dedup does *not* catch this, because `page_number` is
  part of that id — each page's occurrence is legitimately a different row *by that key*,
  even though it's a redundant restatement, not a new measurement. Rather than silently
  load every repeat as if it were a distinct result (inflating counts an analyst might
  query), or silently collapsing them (risking loss of a genuinely repeated measurement,
  which is rare but possible), every occurrence after the first is flagged —
  `duplicate_within_report = true` on the row, with a `flag_reason` naming the page it was
  first seen on — and still loaded, visible in the UI's Flagged Queue under "Within-report
  duplicate." Proven against real data by
  `tests/test_pipeline_integration.py::test_within_document_repeated_value_flagged_not_dropped`.
  **Known remaining limitation**: this only catches *identical* repeats (same canonical
  test, same result text). If a composite field and a standalone row for the same test
  ever *disagree* in value across pages, neither is flagged as contradictory — the
  mismatch would only surface if a human happened to compare both rows. Worth hardening
  before production (e.g. flag same-test-different-value pairs within one document too,
  not just same-test-same-value).
- **Two of the five real sample files (1 and 3) are byte-identical in clinical content but
  wrapped in different `document_id`, `correlation_id`, and `claim_no` values.** This is a
  genuine duplicate an identity-key-only dedup strategy would miss entirely (FR-1.2 doesn't
  specify *how* to detect a duplicate, and the naive read — "match on document/correlation
  id" — isn't sufficient against real data). The pipeline's dedup now also hashes the
  clinical payload independently of envelope ids, specifically because this was observed,
  not because it was anticipated in advance.
- **A known gap in the same logic: a *partial* identity match isn't caught.** The id-based
  key requires every declared field (`document_id` AND `correlation_id`) to match — a file
  sharing only `correlation_id` with a prior file, but a new `document_id` and even slightly
  different content, is loaded as a new record, not suppressed. This is deliberate rather
  than an oversight: that pattern is genuinely ambiguous (it could be a true duplicate with
  an incidental field changed, or a legitimate correction/reissue of the same visit under a
  new document id), and auto-suppressing on a partial match risks silently discarding a real
  correction, which seemed worse than occasionally letting a near-duplicate through. A more
  complete design would route partial-key matches to a human review queue ("possible
  duplicate, not auto-suppressed") rather than a binary suppress/don't-suppress decision —
  not built here because it would need a UI review workflow this take-home doesn't have.
- **`metaDetails` (an array of `{key, value}` pairs carrying `claim_no`, `source_system`,
  `nt_code`, `ConsumerClientId`, `DestinationIdentifier`) is present on every real sample
  file.** An earlier version of this document claimed these fields weren't in the
  ideal-schema CSV — that was wrong, caught only after doing a careful column-by-column
  diff between `Ourput-table-ideal-schema.csv` and this pipeline's actual output columns
  (see `docs/COMPLIANCE_MATRIX.md`, FR-2.2). The CSV does explicitly list `nt_code`,
  `consumer_client_id`, and `destination_identifier` as expected columns — they just hadn't
  been promoted from `Ingestion._parse_meta_details()` into the saved row yet. Fixed: all
  five metaDetails fields (`claim_no`, `source_system_reported`, `nt_code`,
  `consumer_client_id`, `destination_identifier`) are now captured on every row.
  `claim_no` in particular is the foreign key back to the insurance claim record — for a
  claims company, dropping it would mean the standardised data can't be joined back to the
  claim it came from.
- **Unit conversion factors for ambiguous unit pairs (e.g. `mil/cu.mm` vs `mil/cu.cm`) are
  a documented best guess** (1000x, treating cu.cm as mL), not verified against a specific
  lab's SOP — flagged inline in `config/unit_conversions.json`. This is exactly the kind of
  thing that needs a domain-expert sign-off before trusting it in an adjudication pipeline.

## Scope Exclusions

Left out deliberately, given the 12–24 hour target — each with what it would take to add:

- **Real GCS/Pub-Sub/Cloud Run wiring.** The ingestion module reads a local folder using the
  same `{clinic_id}/{date}/{file}.json` convention GCS would use, so swapping the file-system
  walk for a `google-cloud-storage` client + Eventarc trigger is a contained change to
  `ingestion.py`'s `discover_files()`, not a redesign.
- **Schema versioning for a clinic that changes format over time (NFR-2.3).** The config
  loader currently keys purely on `clinic_id`; production would need
  `clinic_id + effective_date range → config version` resolution. Skipped because the
  sample data doesn't exercise it and it's a straightforward (if fiddly) extension of
  `ClinicConfigRegistry`.
- **PII masking/tokenisation implementation (NFR-4.3).** Discussed under Data Assumptions;
  not implemented because the sample data arrives pre-redacted, so there was nothing to mask
  against, and a real implementation deserves a real compliance conversation (what counts as
  PII under which regulation) rather than a guessed-at stub.
- **Monitoring/alerting integration (NFR-5.1)** beyond structured logging. The pipeline
  emits the metrics a monitoring system would consume (`pipeline_runs`,
  `clinic_quality_stats` tables; per-stage log lines with `trace_id`/`correlation_id`), but
  wiring those into actual Cloud Monitoring dashboards/alert policies is infrastructure
  config, not application code, and out of scope for a local prototype.
- **A learned/embedding-based test-name matcher.** `difflib`'s edit-distance approach
  handles the typo/spacing/method-note variance seen in the sample data; it would not
  handle a clinic that uses a genuinely different vocabulary (e.g. a local abbreviation
  with no string similarity to the canonical name). That needs either a much larger seed
  dictionary or a small classification model — reasonable phase-2 work once real
  unresolved-name volume from production clinics shows the pattern is common.
- **Load/throughput testing against the 200k/day, 15-minute p95 latency targets.** Addressed
  qualitatively in the architecture doc (Cloud Run autoscaling, per-file event model); not
  benchmarked here since the take-home dataset is 6 files, not 200,000.
