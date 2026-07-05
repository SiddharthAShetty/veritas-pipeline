import sys
import json
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))

from ingestion import ClinicConfigRegistry, Ingestion  # noqa: E402
from standardisation import TestNameMatcher, UnitConverter, MedicineMatcher  # noqa: E402
from validation import ValidationEngine  # noqa: E402
from pipeline import StandardisationPipeline  # noqa: E402
from db_loader import deterministic_row_id  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "config"
SAMPLE_DATA = ROOT / "sample-data"


@pytest.fixture(scope="module")
def pipeline_result():
    registry = ClinicConfigRegistry(str(CONFIG / "clinics"))
    ingestion = Ingestion(str(SAMPLE_DATA), registry, dedup_enabled=True)
    records = ingestion.run()

    std = StandardisationPipeline(
        TestNameMatcher(str(CONFIG / "test_dictionary.json")),
        UnitConverter(str(CONFIG / "unit_conversions.json")),
        MedicineMatcher(str(CONFIG / "medicine_mapping.json")),
        ValidationEngine(str(CONFIG / "reference_ranges.json")),
    )
    all_rows = []
    for r in records:
        all_rows.extend(std.process_record(r))
    return {"ingestion": ingestion, "std": std, "records": records, "rows": all_rows}


# ---- FR-1.1 multi-source ingestion ------------------------------------
def test_discovers_all_five_clinics(pipeline_result):
    clinic_ids = {r.clinic_id for r in pipeline_result["records"]}
    assert clinic_ids == {"apollo_diagnostics", "medplus_labs", "citycare_hospital", "wellness_diagnostics"}
    # sunrise_clinic's only file is malformed -> never produces a record


# ---- FR-1.2 duplicate detection -----------------------------------------
def test_duplicate_citycare_submission_suppressed(pipeline_result):
    dup_files = [d for d in pipeline_result["ingestion"].duplicates_suppressed
                 if d["clinic_id"] == "citycare_hospital"]
    assert len(dup_files) == 1


def test_dedup_is_configurable():
    registry = ClinicConfigRegistry(str(CONFIG / "clinics"))
    ingestion_off = Ingestion(str(SAMPLE_DATA), registry, dedup_enabled=False)
    records = ingestion_off.run()
    assert len(ingestion_off.duplicates_suppressed) == 0
    citycare_files = [r for r in records if r.clinic_id == "citycare_hospital"]
    assert len(citycare_files) == 2  # both the original and the resubmit are kept


# ---- FR-1.3 schema flexibility across differently-shaped clinics --------
def test_medplus_flat_schema_parsed(pipeline_result):
    rows = [r for r in pipeline_result["rows"] if r["clinic_id"] == "medplus_labs"]
    assert any(r["test_name_canonical"] == "Hemoglobin" for r in rows)


def test_citycare_alternate_keys_parsed(pipeline_result):
    rows = [r for r in pipeline_result["rows"] if r["clinic_id"] == "citycare_hospital"]
    assert any(r["test_name_canonical"] == "Alanine Aminotransferase" for r in rows)


# ---- error handling / dead-letter (FR-4.2) -------------------------------
def test_malformed_json_goes_to_dead_letter(pipeline_result):
    reasons = [e.reason for e in pipeline_result["ingestion"].errors]
    assert "malformed_json" in reasons


def test_unknown_clinic_flagged_not_crashed(tmp_path):
    (tmp_path / "ghost_clinic" / "2026-01-01").mkdir(parents=True)
    (tmp_path / "ghost_clinic" / "2026-01-01" / "f.json").write_text('{"a": 1}')
    registry = ClinicConfigRegistry(str(CONFIG / "clinics"))
    ingestion = Ingestion(str(tmp_path), registry)
    records = ingestion.run()
    assert records == []
    assert any(e.reason == "unknown_clinic" for e in ingestion.errors)


# ---- validation flags surface real data-quality issues in the sample ----
def test_mislabeled_haemoglobin_row_flagged_outlier(pipeline_result):
    """The real Apollo sample has a row literally named
    'Haemoglobin (whole blood/photometric method)' carrying a TLC-shaped
    value (8200 cells/cumm) -- a genuine upstream extraction defect. The
    pipeline should not silently accept it; it must classify as Outlier."""
    rows = [r for r in pipeline_result["rows"]
            if r["clinic_id"] == "apollo_diagnostics"
            and r.get("test_name_original") == "Haemoglobin (whole blood/photometric method)"]
    assert rows
    assert rows[0]["test_analytics"] == "Outlier"


def test_citycare_implausible_haemoglobin_999_flagged(pipeline_result):
    rows = [r for r in pipeline_result["rows"]
            if r["clinic_id"] == "citycare_hospital" and r["test_name_original"] == "Haemoglobin"]
    assert rows
    assert rows[0]["test_analytics"] == "Outlier"


def test_citycare_pending_ast_flagged_invalid(pipeline_result):
    rows = [r for r in pipeline_result["rows"]
            if r["clinic_id"] == "citycare_hospital" and r["test_name_original"] == "AST (SGOT)"]
    assert rows
    assert rows[0]["test_analytics"] == "Invalid"


def test_wellness_composite_differential_split_into_subtests(pipeline_result):
    rows = [r for r in pipeline_result["rows"]
            if r["clinic_id"] == "wellness_diagnostics" and r["test_name_canonical"] == "Neutrophils"]
    assert rows
    assert rows[0]["result_value"] == 55.0


# ---- NFR-3.2 idempotency --------------------------------------------------
def test_deterministic_id_stable_across_runs(pipeline_result):
    row = pipeline_result["rows"][0]
    id1 = deterministic_row_id(row)
    id2 = deterministic_row_id(dict(row))
    assert id1 == id2


# ---- FR-4.2/NFR-4.2 metaDetails lineage (claim_no, source_system) --------
def test_claim_no_captured_from_meta_details(pipeline_result):
    apollo_rows = [r for r in pipeline_result["rows"] if r["clinic_id"] == "apollo_diagnostics"]
    assert apollo_rows
    assert all(r.get("claim_no") for r in apollo_rows)
    # source_system_reported legitimately varies *within* one clinic folder --
    # the real sample data shows both 'ARTEMIS' and 'FASTTRACK' backends
    # feeding the same envelope shape into the same staging pipeline, which
    # is exactly why clinic identity comes from the folder path (FR-1.3),
    # not from any single content field.
    reported_systems = {r.get("source_system_reported") for r in apollo_rows}
    assert reported_systems == {"ARTEMIS", "FASTTRACK"}


# ---- FR-1.2 content-based dedup across different document/correlation ids -
def test_content_identical_resubmit_under_new_ids_is_deduped(pipeline_result):
    """Two real sample files (Sample_JSON_file1 / file3) carry byte-identical
    clinical content but different document_id/correlation_id/claim_no --
    a real duplicate an id-only dedup key would miss. The content-hash
    dedup signal must catch it."""
    dup_reasons = [d["reason"] for d in pipeline_result["ingestion"].duplicates_suppressed
                   if d["clinic_id"] == "apollo_diagnostics"]
    assert dup_reasons  # at least one apollo duplicate suppressed by content hash
