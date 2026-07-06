"""
Pipeline orchestrator.

Takes a RawRecord (one clinic JSON file) + its clinic config, walks the
config-declared blocks/rows, and emits canonical row dicts ready for
db_loader. This is the piece that makes "adding a test or clinic variant
requires only a config change" (Deliverable 3.2 evaluation criterion) true
in practice: every field lookup here goes through the clinic config, not
a clinic-specific branch of code.
"""

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pathutil import get_path
from ingestion import RawRecord
from standardisation import (
    TestNameMatcher, UnitConverter, MedicineMatcher,
    parse_numeric_result, split_composite_result,
    normalize_age, normalize_gender, normalize_date,
)
from validation import ValidationEngine

logger = logging.getLogger("veritas.pipeline")

RANGE_TEXT_RE = re.compile(r"^\s*(-?\d+\.?\d*)\s*-\s*(-?\d+\.?\d*)\s*$")


def parse_range_text(range_text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not range_text:
        return None, None
    m = RANGE_TEXT_RE.match(range_text.strip())
    if not m:
        return None, None
    try:
        return float(m.group(1)), float(m.group(2))
    except ValueError:
        return None, None


class RowError:
    def __init__(self, file_path: str, clinic_id: str, stage: str, reason: str, detail: str = ""):
        self.file_path = file_path
        self.clinic_id = clinic_id
        self.stage = stage
        self.reason = reason
        self.detail = detail
        self.occurred_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return {"file_path": self.file_path, "clinic_id": self.clinic_id, "stage": self.stage,
                "reason": self.reason, "detail": self.detail, "occurred_at": self.occurred_at}


class StandardisationPipeline:
    def __init__(self, test_matcher: TestNameMatcher, unit_converter: UnitConverter,
                 medicine_matcher: MedicineMatcher, validator: ValidationEngine):
        self.test_matcher = test_matcher
        self.unit_converter = unit_converter
        self.medicine_matcher = medicine_matcher
        self.validator = validator
        self.row_errors: List[RowError] = []

    # -----------------------------------------------------------------
    def _get_blocks(self, record: RawRecord) -> List[Tuple[str, Dict[str, Any]]]:
        cfg = record.clinic_config
        env = cfg.get("envelope", {})
        blocks_path = env.get("blocks_path")
        if blocks_path:
            blocks = get_path(record.raw_json, blocks_path, []) or []
            out = []
            type_field = env.get("record_type_field")
            type_map = env.get("record_type_map", {})
            for block in blocks:
                raw_type = block.get(type_field) if type_field else None
                record_type = type_map.get(raw_type)
                if record_type is None:
                    self.row_errors.append(RowError(
                        record.file_path, record.clinic_id, "block_routing",
                        "unknown_classifier", detail=f"classifier='{raw_type}'"))
                    continue
                out.append((record_type, block))
            return out
        default_type = env.get("record_type_map", {}).get("default", "lab_report")
        return [(default_type, record.raw_json)]

    def _base_fields(self, record: RawRecord) -> Dict[str, Any]:
        meta = record.meta_details or {}
        return {
            "id": str(uuid.uuid4()),
            "document_id": record.document_id,
            "correlation_id": record.correlation_id,
            "trace_id": record.trace_id,
            "clinic_id": record.clinic_id,
            "source_system": record.clinic_config.get("source_system"),
            "source_system_reported": meta.get("source_system"),
            "claim_no": meta.get("claim_no"),
            "nt_code": meta.get("nt_code"),
            "consumer_client_id": meta.get("ConsumerClientId"),
            "destination_identifier": meta.get("DestinationIdentifier"),
            "file_gcs_path": record.file_path,
            "ingested_at": record.ingested_at,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "metadetails": json.dumps(meta) if meta else None,
        }

    # -----------------------------------------------------------------
    def _process_lab_report(self, record: RawRecord, block_root: Dict[str, Any]) -> List[Dict[str, Any]]:
        cfg = record.clinic_config
        spec = cfg.get("lab_report")
        if not spec:
            self.row_errors.append(RowError(record.file_path, record.clinic_id,
                                              "lab_report", "no_lab_report_spec_for_clinic"))
            return []
        date_fmt = cfg.get("date_format")
        demo_path = spec.get("basic_info_path")
        demo_root = get_path(block_root, demo_path, block_root) if demo_path else block_root
        dmap = spec.get("demographics_field_map", {})

        age_norm = normalize_age(demo_root.get(dmap.get("age")) if isinstance(demo_root, dict) else None)
        gender_norm = normalize_gender(demo_root.get(dmap.get("gender")) if isinstance(demo_root, dict) else None)

        demographics = {
            "record_type": "lab_test",
            "patient_name": demo_root.get(dmap.get("patient_name")) if isinstance(demo_root, dict) else None,
            "uhid": demo_root.get(dmap.get("uhid")) if isinstance(demo_root, dict) else None,
            "gender": gender_norm,
            "hospital_name": demo_root.get(dmap.get("hospital_name")) if isinstance(demo_root, dict) else None,
            "bill_date": normalize_date(demo_root.get(dmap.get("bill_date")) if isinstance(demo_root, dict) else None, date_fmt),
            "reports_date": normalize_date(demo_root.get(dmap.get("reports_date")) if isinstance(demo_root, dict) else None, date_fmt),
            **{f"age_{k}" if k != "age_text" else k: v for k, v in age_norm.items()},
            "lab_or_hospital_name": demo_root.get("lab_or_hospital_name") if isinstance(demo_root, dict) else None,
            "report_date": demo_root.get(dmap.get("reports_date")) if isinstance(demo_root, dict) else None,
        }

        rows_path = spec.get("rows_path")
        rows = get_path(block_root, rows_path, []) or []
        rmap = spec.get("row_field_map", {})
        skip_where = spec.get("skip_rows_where", {})

        out_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if skip_where and all(row.get(k) == v for k, v in skip_where.items()):
                continue  # placeholder/header row, e.g. Apollo's literal "test_name"/"result"

            test_name_raw = row.get(rmap.get("test_name"))
            result_raw = row.get(rmap.get("result_text"))
            unit_raw = row.get(rmap.get("unit_original"))
            range_raw = row.get(rmap.get("range_text"))
            analytics_raw = row.get(rmap.get("test_analytics")) if rmap.get("test_analytics") else None
            page_no = row.get(rmap.get("page_number")) if rmap.get("page_number") else None

            if not test_name_raw or str(test_name_raw).strip() == "":
                continue

            expanded = split_composite_result(str(result_raw)) if result_raw else None
            sub_items = expanded if expanded else [(test_name_raw, result_raw)]

            for sub_name_raw, sub_result_raw in sub_items:
                test_row = self._build_test_row(
                    record,
                    demographics,
                    sub_name_raw,
                    sub_result_raw,
                    unit_raw,
                    range_raw,
                    page_no,
                    result_text_original=result_raw, 
                    range_text_original=range_raw
                )
                if test_row is not None:
                    out_rows.append(test_row)
                    
        self._flag_within_document_value_repeats(out_rows)
        # Demographics are denormalised onto every test row (flat canonical
        # schema) rather than emitted as a separate header row.
        return out_rows

    @staticmethod
    def _flag_within_document_value_repeats(rows: List[Dict[str, Any]]) -> None:
        """Some source reports print the same result more than once across
        different pages of the same document (e.g. a detail page and a
        later summary/consolidated page repeating the same value) --
        confirmed in real data by diffing an exported clinical_records.csv:
        the same (test_name_canonical, result_text) pair recurring under
        different page_number values within one document, sometimes with a
        different original label each time. The pipeline's existing
        deterministic-id dedup does NOT catch this, because page_number is
        part of that id -- each page's occurrence is a legitimately
        different row *by that key*, even though it's a redundant
        restatement of the same measurement, not a second, distinct one.
        Rather than silently drop the repeats (a genuinely repeated
        measurement at a different time within one report is possible, just
        rare), every row after the first is flagged for review, mirroring
        the "log, don't silently drop" approach used everywhere else in
        this pipeline."""
        seen: Dict[Any, int] = {}
        for row in rows:
            key = (row.get("test_name_canonical"), row.get("result_text"))
            if key in seen:
                note = (f"same value already recorded on page {seen[key]} of this "
                        f"document -- likely a restated result, not a new measurement")
                row["duplicate_within_report"] = True
                row["flag_reason"] = (row.get("flag_reason") + "; " + note) if row.get("flag_reason") else note
            else:
                row["duplicate_within_report"] = False
                seen[key] = row.get("page_number")

    def _build_test_row(self, record: RawRecord, demographics: Dict[str, Any],
                     test_name_raw: str, result_raw: Any, unit_raw: Any,
                     range_raw: Any, page_no: Any, result_text_original=None, range_text_original=None) -> Optional[Dict[str, Any]]:
        canonical, method, confidence = self.test_matcher.match(str(test_name_raw))
        value_type = self.test_matcher.value_type_for(canonical) if canonical else "text"
        numeric_result = parse_numeric_result(result_raw)

        unit_expected = self.test_matcher.unit_for(canonical) if canonical else None
        value_canonical, unit_canonical = self.unit_converter.convert(
            numeric_result.get("value"), unit_raw, unit_expected)

        range_low, range_high = parse_range_text(range_raw if isinstance(range_raw, str) else None)
        range_source = "source_reported" if range_low is not None or range_high is not None else None
        if range_low is None and range_high is None and canonical:
            ref = self.validator.ranges.get(canonical)
            if ref:
                range_low, range_high = ref["low"], ref["high"]
                range_source = "veritas_reference_config"

        # CHANGE 1: pass this row's own resolved unit into classify() so it can
        # be checked against test_dictionary.json's unit_canonical for this test
        classification = self.validator.classify(
            canonical, value_type, numeric_result,
            reported_unit_canonical=unit_canonical,
        )

        if classification.get("exclude_from_db"):
            self.row_errors.append(RowError(
                record.file_path, record.clinic_id, "unit_validation",
                "unit_mismatch_excluded", detail=classification["flag_reason"]))
            return None

        row = dict(demographics)
        row.update(self._base_fields(record))
        row.update({
            "record_type": "lab_test",
            "test_name_canonical": canonical or "UNRESOLVED",
            "test_name_original": test_name_raw,
            "result_value": value_canonical if numeric_result.get("is_numeric") else None,
            "result_text": str(result_raw) if result_raw is not None else None,
            "unit_canonical": unit_canonical,
            "unit_original": unit_raw,
            "range_low": range_low,
            "range_high": range_high,
            "range_text": range_raw,
            "range_source": range_source,
            "test_analytics": classification["test_analytics"],
            "flag_reason": classification["flag_reason"],
            "normalization_method": method,
            "normalization_confidence": confidence,
            "page_number": page_no,
            "result_text_original": str(result_text_original) if result_text_original is not None else None,
            "range_text_original": range_text_original,
        })
        if canonical is None:
            self.row_errors.append(RowError(
                record.file_path, record.clinic_id, "test_name_normalisation",
                "unresolved_test_name", detail=f"'{test_name_raw}' -- add to config/test_dictionary.json"))
        return row
    # -----------------------------------------------------------------
    def _process_discharge_summary(self, record: RawRecord, block_root: Dict[str, Any]) -> List[Dict[str, Any]]:
        cfg = record.clinic_config
        spec = cfg.get("discharge_summary")
        if not spec:
            self.row_errors.append(RowError(record.file_path, record.clinic_id,
                                              "discharge_summary", "no_discharge_summary_spec_for_clinic"))
            return []
        date_fmt = cfg.get("date_format")
        root_path = spec.get("root_path")
        root = get_path(block_root, root_path, block_root) if root_path else block_root
        fmap = spec.get("field_map", {})

        age_norm = normalize_age(root.get(fmap.get("age")))
        gender_norm = normalize_gender(root.get(fmap.get("gender")))

        row = self._base_fields(record)
        row.update({
            "record_type": "discharge_summary",
            "patient_name": root.get(fmap.get("patient_name")),
            "gender": gender_norm,
            "hospital_name": root.get(fmap.get("hospital_name")),
            "hospital_address": root.get(fmap.get("hospital_address")),
            "doctor_name": root.get(fmap.get("doctor_name")),
            "ward": root.get(fmap.get("ward")),
            "admission_date": normalize_date(root.get(fmap.get("admission_date")), date_fmt),
            "discharge_date": normalize_date(root.get(fmap.get("discharge_date")), date_fmt),
            "diagnosis": root.get(fmap.get("diagnosis")),
            "brief_history": root.get(fmap.get("brief_history")),
            "general_examinations": root.get(fmap.get("general_examinations")),
            "course_during_hospitalisation": root.get(fmap.get("course_during_hospitalisation")),
            "recommendations": root.get(fmap.get("recommendations")),
            "post_discharge_advice": root.get(fmap.get("post_discharge_advice")),
            "medicine_injections_investigation": root.get(fmap.get("medicine_injections_investigation")),
            "other_med_inj_investigations": root.get(fmap.get("medicine_injections_investigation")),
            **{f"age_{k}" if k != "age_text" else k: v for k, v in age_norm.items()},
        })
        out = [row]

        meds_path = spec.get("medications_path")
        meds = get_path(block_root, meds_path, []) if meds_path else []
        mmap = spec.get("medication_field_map", {})
        for med in (meds or []):
            if not isinstance(med, dict):
                continue
            raw_name = med.get(mmap.get("medicine"))
            raw_dose = med.get(mmap.get("dose"))
            raw_frequency = med.get(mmap.get("frequency"))
            match = self.medicine_matcher.match(raw_name)
            med_row = self._base_fields(record)
            med_row.update({
                "record_type": "medication",
                "patient_name": root.get(fmap.get("patient_name")),
                "medicine": match["medicine_name"], "generic_name": match["generic_name"],
                "drug_class": match["drug_class"], "medicine_type": match["medicine_type"],
                "dose": raw_dose, "frequency": raw_frequency,
                "medication_name": match["medicine_name"], "medication_medicine": raw_name,
                "medication_dose": raw_dose, "medication_frequency": raw_frequency,
                "discharge_medications_medicine": raw_name, "discharge_medications_dose": raw_dose,
                "discharge_medications_frequency": raw_frequency,
            })
            out.append(med_row)
        return out

    # -----------------------------------------------------------------
    def process_record(self, record: RawRecord) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for record_type, block_root in self._get_blocks(record):
            if record_type == "lab_report":
                rows.extend(self._process_lab_report(record, block_root))
            elif record_type == "discharge_summary":
                rows.extend(self._process_discharge_summary(record, block_root))
            else:
                self.row_errors.append(RowError(record.file_path, record.clinic_id,
                                                  "block_routing", f"unhandled_record_type_{record_type}"))
        logger.info(
            "Standardised document_id=%s correlation_id=%s clinic_id=%s -> %d canonical rows",
            record.document_id, record.correlation_id, record.clinic_id, len(rows))
        return rows
