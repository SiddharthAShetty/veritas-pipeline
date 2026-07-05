"""
Validation & Analytics Flags module.

Implements FR-3.1 through FR-3.4:
  3.1 Range validation        -- against config/reference_ranges.json
  3.2 Outlier detection       -- separate, wider bound than range, see
                                  reference_ranges.json._meta.outlier_rule
  3.3 Analytics classification -- Within Range / Above Range / Below Range /
                                  Outlier / Invalid / Unclassified
  3.4 Incorrect value flagging -- non-numeric-where-numeric-expected,
                                  contradictory (combined-field / range-in-
                                  result), and unit-mismatch-vs-dictionary
                                  cases, surfaced via the same test_analytics
                                  field so the UI's flagged queue (FR-5.3)
                                  has one place to look. Unit-mismatch rows
                                  are additionally marked exclude_from_db so
                                  the pipeline routes them to the dead
                                  letter store (FR-4.2) instead of loading
                                  a clinically implausible value.
"""
import json
from typing import Any, Dict, Optional

ANALYTICS_WITHIN = "Within Range"
ANALYTICS_ABOVE = "Above Range"
ANALYTICS_BELOW = "Below Range"
ANALYTICS_OUTLIER = "Outlier"
ANALYTICS_INVALID = "Invalid"
ANALYTICS_UNCLASSIFIED = "Unclassified"
ANALYTICS_NOT_APPLICABLE = "Not Applicable"


class ValidationEngine:
    def __init__(self, ranges_config_path: str, test_dict_path: str):
        with open(ranges_config_path, "r", encoding="utf-8") as f:
            spec = json.load(f)
        self.ranges: Dict[str, Dict[str, float]] = spec.get("ranges", {})

        with open(test_dict_path, "r", encoding="utf-8") as f:
            test_dict = json.load(f)
        # canonical_name -> the unit that name is *defined* to be reported in
        self.expected_unit: Dict[str, str] = {
            t["canonical_name"]: t.get("unit_canonical")
            for t in test_dict.get("tests", [])
            if t.get("unit_canonical")
        }

    def classify(self, canonical_test_name: Optional[str], value_type: str,
                 numeric_result: Dict[str, Any],
                 reported_unit_canonical: Optional[str] = None) -> Dict[str, Any]:
        """numeric_result is the dict produced by standardisation.parse_numeric_result.
        reported_unit_canonical is the unit this specific row was standardised to
        (from convert_unit()/standardisation), checked against the dictionary's
        expected unit for this canonical test.
        Returns {"test_analytics": ..., "flag_reason": ..., "exclude_from_db": bool}."""
        is_numeric = numeric_result.get("is_numeric", False)
        contradictory = numeric_result.get("contradictory", False)
        value = numeric_result.get("value")

        if value_type == "text":
            return {"test_analytics": ANALYTICS_NOT_APPLICABLE, "flag_reason": None,
                    "exclude_from_db": False}

        # --- unit-vs-dictionary plausibility check (runs before range check,
        # since comparing a value against a range meant for a different unit
        # is meaningless -- e.g. 0.9 "mg/dL" against an ALT range defined in U/L) ---
        expected_unit = self.expected_unit.get(canonical_test_name) if canonical_test_name else None
        if expected_unit and reported_unit_canonical and reported_unit_canonical != expected_unit:
            return {
                "test_analytics": ANALYTICS_INVALID,
                "flag_reason": (
                    f"unit mismatch: '{canonical_test_name}' expects '{expected_unit}' "
                    f"per test_dictionary.json, but this row reported '{reported_unit_canonical}' "
                    f"-- likely a source-side extraction/OCR misattribution, not standardisable"
                ),
                "exclude_from_db": True,
            }

        if contradictory:
            return {"test_analytics": ANALYTICS_INVALID,
                    "flag_reason": "result field contains a range/contradictory value instead of a single measurement",
                    "exclude_from_db": False}

        if not is_numeric:
            raw = numeric_result.get("raw")
            if raw is None or str(raw).strip() == "":
                return {"test_analytics": ANALYTICS_UNCLASSIFIED, "flag_reason": "missing result",
                        "exclude_from_db": False}
            return {"test_analytics": ANALYTICS_INVALID,
                    "flag_reason": f"non-numeric result '{raw}' where a numeric test result was expected",
                    "exclude_from_db": False}

        if not canonical_test_name or canonical_test_name not in self.ranges:
            return {"test_analytics": ANALYTICS_UNCLASSIFIED,
                    "flag_reason": "no reference range configured for this test yet",
                    "exclude_from_db": False}

        r = self.ranges[canonical_test_name]
        if value < r["outlier_low"] or value > r["outlier_high"]:
            return {"test_analytics": ANALYTICS_OUTLIER,
                    "flag_reason": f"value {value} is physiologically implausible (outside [{r['outlier_low']}, {r['outlier_high']}])",
                    "exclude_from_db": False}
        if value < r["low"]:
            return {"test_analytics": ANALYTICS_BELOW, "flag_reason": None, "exclude_from_db": False}
        if value > r["high"]:
            return {"test_analytics": ANALYTICS_ABOVE, "flag_reason": None, "exclude_from_db": False}
        return {"test_analytics": ANALYTICS_WITHIN, "flag_reason": None, "exclude_from_db": False}