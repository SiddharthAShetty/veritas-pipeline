import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))

from standardisation import (  # noqa: E402
    TestNameMatcher, UnitConverter, MedicineMatcher,
    parse_numeric_result, split_composite_result,
    normalize_age, normalize_gender, normalize_date,
)
from validation import ValidationEngine  # noqa: E402

CONFIG = Path(__file__).resolve().parent.parent / "config"


@pytest.fixture(scope="module")
def test_matcher():
    return TestNameMatcher(str(CONFIG / "test_dictionary.json"))


@pytest.fixture(scope="module")
def unit_converter():
    return UnitConverter(str(CONFIG / "unit_conversions.json"))


@pytest.fixture(scope="module")
def medicine_matcher():
    return MedicineMatcher(str(CONFIG / "medicine_mapping.json"))


@pytest.fixture(scope="module")
def validator():
    return ValidationEngine(str(CONFIG / "reference_ranges.json"))


# ---- 2.1 test name normalisation ----------------------------------
def test_exact_alias_match(test_matcher):
    canonical, method, conf = test_matcher.match("HAEMOGLOBIN")
    assert canonical == "Hemoglobin"
    assert method == "exact_alias_match"
    assert conf == 1.0


def test_alias_with_method_note_matches(test_matcher):
    canonical, method, conf = test_matcher.match("Haemoglobin (whole blood/photometric method)")
    assert canonical == "Hemoglobin"


def test_fuzzy_match_typo(test_matcher):
    canonical, method, conf = test_matcher.match("HAEMAGLOBIN")  # deliberate typo
    assert canonical == "Hemoglobin"
    assert method == "fuzzy_match"


def test_unresolved_name_is_logged_not_dropped(test_matcher):
    before = len(test_matcher.unresolved_log)
    canonical, method, conf = test_matcher.match("Some Totally Unknown Analyte XYZ123")
    assert canonical is None
    assert method == "unresolved"
    assert len(test_matcher.unresolved_log) == before + 1


def test_suffix_match_leading_truncation(test_matcher):
    """Real Apollo/FASTTRACK OCR output systematically drops leading
    characters, e.g. 'sophils' for 'Basophils'. difflib's fuzzy tier can't
    rescue a 2-character drop on a short word (ratio falls below cutoff),
    so this must be caught by the dedicated suffix-match tier."""
    canonical, method, conf = test_matcher.match("sophils")
    assert canonical == "Basophils"
    assert method == "suffix_match_truncated_name"


def test_leading_truncation_resolves_via_fuzzy_or_suffix_tier(test_matcher):
    """Other truncated variants in the real data are close enough that the
    general fuzzy tier resolves them directly -- either tier is acceptable,
    what matters is the name resolves correctly rather than staying
    unresolved."""
    for raw, expected in [("tal WBC Count", "Total Leucocyte Count"),
                          ("ematocrit HCT", "Hematocrit"),
                          ("eutrophils", "Neutrophils"),
                          ("mphocytes", "Lymphocytes")]:
        canonical, method, conf = test_matcher.match(raw)
        assert canonical == expected
        assert method in ("fuzzy_match", "suffix_match_truncated_name")


def test_prefix_match_trailing_truncation(test_matcher):
    """Mirror case: name truncated at the end, e.g. OCR line-wrapping
    clipping a long test name."""
    canonical, method, conf = test_matcher.match("eGFR - ESTIMATED GLOMERULAR")
    assert canonical == "Estimated Glomerular Filtration Rate"
    assert method == "prefix_match_truncated_name"


def test_short_common_word_does_not_spuriously_suffix_match(test_matcher):
    """Guards against the suffix-match tier being too permissive -- a
    generic short word shouldn't match just because it happens to be a
    trailing fragment of some unrelated alias."""
    canonical, method, conf = test_matcher.match("count")
    assert canonical is None


# ---- 2.3 numeric conversion -----------------------------------------
def test_parse_plain_number():
    r = parse_numeric_result("10.7")
    assert r["is_numeric"] is True
    assert r["value"] == 10.7


def test_parse_number_with_trailing_unit():
    r = parse_numeric_result("8200 cells/cumm")
    assert r["is_numeric"] is True
    assert r["value"] == 8200.0


def test_parse_flagged_value():
    r = parse_numeric_result("H 0.7")
    assert r["flag"] == "H"
    assert r["value"] == 0.7


def test_parse_non_numeric_text():
    r = parse_numeric_result("NEGATIVE")
    assert r["is_numeric"] is False


def test_parse_range_shaped_result_is_contradictory():
    r = parse_numeric_result("12-20")
    assert r["contradictory"] is True
    assert r["is_numeric"] is False


def test_split_composite_result():
    parts = split_composite_result("Neutrophil - 72.4, Lymphocyte - 23.5, Eosinophils - 3.0")
    assert parts is not None
    names = [p[0] for p in parts]
    assert "Neutrophil" in names
    assert len(parts) == 3


def test_non_composite_result_not_split():
    assert split_composite_result("10.7") is None


# ---- 2.4 unit harmonisation ------------------------------------------
def test_unit_conversion_passthrough_for_known_alias(unit_converter):
    value, unit = unit_converter.convert(10.7, "g/dl", "g/dL")
    assert unit == "g/dL"
    assert value == 10.7


def test_unit_conversion_unknown_unit_falls_back_to_expected(unit_converter):
    value, unit = unit_converter.convert(5.0, "some_weird_unit", "mg/dL")
    assert unit == "mg/dL"
    assert value == 5.0


# ---- 2.5 demographic normalisation ------------------------------------
def test_normalize_age_plain_years():
    r = normalize_age("41")
    assert r["age_years"] == 41.0


def test_normalize_age_composite_string():
    r = normalize_age("33Y11M265D")
    assert r["age_years"] == 33
    assert r["age_months"] == 11
    assert r["age_days"] == 265


def test_normalize_age_redacted():
    r = normalize_age("[AGE REDACTED]")
    assert r["age_years"] is None


def test_normalize_gender_variants():
    assert normalize_gender("M") == "Male"
    assert normalize_gender("female") == "Female"
    assert normalize_gender("[GENDER REDACTED]") is None
    assert normalize_gender("") is None


def test_normalize_date_dd_mm_yyyy():
    assert normalize_date("15/06/2026", "%d/%m/%Y") == "2026-06-15"


def test_normalize_date_falls_back_across_formats():
    # declared format is %d/%m/%Y but value is actually ISO -- should still parse
    assert normalize_date("2026-06-15", "%d/%m/%Y") == "2026-06-15"


def test_normalize_date_unparseable_returns_none():
    assert normalize_date("not-a-date", "%d/%m/%Y") is None


# ---- 2.6 medicine mapping --------------------------------------------
def test_medicine_brand_resolves_to_generic(medicine_matcher):
    r = medicine_matcher.match("Dolo 650mg")
    assert r["generic_name"] == "Paracetamol"
    assert r["medicine_type"] == "brand_resolved"


def test_medicine_unmapped_brand_passes_through(medicine_matcher):
    r = medicine_matcher.match("Some Unknown Drug XYZ")
    assert r["medicine_type"] == "unmapped_brand_or_generic"
    assert r["generic_name"] == "Some Unknown Drug XYZ"


# ---- 3.x validation -----------------------------------------------------
def test_validation_within_range(validator):
    result = validator.classify("Hemoglobin", "numeric", {"is_numeric": True, "value": 14.0, "contradictory": False})
    assert result["test_analytics"] == "Within Range"


def test_validation_outlier(validator):
    result = validator.classify("Hemoglobin", "numeric", {"is_numeric": True, "value": 8200.0, "contradictory": False})
    assert result["test_analytics"] == "Outlier"


def test_validation_below_range(validator):
    result = validator.classify("Hemoglobin", "numeric", {"is_numeric": True, "value": 10.7, "contradictory": False})
    assert result["test_analytics"] == "Below Range"


def test_validation_invalid_non_numeric(validator):
    result = validator.classify("Serum Creatinine", "numeric",
                                 {"is_numeric": False, "value": None, "contradictory": False, "raw": "PENDING"})
    assert result["test_analytics"] == "Invalid"


def test_validation_contradictory_range_in_result(validator):
    result = validator.classify("Respiratory Rate", "numeric",
                                 {"is_numeric": False, "value": None, "contradictory": True, "raw": "12-20"})
    assert result["test_analytics"] == "Invalid"


def test_validation_text_type_not_applicable(validator):
    result = validator.classify("Urine Routine Microscopy", "text",
                                 {"is_numeric": False, "value": None, "contradictory": False, "raw": "Clear"})
    assert result["test_analytics"] == "Not Applicable"


def test_validation_unclassified_no_range_configured(validator):
    result = validator.classify("Some New Test Not In Ranges", "numeric",
                                 {"is_numeric": True, "value": 5.0, "contradictory": False})
    assert result["test_analytics"] == "Unclassified"
