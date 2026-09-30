"""
Unit tests for Layer 4: AI Risk Narrative Integration.

All tests use mocked Gemini responses and mocked database interactions.
No live API calls or active database instances are required.
"""

import json
import logging
import os
from unittest.mock import MagicMock, patch
import pytest
from pydantic import ValidationError

from ai_risk_narrative import (
    NarrativeStatus,
    RiskNarrative,
    build_user_prompt,
    fetch_existing_narratives,
    fetch_vendors_for_narrative,
    generate_vendor_narrative,
    init_narrative_table,
    is_daily_quota_exhausted,
    is_narrative_valid_for_vendor,
    parse_risk_tiers,
    run_layer4,
    store_narrative,
)


# ── Sample Fixtures ──────────────────────────────────────────────────

@pytest.fixture
def sample_vendor():
    return {
        "vendor_id": 151,
        "vendor_name": "Khare and Sons",
        "category": "Automobiles",
        "state_code": "33",
        "anomaly_frequency": 83.29,
        "deviation_magnitude": 10.37,
        "validation_failures": 100.0,
        "recency_score": 16.13,
        "composite_score": 51.32,
        "risk_tier": "HIGH",
        "total_invoices": 230,
        "flagged_invoices": 191,
        "flag_rate": 83.04,
        "flag_breakdown": [
            {"flag_type": "GSTIN_MISMATCH", "severity": "HIGH", "flag_count": 85},
            {"flag_type": "STATISTICAL_ZSCORE", "severity": "HIGH", "flag_count": 45},
            {"flag_type": "DUPLICATE", "severity": "MEDIUM", "flag_count": 61},
        ],
    }


@pytest.fixture
def valid_narrative_json():
    return json.dumps({
        "risk_summary": "Vendor displays systemic high-risk anomalies dominated by multi-state GSTIN mismatches and severe price deviations.",
        "key_drivers": [
            "85 high-severity GSTIN state mismatches",
            "45 high-severity statistical deviations exceeding 3 standard deviations",
            "Overall 83.04% flag rate across 230 invoices"
        ],
        "investigation_priorities": [
            "Reconcile outward supply state codes against registered GSTIN address",
            "Audit line-item pricing for invoices exceeding 3-sigma baseline",
            "Inspect duplicate billing sequences for potential double-claiming"
        ],
        "evidence": [
            "Validation Failures Score: 100.0 / 100",
            "Composite Risk Score: 51.32 / 100",
            "Flagged Invoices: 191 of 230"
        ]
    })


# ── Test 1: Valid Gemini Structured Response ─────────────────────────

def test_valid_gemini_structured_response(sample_vendor, valid_narrative_json):
    """Test successful parsing and validation of a valid Gemini JSON response."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.text = valid_narrative_json
    mock_client.models.generate_content.return_value = mock_response

    narrative = generate_vendor_narrative(mock_client, sample_vendor, "gemini-2.5-flash")

    assert narrative is not None
    assert isinstance(narrative, RiskNarrative)
    assert "systemic high-risk anomalies" in narrative.risk_summary
    assert len(narrative.key_drivers) == 3
    assert len(narrative.investigation_priorities) == 3
    assert len(narrative.evidence) == 3
    assert "Validation Failures Score: 100.0 / 100" in narrative.evidence[0]


# ── Test 2: Missing API Key ──────────────────────────────────────────

def test_missing_api_key(sample_vendor, monkeypatch, caplog):
    """Test that missing GEMINI_API_KEY gracefully skips execution without error."""
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    with caplog.at_level(logging.WARNING):
        results = run_layer4(api_key="", risk_tiers="HIGH")

    assert results == []
    assert any("GEMINI_API_KEY is not set" in record.message for record in caplog.records)


# ── Test 3: Gemini API Failure / Rate Limit / Timeout ────────────────

def test_gemini_api_failure(sample_vendor, caplog):
    """Test that API exceptions (timeouts, rate limits, network errors) are caught safely."""
    mock_client = MagicMock()
    mock_client.models.generate_content.side_effect = RuntimeError("503 Service Unavailable / Rate Limit")

    with caplog.at_level(logging.ERROR):
        narrative = generate_vendor_narrative(mock_client, sample_vendor, "gemini-2.5-flash")

    assert narrative is None
    assert any("Gemini API invocation failed" in record.message for record in caplog.records)


# ── Test 4: Malformed Response ───────────────────────────────────────

def test_malformed_json_response(sample_vendor, caplog):
    """Test handling of invalid non-JSON output from the model."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.text = "Error: Internal server error {not json}"
    mock_client.models.generate_content.return_value = mock_response

    with caplog.at_level(logging.ERROR):
        narrative = generate_vendor_narrative(mock_client, sample_vendor, "gemini-2.5-flash")

    assert narrative is None
    assert any("Malformed JSON returned" in record.message or "Schema validation failed" in record.message for record in caplog.records)


# ── Test 5: Schema Validation Failure ────────────────────────────────

def test_schema_validation_failure(sample_vendor, caplog):
    """Test handling of JSON that does not match the Pydantic RiskNarrative schema."""
    mock_client = MagicMock()
    mock_response = MagicMock()
    # Missing 'key_drivers' and 'investigation_priorities'
    mock_response.text = json.dumps({
        "risk_summary": "Incomplete schema",
        "unexpected_field": 123
    })
    mock_client.models.generate_content.return_value = mock_response

    with caplog.at_level(logging.ERROR):
        narrative = generate_vendor_narrative(mock_client, sample_vendor, "gemini-2.5-flash")

    assert narrative is None
    assert any("Schema validation failed" in record.message for record in caplog.records)


# ── Test 6: HIGH-only Filtering ──────────────────────────────────────

def test_high_only_filtering():
    """Verify default and explicit HIGH tier filtering."""
    assert parse_risk_tiers("HIGH") == ["HIGH"]
    assert parse_risk_tiers("high") == ["HIGH"]
    assert parse_risk_tiers(None) == ["HIGH"]


def test_db_fetch_filters_high_only():
    """Verify that fetch_vendors_for_narrative passes only target tiers to SQL query."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchall.side_effect = [
        # First query for vendors
        [(101, "Vendor A", "Tech", "07", 50.0, 10.0, 20.0, 15.0, 45.0, "HIGH", 100, 40, 40.0)],
        # Second query for flags
        [("DUPLICATE", "MEDIUM", 40)],
    ]

    vendors = fetch_vendors_for_narrative(mock_conn, ["HIGH"])

    assert len(vendors) == 1
    assert vendors[0]["risk_tier"] == "HIGH"
    # Verify SQL query executed with (['HIGH'],)
    executed_args = mock_cur.execute.call_args_list[0][0][1]
    assert executed_args == (["HIGH"],)


# ── Test 7: HIGH + MEDIUM Filtering ──────────────────────────────────

def test_high_and_medium_filtering():
    """Verify parsing and query arguments for multi-tier selection."""
    assert parse_risk_tiers("HIGH,MEDIUM") == ["HIGH", "MEDIUM"]
    assert parse_risk_tiers("HIGH, MEDIUM") == ["HIGH", "MEDIUM"]
    assert parse_risk_tiers(" high, medium ") == ["HIGH", "MEDIUM"]

    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_cur.fetchall.side_effect = [
        # Two vendors: one HIGH, one MEDIUM
        [
            (101, "Vendor A", "Tech", "07", 50.0, 10.0, 20.0, 15.0, 45.0, "HIGH", 100, 40, 40.0),
            (102, "Vendor B", "Retail", "27", 30.0, 5.0, 10.0, 10.0, 28.0, "MEDIUM", 80, 20, 25.0),
        ],
        [],  # flags vendor A
        [],  # flags vendor B
    ]

    vendors = fetch_vendors_for_narrative(mock_conn, ["HIGH", "MEDIUM"])
    assert len(vendors) == 2
    assert {v["risk_tier"] for v in vendors} == {"HIGH", "MEDIUM"}
    executed_args = mock_cur.execute.call_args_list[0][0][1]
    assert executed_args == (["HIGH", "MEDIUM"],)


# ── Test 8: Deterministic Risk Score Remains Unchanged ───────────────

def test_deterministic_risk_score_remains_unchanged(sample_vendor, valid_narrative_json):
    """Verify that generating and storing narratives NEVER modifies vendor_risk_scores table."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    narrative = RiskNarrative.model_validate_json(valid_narrative_json)

    # Store narrative
    store_narrative(mock_conn, sample_vendor, narrative, "gemini-2.5-flash")

    # Check all executed queries: none should UPDATE or touch vendor_risk_scores
    for call in mock_cur.execute.call_args_list:
        query_sql = call[0][0].upper()
        assert "UPDATE VENDOR_RISK_SCORES" not in query_sql
        assert "DELETE FROM VENDOR_RISK_SCORES" not in query_sql
        assert "INTO VENDOR_RISK_SCORES" not in query_sql
        # Should only target ai_risk_narratives
        assert "AI_RISK_NARRATIVES" in query_sql

    # Verify original scores in sample_vendor are untouched
    assert sample_vendor["composite_score"] == 51.32
    assert sample_vendor["risk_tier"] == "HIGH"


# ── Test 9: AI Narrative Is Stored Separately ────────────────────────

def test_ai_narrative_stored_separately(sample_vendor, valid_narrative_json):
    """Verify that AI narratives are written to the dedicated ai_risk_narratives table."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    narrative = RiskNarrative.model_validate_json(valid_narrative_json)
    store_narrative(mock_conn, sample_vendor, narrative, "gemini-2.5-flash")

    # Find the INSERT call
    insert_calls = [
        c for c in mock_cur.execute.call_args_list
        if "INSERT INTO AI_RISK_NARRATIVES" in c[0][0].upper()
    ]
    assert len(insert_calls) == 1
    params = insert_calls[0][0][1]

    # params: (vendor_id, risk_tier, composite_score, risk_summary, key_drivers, investigation_priorities, evidence, model_name)
    assert params[0] == 151
    assert params[1] == "HIGH"
    assert params[2] == 51.32
    assert params[3] == narrative.risk_summary
    assert params[4] == narrative.key_drivers
    assert params[5] == narrative.investigation_priorities
    assert params[6] == narrative.evidence
    assert params[7] == "gemini-2.5-flash"


# ── Test 10: No Secret Is Exposed in Logs ────────────────────────────

def test_no_secret_exposed_in_logs(sample_vendor, caplog):
    """Verify that credentials or API keys are never exposed in log outputs."""
    secret_key = "SAMPLE_MOCK_KEY_SECRET_FOR_REDACTION"
    secret_db_pass = "SuperSecretPostgresPassword123"

    mock_client = MagicMock()
    # Simulate an error that contains raw error text
    mock_client.models.generate_content.side_effect = RuntimeError(
        f"Auth failed with key: {secret_key} and db pass {secret_db_pass}"
    )

    with caplog.at_level(logging.DEBUG):
        # We also simulate run_layer4 with missing key
        run_layer4(api_key="", risk_tiers="HIGH")
        generate_vendor_narrative(mock_client, sample_vendor, "gemini-2.5-flash")

    logged_text = " ".join(record.message for record in caplog.records)
    # The actual secret string should NOT appear in standard logs
    assert secret_key not in logged_text
    assert secret_db_pass not in logged_text


# ── Test 11: Daily Quota Detection ───────────────────────────────────

def test_daily_quota_detection():
    """Verify accurate detection of daily/free-tier quota exhaustion markers."""
    exhaustion_messages = [
        "429 RESOURCE_EXHAUSTED: Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 20",
        "GenerateRequestsPerDayPerProjectPerModel-FreeTier limit exceeded",
        "Requests per day quota exceeded for model gemini-3.8-flash",
        "Daily quota exceeded, please retry tomorrow",
        "error code 429: limit: 20 reached",
        "type.googleapis.com/google.rpc.QuotaFailure",
    ]
    for msg in exhaustion_messages:
        assert is_daily_quota_exhausted(msg) is True

    # Standard per-minute rate limit should NOT be marked as daily quota
    temporary_messages = [
        "429 Too Many Requests: Rate limit exceeded, retry in 2s",
        "Resource exhausted: 15 RPM limit reached, please slow down",
        "Rate limit exceeded",
    ]
    for msg in temporary_messages:
        assert is_daily_quota_exhausted(msg) is False


# ── Test 12: Daily Quota 429 Stops Vendor Retries Immediately ─────────

def test_daily_quota_429_stops_vendor_retries_immediately(sample_vendor):
    """When daily quota is exhausted, fail immediately on attempt 1 without 3-attempt retry loop."""
    mock_client = MagicMock()
    daily_quota_error = RuntimeError(
        "429 RESOURCE_EXHAUSTED: Quota exceeded for metric: "
        "generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 20"
    )
    mock_client.models.generate_content.side_effect = daily_quota_error

    err_out = {}
    narrative = generate_vendor_narrative(
        mock_client, sample_vendor, "gemini-3.8-flash", retries=3, error_out=err_out
    )

    assert narrative is None
    # Exactly ONE call was attempted (no retries!)
    assert mock_client.models.generate_content.call_count == 1
    assert err_out.get("status") == NarrativeStatus.QUOTA_EXHAUSTED
    assert err_out.get("error_type") == "429_QUOTA_EXHAUSTED"


# ── Test 13: Daily Quota 429 Stops Entire Batch (No Subsequent Vendors) ─

def test_daily_quota_429_no_additional_vendor_requests(sample_vendor):
    """When a daily quota error is returned, run_layer4 aborts the batch with 0 calls to later vendors."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Mock DB queries:
    # 1. fetch_vendors: 3 vendors
    vendor_rows = [
        (101, "Vendor 1", "Tech", "07", 50.0, 10.0, 20.0, 15.0, 45.0, "HIGH", 100, 40, 40.0),
        (102, "Vendor 2", "Textiles", "27", 60.0, 12.0, 25.0, 18.0, 48.0, "HIGH", 120, 50, 41.6),
        (103, "Vendor 3", "Steel", "09", 70.0, 15.0, 30.0, 20.0, 52.0, "HIGH", 150, 60, 40.0),
    ]
    # Existing narratives: empty
    mock_cur.fetchall.side_effect = [
        vendor_rows,  # vendors query
        [],           # flags for v1
        [],           # flags for v2
        [],           # flags for v3
        [],           # existing narratives query
    ]

    mock_client = MagicMock()
    daily_quota_error = RuntimeError(
        "429 RESOURCE_EXHAUSTED: Quota exceeded for metric: "
        "generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 20"
    )
    mock_client.models.generate_content.side_effect = daily_quota_error

    with patch("google.genai.Client", return_value=mock_client):
        results = run_layer4(conn=mock_conn, api_key="test_key", risk_tiers="HIGH")

    assert results == []
    # Exactly ONE call was made across the entire batch (vendor 2 and vendor 3 received ZERO calls)
    assert mock_client.models.generate_content.call_count == 1


# ── Test 14: Temporary 429 Bounded Retry Still Works ──────────────────

def test_temporary_429_bounded_retry_still_works(sample_vendor, valid_narrative_json):
    """Temporary rate limit (without daily quota marker) retries with backoff and succeeds."""
    mock_client = MagicMock()
    temp_429 = RuntimeError("429 Too Many Requests: Rate limit exceeded, retry in 2s")
    mock_success = MagicMock()
    mock_success.text = valid_narrative_json

    # Attempt 1: temporary 429, Attempt 2: success
    mock_client.models.generate_content.side_effect = [temp_429, mock_success]

    err_out = {}
    narrative = generate_vendor_narrative(
        mock_client, sample_vendor, "gemini-3.8-flash", retries=3, error_out=err_out
    )

    assert narrative is not None
    assert mock_client.models.generate_content.call_count == 2
    assert err_out.get("error_type") == "none"


# ── Test 15: 503 Bounded Retry Still Works ────────────────────────────

def test_503_bounded_retry_still_works(sample_vendor, valid_narrative_json):
    """503 Service Unavailable retries with bounded backoff and succeeds on retry."""
    mock_client = MagicMock()
    err_503 = RuntimeError("503 Service Unavailable: The model is overloaded. Please try again.")
    mock_success = MagicMock()
    mock_success.text = valid_narrative_json

    mock_client.models.generate_content.side_effect = [err_503, mock_success]

    err_out = {}
    narrative = generate_vendor_narrative(
        mock_client, sample_vendor, "gemini-3.8-flash", retries=3, error_out=err_out
    )

    assert narrative is not None
    assert mock_client.models.generate_content.call_count == 2
    assert err_out.get("error_type") == "none"


# ── Test 16: Existing Valid Narrative -> Gemini Not Called ─────────────

def test_existing_valid_narrative_gemini_not_called(sample_vendor):
    """When a vendor already has a valid narrative in DB matching Layer 3, Gemini is NOT called."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    vendor_rows = [
        (151, "Khare and Sons", "Automobiles", "33", 83.29, 10.37, 100.0, 16.13, 51.32, "HIGH", 230, 191, 83.04),
    ]
    # Return existing narrative matching score 51.32 and tier HIGH
    existing_rows = [
        (151, "HIGH", 51.32, "Existing valid risk explanation narrative", "gemini-3.8-flash", "2026-09-30 10:00:00"),
    ]

    mock_cur.fetchall.side_effect = [
        vendor_rows,   # vendors
        [],            # flags
        existing_rows, # existing narratives
    ]

    mock_client = MagicMock()
    with patch("google.genai.Client", return_value=mock_client):
        results = run_layer4(conn=mock_conn, api_key="test_key", risk_tiers="HIGH")

    # Gemini generate_content was NOT called!
    assert mock_client.models.generate_content.call_count == 0


# ── Test 17: Existing Invalid Narrative -> Regenerates ────────────────

def test_existing_invalid_narrative_regenerates(sample_vendor, valid_narrative_json):
    """When existing narrative is empty or corrupt, it is treated as invalid and regenerated."""
    # Corrupt/empty narrative
    corrupt_record = {
        "vendor_id": 151,
        "risk_tier": "HIGH",
        "composite_score": 51.32,
        "risk_summary": "",  # empty summary
        "model_name": "gemini-3.8-flash",
    }
    assert is_narrative_valid_for_vendor(corrupt_record, sample_vendor) is False

    none_record = None
    assert is_narrative_valid_for_vendor(none_record, sample_vendor) is False


# ── Test 18: Layer 3 Score or Tier Mismatch Causes Regeneration ──────

def test_layer3_score_or_tier_mismatch_causes_regeneration(sample_vendor):
    """If Layer 3 score or tier changed, the old narrative is invalidated and regenerated."""
    # Stored was MEDIUM (score 30.0), but vendor is now HIGH (51.32)
    old_record = {
        "vendor_id": 151,
        "risk_tier": "MEDIUM",
        "composite_score": 30.0,
        "risk_summary": "Old medium risk narrative",
        "model_name": "gemini-3.8-flash",
    }
    assert is_narrative_valid_for_vendor(old_record, sample_vendor) is False

    # Score slightly changed from 51.32 to 55.00
    changed_score_record = {
        "vendor_id": 151,
        "risk_tier": "HIGH",
        "composite_score": 55.00,
        "risk_summary": "Old narrative with 55 score",
        "model_name": "gemini-3.8-flash",
    }
    assert is_narrative_valid_for_vendor(changed_score_record, sample_vendor) is False

    # Exact match passes
    matching_record = {
        "vendor_id": 151,
        "risk_tier": "HIGH",
        "composite_score": 51.32,
        "risk_summary": "Valid explanation matching current Layer 3",
        "model_name": "gemini-3.8-flash",
    }
    assert is_narrative_valid_for_vendor(matching_record, sample_vendor) is True


# ── Test 19: Limit Unprocessed Vendors ────────────────────────────────

def test_limit_unprocessed_vendors(valid_narrative_json):
    """--limit 1 processes at most 1 unprocessed vendor, skipping already-valid vendors."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    # Three vendors
    vendor_rows = [
        (101, "Vendor 1", "Tech", "07", 50.0, 10.0, 20.0, 15.0, 45.0, "HIGH", 100, 40, 40.0),
        (102, "Vendor 2", "Textiles", "27", 60.0, 12.0, 25.0, 18.0, 48.0, "HIGH", 120, 50, 41.6),
        (103, "Vendor 3", "Steel", "09", 70.0, 15.0, 30.0, 20.0, 52.0, "HIGH", 150, 60, 40.0),
    ]
    # Vendor 101 already has a valid narrative
    existing_rows = [
        (101, "HIGH", 45.0, "Valid narrative for v1", "gemini-3.8-flash", "2026-09-30"),
    ]

    mock_cur.fetchall.side_effect = [
        vendor_rows,   # vendors
        [], [], [],    # flags for v1, v2, v3
        existing_rows, # existing narratives
    ]

    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.text = valid_narrative_json
    mock_client.models.generate_content.return_value = mock_resp

    with patch("google.genai.Client", return_value=mock_client):
        # Limit = 1 unprocessed vendor
        results = run_layer4(conn=mock_conn, api_key="test_key", risk_tiers="HIGH", limit=1)

    # Vendor 101 was skipped because it already has a narrative.
    # Vendor 102 was processed (1 unprocessed attempted).
    # Vendor 103 was NOT processed because limit of 1 was satisfied.
    assert mock_client.models.generate_content.call_count == 1
    assert len(results) == 1
    assert results[0]["vendor_id"] == 102


# ── Test 20: Dry Run Makes Zero Gemini Requests and Zero DB Writes ────

def test_dry_run_makes_zero_gemini_calls_and_no_db_writes():
    """--dry-run calculates workload, makes ZERO Gemini API requests, and ZERO database modifications."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    vendor_rows = [
        (101, "Vendor 1", "Tech", "07", 50.0, 10.0, 20.0, 15.0, 45.0, "HIGH", 100, 40, 40.0),
        (102, "Vendor 2", "Textiles", "27", 60.0, 12.0, 25.0, 18.0, 48.0, "HIGH", 120, 50, 41.6),
    ]
    existing_rows = [
        (101, "HIGH", 45.0, "Valid narrative for v1", "gemini-3.8-flash", "2026-09-30"),
    ]

    mock_cur.fetchall.side_effect = [
        vendor_rows,   # vendors
        [], [],        # flags
        existing_rows, # existing narratives
    ]

    mock_client = MagicMock()
    with patch("google.genai.Client", return_value=mock_client):
        results = run_layer4(conn=mock_conn, api_key="test_key", risk_tiers="HIGH", dry_run=True)

    # ZERO Gemini calls
    assert mock_client.models.generate_content.call_count == 0
    # ZERO results
    assert results == []
    # No INSERT, UPDATE, or DELETE executed on DB
    for call in mock_cur.execute.call_args_list:
        sql = call[0][0].upper()
        assert "INSERT INTO" not in sql
        assert "UPDATE" not in sql
        assert "DELETE FROM" not in sql


# ── Test 21: Resume Skips Previously Successful Vendors ───────────────

def test_resume_skips_previously_successful_vendors(valid_narrative_json):
    """Resume seamlessly skips already processed vendors and generates only for remaining."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    vendor_rows = [
        (101, "Vendor 1", "Tech", "07", 50.0, 10.0, 20.0, 15.0, 45.0, "HIGH", 100, 40, 40.0),
        (102, "Vendor 2", "Textiles", "27", 60.0, 12.0, 25.0, 18.0, 48.0, "HIGH", 120, 50, 41.6),
    ]
    # Vendor 101 already processed
    existing_rows = [
        (101, "HIGH", 45.0, "Valid narrative for v1", "gemini-3.8-flash", "2026-09-30"),
    ]

    mock_cur.fetchall.side_effect = [
        vendor_rows,   # vendors
        [], [],        # flags
        existing_rows, # existing narratives
    ]

    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.text = valid_narrative_json
    mock_client.models.generate_content.return_value = mock_resp

    with patch("google.genai.Client", return_value=mock_client):
        results = run_layer4(conn=mock_conn, api_key="test_key", risk_tiers="HIGH")

    # Only Vendor 102 was processed
    assert mock_client.models.generate_content.call_count == 1
    assert len(results) == 1
    assert results[0]["vendor_id"] == 102

