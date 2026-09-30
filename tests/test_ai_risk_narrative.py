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
    RiskNarrative,
    build_user_prompt,
    fetch_vendors_for_narrative,
    generate_vendor_narrative,
    init_narrative_table,
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
