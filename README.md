# GST Invoice Anomaly Detection & Vendor Risk Scoring

An end-to-end GST invoice analytics and vendor risk assessment system built with **Python, PostgreSQL, statistical anomaly detection, Gemini AI, and Tableau**.

The project processes **50,000+ GST invoice records across 210 vendors**, detects deterministic validation failures and statistical anomalies, calculates vendor-level risk scores, and uses Gemini AI to generate structured risk narratives that explain the already-computed risk signals.

The architecture follows:

> **DETECT → SCORE → EXPLAIN**

- **DETECT** — identify invoice validation failures and statistical anomalies.
- **SCORE** — calculate deterministic vendor risk scores from validated signals.
- **EXPLAIN** — use Gemini AI to explain the deterministic risk signals and suggest investigation priorities.

---

## Project Overview

Traditional anomaly detection can identify suspicious records, but turning those signals into an understandable vendor-level risk assessment requires multiple analytical layers.

This project combines:

1. Deterministic invoice validation
2. Statistical anomaly detection
3. Vendor-level risk scoring
4. AI-generated risk narratives
5. Tableau-based visualization

The system is designed so that the AI layer **does not determine fraud or risk**.

The authoritative risk tier and composite score are calculated by the deterministic Layer 3 scoring engine.

Gemini only explains those validated signals.

### Architecture

```text
                    GST Invoice Data
                           │
                           ▼
              ┌────────────────────────┐
              │ Layer 1: Validation     │
              │                        │
              │ Rule-based checks      │
              │ GSTIN mismatches       │
              │ Duplicates             │
              │ Invalid values         │
              │ Structural validation  │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ Layer 2: Anomaly       │
              │ Detection              │
              │                        │
              │ Z-score anomalies      │
              │ IQR outliers           │
              │ Rolling spikes         │
              │ Statistical signals    │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ Layer 3: Risk Scoring  │
              │                        │
              │ Deterministic scoring  │
              │ Vendor aggregation     │
              │ Risk tier assignment   │
              └────────────┬───────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │ Layer 4: Gemini AI     │
              │ Risk Narrative         │
              │                        │
              │ Explain signals        │
              │ Summarize evidence     │
              │ Suggest investigation  │
              │ priorities             │
              └────────────┬───────────┘
                           │
                           ▼
                 PostgreSQL / CSV
                           │
                           ▼
                    Tableau Dashboard
```

---

# Key Results

| Metric | Result |
|---|---:|
| Invoice records analyzed | 50,000+ |
| Vendors analyzed | 210 |
| Current HIGH-risk vendors | 32 |
| Current MEDIUM-risk vendors | 115 |
| Current LOW-risk vendors | 63 |
| Automated Layer 4 tests | 22 passing |
| Layer 4 AI narratives | 32 |
| Tableau AI narratives connected | 32 |
| Tableau dashboard worksheets | 8 |

The current Layer 3 risk distribution is:

```text
HIGH      32
MEDIUM   115
LOW       63
```

The AI narrative layer currently generates narratives for the configured **HIGH-risk vendors**.

---

# Layer 1 — Deterministic Invoice Validation

The first layer performs rule-based validation of invoice records.

Examples include:

- GSTIN validation
- Duplicate invoice detection
- Invalid or missing values
- Structural validation
- Rule-based consistency checks
- Other configured invoice-level validation rules

The output of Layer 1 becomes the foundation for subsequent analytical layers.

The objective is to establish reliable and reproducible invoice-level signals before statistical analysis or vendor scoring.

---

# Layer 2 — Statistical Anomaly Detection

Layer 2 identifies unusual invoice behavior using statistical methods.

Implemented techniques include:

### Z-score analysis

Identifies observations that deviate significantly from expected distributions.

### IQR outlier detection

Identifies observations outside the interquartile range.

### Rolling spike detection

Identifies unusually large changes relative to historical invoice behavior.

These statistical signals are stored alongside the deterministic validation results and are later aggregated at the vendor level.

---

# Layer 3 — Deterministic Vendor Risk Scoring

Layer 3 converts invoice-level signals into a vendor-level risk score.

The composite score uses four deterministic components:

| Component | Weight |
|---|---:|
| Anomaly frequency | 30% |
| Deviation magnitude | 30% |
| Validation failures | 20% |
| Recency | 20% |

Risk tiers are assigned using deterministic thresholds:

```text
HIGH       >= 35
MEDIUM     >= 20
LOW        < 20
```

The scoring engine is the **authoritative source of risk classification**.

Gemini does not modify these scores or tiers.

---

# Layer 4 — Gemini AI Risk Narrative

Layer 4 adds an explanation layer on top of the deterministic risk engine.

Gemini receives validated vendor-level signals such as:

- Vendor ID
- Vendor name
- Risk tier
- Composite risk score
- Anomaly frequency
- Deviation magnitude
- Validation failures
- Recency score
- Total invoices
- Flagged invoices
- Invoice flag rate
- Relevant anomaly counts and types

The model generates structured information including:

- Risk summary
- Key drivers
- Investigation priorities
- Evidence

### Important Design Principle

Gemini is an **explanation layer**, not a risk-scoring engine.

```text
Layer 1 → DETECT
Layer 2 → DETECT
Layer 3 → SCORE
Layer 4 → EXPLAIN
```

Gemini does **not**:

- Detect fraud independently
- Calculate the composite risk score
- Assign the HIGH/MEDIUM/LOW tier
- Modify the deterministic score
- Override Layer 3 results
- Invent supporting evidence

This separation keeps the risk calculation reproducible and auditable while allowing the dashboard to provide human-readable explanations.

---

# Example AI Risk Narrative

For a high-risk vendor, the dashboard can surface information such as:

```text
AI RISK NARRATIVE

Vaidya-Dhawan

HIGH · Score 52.29

RISK SUMMARY

Vaidya-Dhawan has been deterministically assigned a HIGH risk
tier with a composite score of 52.29 out of 100.0.

KEY DRIVERS

• Critical validation failures score of 100.0 out of 100.0
• High anomaly frequency score of 82.14 out of 100.0
• Presence of high-severity rolling spikes and GSTIN mismatches

INVESTIGATION PRIORITIES

• Review invoices flagged for GSTIN mismatches.
• Examine invoices identified with rolling spikes.

EVIDENCE

Score: 52.29 | Total Invoices: 355 |
Flagged Invoices: 127 (35.77%)

Validation Score: 100.0 | Anomaly Score: 82.1
```

The narrative is generated from the deterministic Layer 3 signals.

---

# Tableau Dashboard

The project includes an interactive Tableau dashboard combining the analytical layers.

### Dashboard components

- KPI cards
- Monthly revenue trend
- Top risky vendors
- Risk by category
- AI Risk Narrative panel

The AI panel is connected to the vendor risk table using:

```text
vendor_risk_scores.vendor_id
        =
ai_risk_narratives.vendor_id
```

The dashboard uses a **logical relationship** rather than a physical cross-join.

This preserves the underlying invoice and vendor row counts.

### Dashboard interaction

Selecting a vendor from **Top Risky Vendors** updates the AI Risk Narrative panel.

For example:

```text
Top Risky Vendors
       │
       │ Vendor selection
       ▼
AI Risk Narrative
```

The panel displays the corresponding:

- Vendor
- Risk tier
- Composite score
- Risk summary
- Key drivers
- Investigation priorities
- Evidence

When no vendor is selected, the panel defaults to the highest-ranked vendor.

---

# Tableau Data

The project exports the following datasets for Tableau:

```text
tableau_exports/
│
├── ai_risk_narratives.csv
├── category_risk.csv
├── flags_breakdown.csv
├── invoice_detail.csv
├── kpi_overview.csv
├── monthly_trends.csv
└── vendor_risk_scores.csv
```

### `ai_risk_narratives.csv`

Contains the structured Layer 4 output:

```text
vendor_id
vendor_name
category
risk_tier
composite_score
risk_summary
key_drivers
investigation_priorities
evidence
model_name
generated_at
```

---

# Project Structure

```text
GST/
│
├── ai_risk_narrative.py
├── export.py
│
├── layer1_validation.py
├── layer2_statistical.py
├── layer3_scoring.py
│
├── schema.py
├── simulation.py
│
├── tests/
│   └── test_ai_risk_narrative.py
│
├── tableau_exports/
│   ├── ai_risk_narratives.csv
│   ├── category_risk.csv
│   ├── flags_breakdown.csv
│   ├── invoice_detail.csv
│   ├── kpi_overview.csv
│   ├── monthly_trends.csv
│   └── vendor_risk_scores.csv
│
├── dashboard.twbx
├── dashboard.png
│
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

# Technology Stack

### Programming & Analytics

- Python
- Pandas
- NumPy
- SQL
- PostgreSQL

### Statistical Analysis

- Z-score analysis
- IQR outlier detection
- Rolling-window analysis
- Statistical anomaly detection

### Data Engineering

- ETL/ELT
- PostgreSQL
- Data validation
- Automated testing
- Environment-based configuration

### AI

- Google Gemini API
- Structured AI output
- Gemini risk narrative generation

### Visualization

- Tableau
- CSV-based Tableau exports
- Interactive dashboard filtering

### Testing

- Pytest
- Mocked Gemini API responses
- Database-level validation
- Quota-aware API handling

---

# Layer 4 Reliability Features

The AI pipeline was designed to remain safe and resumable when API availability or quota becomes an issue.

Implemented features include:

### Quota-aware execution

Daily Gemini quota exhaustion is detected and stops the batch rather than repeatedly sending failed requests.

### Bounded retries

Temporary `429` and `503` responses can be retried with bounded backoff.

Permanent errors such as invalid configuration or unavailable models fail without unnecessary retries.

### Resume support

Already-generated valid narratives are skipped.

This allows the pipeline to resume without regenerating previously successful results.

### Validation before regeneration

Existing narratives can be regenerated when their stored risk tier or composite score no longer matches the current Layer 3 result.

### Dry-run mode

The pipeline supports:

```bash
python ai_risk_narrative.py --dry-run
```

This checks eligible vendors and remaining work without making Gemini API calls or modifying the database.

### Controlled execution

A limited run can be performed with:

```bash
python ai_risk_narrative.py --limit 1
```

This is useful for validating the live API connection before processing the remaining vendors.

---

# Testing

Layer 4 includes automated tests covering:

- Gemini API response handling
- Structured output validation
- Daily quota detection
- `429` retry behavior
- `503` retry behavior
- Immediate stop on exhausted daily quota
- Existing narrative detection
- Invalid narrative regeneration
- Layer 3 score/tier mismatch detection
- Resume behavior
- `--limit`
- `--dry-run`
- Database write behavior
- Zero API calls during dry runs

Current test result:

```text
22 passed
```

Run the Layer 4 test suite with:

```bash
python -m pytest tests/test_ai_risk_narrative.py
```

---

# Configuration

Create a local `.env` file from `.env.example`.

Example:

```env
GEMINI_API_KEY=your_gemini_api_key
GEMINI_MODEL=gemini-3.8-flash

AI_RISK_TIERS=HIGH

DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_database_user
DB_PASSWORD=your_database_password
```

The `.env` file is excluded from Git through `.gitignore`.

Never commit API keys or database credentials.

---

# Running the Pipeline

## 1. Install dependencies

```bash
pip install -r requirements.txt
```

## 2. Run Layer 1

```bash
python layer1_validation.py
```

## 3. Run Layer 2

```bash
python layer2_statistical.py
```

## 4. Run Layer 3

```bash
python layer3_scoring.py
```

## 5. Check Layer 4 without making API calls

```bash
python ai_risk_narrative.py --dry-run
```

## 6. Run a controlled AI test

```bash
python ai_risk_narrative.py --limit 1
```

## 7. Generate the Tableau exports

```bash
python export.py
```

The resulting CSV files are written to:

```text
tableau_exports/
```

---

# Database Design

The project separates deterministic risk calculations from AI-generated explanations.

Conceptually:

```text
Invoice Data
    │
    ├── Layer 1 validation
    │
    └── Layer 2 anomaly detection
              │
              ▼
       Vendor Risk Scores
              │
              ├── risk_tier
              ├── composite_score
              ├── anomaly signals
              └── validation signals
                       │
                       ▼
              AI Risk Narratives
                       │
                       ├── risk_summary
                       ├── key_drivers
                       ├── investigation_priorities
                       └── evidence
```

The AI narrative data is stored separately from the authoritative vendor risk scores.

This prevents the AI layer from overwriting deterministic risk calculations.

---

# Data Integrity

The Tableau integration was designed to preserve the underlying data model.

Verified baseline values include:

```text
Total Orders      : 50,000
Total Amount      : ₹17.754B
Flagged Orders    : 7,881
Flag Rate         : 15.76%
Vendor Records    : 210
AI Narratives     : 32
```

The AI relationship is based on `vendor_id` and does not physically duplicate the invoice-level data.

---

# Security

Sensitive configuration is loaded through environment variables.

The repository includes:

```text
.env.example
```

but the actual:

```text
.env
```

file is ignored by Git.

API credentials and database passwords should never be committed to the repository.

If a credential has previously been exposed in Git history, it should be rotated even after the current working tree has been cleaned.

---

# Design Principles

### Deterministic risk calculation

Risk scores and tiers are calculated by explicit rules rather than generated by an LLM.

### Explainability

AI output is generated from existing risk signals rather than replacing the underlying analytical model.

### Separation of concerns

```text
Detection
    ↓
Scoring
    ↓
Explanation
    ↓
Visualization
```

Each stage has a clearly defined responsibility.

### Reproducibility

The same validated inputs and scoring rules produce deterministic Layer 3 results.

### Resumability

Layer 4 can stop and resume without rebuilding successful narratives.

### Auditability

The AI layer retains the underlying evidence and deterministic score used to generate each narrative.

---

# Project Outcome

The final system combines:

```text
50K+ invoices
      ↓
Rule-based validation
      ↓
Statistical anomaly detection
      ↓
Deterministic vendor risk scoring
      ↓
32 HIGH-risk vendor narratives
      ↓
Interactive Tableau dashboard
```

The result is an end-to-end analytical workflow that connects **data quality, anomaly detection, risk scoring, AI-assisted explanation, and business visualization** in a single project.

---

## Dashboard

The repository includes the packaged Tableau workbook:

```text
dashboard.twbx
```

and a dashboard preview:

```text
dashboard.png
```

The Tableau dashboard provides interactive vendor risk analysis and an AI-generated narrative panel driven by the selected vendor.

---

## Author

**Kumar Saksham**

B.Tech — Electronics & Communication Engineering  
Birla Institute of Technology, Mesra

**GitHub:**  
https://github.com/Saksham3124

**Portfolio:**  
https://kumarsaksham.vercel.app/

**LinkedIn:**  
https://www.linkedin.com/in/kumarsaksham/
