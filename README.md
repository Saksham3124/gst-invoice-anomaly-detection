# GST Invoice Anomaly Detection & Vendor Risk Scoring

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python\&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-17-336791?logo=postgresql\&logoColor=white)](https://www.postgresql.org/)
[![Tableau](https://img.shields.io/badge/Tableau-Public-E97627?logo=tableau\&logoColor=white)](https://public.tableau.com/app/profile/kumar.saksham2703/viz/GST__/Dashboard1)
[![Gemini](https://img.shields.io/badge/Gemini-AI-8E75C2?logo=google\&logoColor=white)](https://ai.google.dev/)
[![Tests](https://img.shields.io/badge/Tests-22%20Passed-brightgreen?logo=pytest\&logoColor=white)](tests/)

An end-to-end analytics and risk-scoring pipeline for GST invoice data. The system validates transactional data, detects statistical anomalies, calculates deterministic vendor risk scores, and uses Gemini to generate evidence-grounded investigation narratives.

**Architecture:** `Detect → Score → Explain`

* **Layers 1–2:** Detect data-quality issues and statistical anomalies
* **Layer 3:** Calculate deterministic vendor risk scores
* **Layer 4:** Explain validated risk signals using Gemini
* **Tableau:** Provide interactive investigation and reporting

---

## 📈 Key Results

| Metric              |      Result |
| ------------------- | ----------: |
| Invoices Processed  |  **50,000** |
| Vendors Analyzed    |     **210** |
| Flagged Invoices    |   **7,881** |
| Overall Flag Rate   |  **15.76%** |
| HIGH Risk Vendors   |      **32** |
| MEDIUM Risk Vendors |     **115** |
| LOW Risk Vendors    |      **63** |
| Transaction Value   | **₹17.75B** |
| Anomaly Types       |       **6** |

---

## 📊 Dashboard

The Tableau dashboard combines executive KPIs, revenue trends, vendor risk scoring, category-level analysis, and AI-assisted investigation.

**[View Live Tableau Dashboard](https://public.tableau.com/app/profile/kumar.saksham2703/viz/GST__/Dashboard1)**

![GST Invoice Risk Dashboard](dashboard.png)

### Dashboard Includes

* Executive KPI cards
* Monthly revenue trends
* Top risky vendors
* Risk distribution by category
* Interactive vendor filtering
* Deterministic risk scores
* AI Risk Narrative panel
* Investigation priorities and supporting evidence

---

## 🏗️ Architecture

```text
Synthetic GST Data
        │
        ▼
   PostgreSQL
        │
        ▼
┌─────────────────────────┐
│ Layer 1                 │
│ Rule-Based Validation   │
│                         │
│ • Duplicates            │
│ • GSTIN mismatches      │
│ • Invalid amounts       │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ Layer 2                 │
│ Statistical Detection   │
│                         │
│ • Z-Score               │
│ • Rolling Spikes        │
│ • IQR Outliers          │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ Layer 3                 │
│ Vendor Risk Scoring     │
│                         │
│ • Frequency             │
│ • Deviation             │
│ • Validation            │
│ • Recency               │
└────────────┬────────────┘
             │
             ▼
      Vendor Risk Scores
             │
             ▼
┌─────────────────────────┐
│ Layer 4                 │
│ Gemini Risk Narrative   │
└────────────┬────────────┘
             │
             ▼
      Tableau Dashboard
```

---

## 🔍 Anomaly Detection

### Rule-Based Checks

Layer 1 identifies transactional integrity issues:

* Duplicate invoices
* GSTIN/state-code mismatches
* Invalid invoice amounts
* Missing critical fields

### Statistical Detection

Layer 2 identifies behavioral anomalies using PostgreSQL analytical SQL:

* **Vendor Z-Score:** Detects transactions significantly above vendor baselines
* **30-invoice rolling average:** Detects sudden vendor-level volume spikes
* **IQR analysis:** Identifies category-level transaction outliers

---

## 📊 Deterministic Risk Scoring

Layer 3 combines four independent signals into a normalized `0–100` vendor risk score:

| Signal              | Weight |
| ------------------- | -----: |
| Anomaly Frequency   |    30% |
| Deviation Magnitude |    30% |
| Validation Failures |    20% |
| Recency Trend       |    20% |

```text
Composite Score =
    0.30 × Anomaly Frequency
  + 0.30 × Deviation Magnitude
  + 0.20 × Validation Failures
  + 0.20 × Recency Trend
```

Risk tiers are assigned deterministically:

```text
HIGH    → Score ≥ 35
MEDIUM  → Score ≥ 20 and < 35
LOW     → Score < 20
```

Layer 3 is the **authoritative source** for risk scores and classifications.

---

## 🤖 AI Risk Narrative

Gemini is used as an **explanation layer**, not as the risk engine.

The model receives validated metrics such as:

* Risk tier
* Composite score
* Anomaly frequency
* Deviation magnitude
* Validation failures
* Recency score
* Flagged invoice counts
* Anomaly types

It produces structured:

* Risk summary
* Key drivers
* Investigation priorities
* Supporting evidence

### AI Governance

Gemini does **not**:

* Calculate the risk score
* Assign risk tiers
* Detect anomalies
* Override analytical results
* Invent investigation evidence

This keeps the numerical risk pipeline deterministic while using AI to make the results easier to interpret.

---

## 🛠️ Tech Stack

| Technology         | Purpose                                   |
| ------------------ | ----------------------------------------- |
| **Python 3.12**    | Pipeline orchestration and analytics      |
| **PostgreSQL 17**  | Relational storage and analytical SQL     |
| **SQL**            | Window functions and statistical analysis |
| **Pandas / NumPy** | Data processing                           |
| **Gemini API**     | Risk narrative generation                 |
| **Pydantic**       | Structured AI output validation           |
| **Tableau Public** | Interactive dashboard                     |
| **Pytest**         | Automated testing                         |
| **Faker**          | Synthetic data generation                 |
| **Git**            | Version control                           |

---

## 📁 Project Structure

```text
gst-invoice-anomaly-detection/
│
├── simulation.py
├── schema.py
├── layer1_validation.py
├── layer2_statistical.py
├── layer3_scoring.py
├── ai_risk_narrative.py
├── export.py
│
├── tests/
│   └── test_ai_risk_narrative.py
│
├── tableau_exports/
│   ├── category_risk.csv
│   ├── flags_breakdown.csv
│   ├── invoice_detail.csv
│   ├── kpi_overview.csv
│   ├── monthly_trends.csv
│   ├── vendor_risk_scores.csv
│   └── ai_risk_narratives.csv
│
├── dashboard.png
├── dashboard.twbx
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## 🚀 Setup

### 1. Clone

```bash
git clone https://github.com/Saksham3124/gst-invoice-anomaly-detection.git
cd gst-invoice-anomaly-detection
```

### 2. Create Environment

```bash
python -m venv gst_env
```

**Windows:**

```powershell
gst_env\Scripts\activate
```

**macOS / Linux:**

```bash
source gst_env/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

Copy `.env.example` to `.env` and configure PostgreSQL and Gemini credentials.

```env
GEMINI_API_KEY=your_api_key
GEMINI_MODEL=gemini-3.8-flash

DB_HOST=localhost
DB_PORT=5432
DB_NAME=gst_analytics
DB_USER=postgres
DB_PASSWORD=your_password
```

Never commit `.env` or API credentials.

---

## ▶️ Run the Pipeline

Run the components sequentially:

```bash
python simulation.py
python layer1_validation.py
python layer2_statistical.py
python layer3_scoring.py
python ai_risk_narrative.py
python export.py
```

### Test AI Generation

```bash
python ai_risk_narrative.py --limit 1
```

### Dry Run

```bash
python ai_risk_narrative.py --dry-run
```

---

## 🧪 Testing

The Layer 4 test suite covers structured AI output, error handling, quota handling, retries, resume behavior, database isolation, deterministic score protection, and CLI behavior.

```bash
pytest -v
```

**Verified result:**

```text
22 passed
0 failed
```

---

## 💡 What This Project Demonstrates

### Data Engineering

* Relational database design
* Data validation
* ETL-style processing
* Structured data exports
* Resumable processing

### Analytics & Risk

* Statistical anomaly detection
* Window functions
* Z-Scores
* Rolling averages
* IQR outlier detection
* Multi-signal risk scoring
* Deterministic risk classification

### AI Engineering

* Gemini API integration
* Structured LLM output
* Pydantic validation
* Evidence-grounded prompting
* API error handling
* Rate-limit and quota handling
* AI governance

### Business Intelligence

* Tableau dashboard development
* KPI reporting
* Vendor drill-down
* Risk visualization
* AI-assisted investigation

---

## 👤 Author

**Kumar Saksham**

* **Portfolio:** [kumarsaksham.vercel.app](https://kumarsaksham.vercel.app/)
* **GitHub:** [@Saksham3124](https://github.com/Saksham3124)
* **LinkedIn:** [Kumar Saksham](https://www.linkedin.com/in/kumarsaksham/)
* **Tableau:** [Kumar Saksham](https://public.tableau.com/app/profile/kumar.saksham2703/)
* **Live Dashboard:** [GST Invoice Risk Dashboard](https://public.tableau.com/app/profile/kumar.saksham2703/viz/GST__/Dashboard1)

---

## 📄 License

This is a portfolio and educational analytics project.

The generated GST invoice data is synthetic and does not represent real taxpayer information.
