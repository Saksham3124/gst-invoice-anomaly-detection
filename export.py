import psycopg2
import pandas as pd
import os

conn = psycopg2.connect(
    dbname="gst_analytics",
    user="postgres",
    password="Saksham@3124",
    host="localhost",
    port="5432"
)

output_dir = "tableau_exports"
os.makedirs(output_dir, exist_ok=True)

print("=" * 50)
print("EXPORTING DATA FOR TABLEAU")
print("=" * 50)

# ── Export 1: Overview KPIs ───────────────────────────
print("\n[1] Exporting overview KPIs...")

query = """
    SELECT
        COUNT(*)                                          AS total_invoices,
        SUM(amount)                                       AS total_amount,
        SUM(tax_claimed)                                  AS total_tax_claimed,
        COUNT(*) FILTER (WHERE validation_status = 'FLAGGED') AS total_flagged,
        ROUND(
            COUNT(*) FILTER (WHERE validation_status = 'FLAGGED')
            * 100.0 / COUNT(*), 2
        )                                                 AS flag_rate_pct
    FROM invoices
"""
df = pd.read_sql(query, conn)
df.to_csv(f"{output_dir}/kpi_overview.csv", index=False)
print(f"    Exported: kpi_overview.csv")

# ── Export 2: Flags by Type & Severity ───────────────
print("\n[2] Exporting flags breakdown...")

query = """
    SELECT
        flag_type,
        severity,
        COUNT(*)            AS flag_count,
        COUNT(DISTINCT vendor_id) AS vendors_affected
    FROM flags
    GROUP BY flag_type, severity
    ORDER BY flag_count DESC
"""
df = pd.read_sql(query, conn)
df.to_csv(f"{output_dir}/flags_breakdown.csv", index=False)
print(f"    Exported: flags_breakdown.csv")

# ── Export 3: Monthly Trend ───────────────────────────
print("\n[3] Exporting monthly invoice trends...")

query = """
    SELECT
        DATE_TRUNC('month', i.invoice_date)::DATE   AS month,
        COUNT(*)                                     AS total_invoices,
        COUNT(*) FILTER (WHERE i.validation_status = 'FLAGGED') AS flagged,
        ROUND(SUM(i.amount), 2)                      AS total_amount,
        ROUND(AVG(i.amount), 2)                      AS avg_amount
    FROM invoices i
    GROUP BY DATE_TRUNC('month', i.invoice_date)
    ORDER BY month
"""
df = pd.read_sql(query, conn)
df.to_csv(f"{output_dir}/monthly_trends.csv", index=False)
print(f"    Exported: monthly_trends.csv")

# ── Export 4: Vendor Risk Scores ──────────────────────
print("\n[4] Exporting vendor risk scores...")

query = """
    SELECT
        vr.vendor_id,
        v.vendor_name,
        v.category,
        v.state_code,
        vr.anomaly_frequency,
        vr.deviation_magnitude,
        vr.validation_failures,
        vr.recency_score,
        vr.composite_score,
        vr.risk_tier
    FROM vendor_risk_scores vr
    JOIN vendors v ON vr.vendor_id = v.vendor_id
    ORDER BY vr.composite_score DESC
"""
df = pd.read_sql(query, conn)
df.to_csv(f"{output_dir}/vendor_risk_scores.csv", index=False)
print(f"    Exported: vendor_risk_scores.csv")

# ── Export 5: Invoice Detail with Flags ───────────────
print("\n[5] Exporting invoice detail with flag info...")

query = """
    SELECT
        i.invoice_id,
        i.vendor_id,
        v.vendor_name,
        v.category,
        gc.category_name      AS gst_category,
        gc.gst_rate,
        i.invoice_date,
        i.amount,
        i.tax_claimed,
        i.state_code,
        i.validation_status,
        STRING_AGG(DISTINCT f.flag_type, ', ') AS flag_types,
        MAX(f.severity)                         AS max_severity
    FROM invoices i
    JOIN vendors v        ON i.vendor_id   = v.vendor_id
    JOIN gst_categories gc ON i.category_id = gc.category_id
    LEFT JOIN flags f     ON i.invoice_id  = f.invoice_id
    GROUP BY
        i.invoice_id, i.vendor_id, v.vendor_name, v.category,
        gc.category_name, gc.gst_rate,
        i.invoice_date, i.amount, i.tax_claimed,
        i.state_code, i.validation_status
    ORDER BY i.invoice_date
"""
df = pd.read_sql(query, conn)
df.to_csv(f"{output_dir}/invoice_detail.csv", index=False)
print(f"    Exported: invoice_detail.csv")

# ── Export 6: Category Risk Summary ──────────────────
print("\n[6] Exporting category risk summary...")

query = """
    SELECT
        gc.category_name,
        gc.gst_rate,
        COUNT(i.invoice_id)                          AS total_invoices,
        COUNT(*) FILTER (WHERE i.validation_status = 'FLAGGED') AS flagged_invoices,
        ROUND(AVG(i.amount), 2)                      AS avg_amount,
        ROUND(SUM(i.amount), 2)                      AS total_amount,
        ROUND(
            COUNT(*) FILTER (WHERE i.validation_status = 'FLAGGED')
            * 100.0 / COUNT(*), 2
        )                                            AS flag_rate_pct
    FROM invoices i
    JOIN gst_categories gc ON i.category_id = gc.category_id
    GROUP BY gc.category_name, gc.gst_rate
    ORDER BY flag_rate_pct DESC
"""
df = pd.read_sql(query, conn)
df.to_csv(f"{output_dir}/category_risk.csv", index=False)
print(f"    Exported: category_risk.csv")

conn.close()

# ── Summary ───────────────────────────────────────────
print("\n" + "=" * 50)
print("EXPORT SUMMARY")
print("=" * 50)
files = os.listdir(output_dir)
for f in files:
    size = os.path.getsize(f"{output_dir}/{f}")
    print(f"  {f:<35} {size/1024:.1f} KB")

print(f"\n✅ All files exported to '{output_dir}/' folder.")
print("   Ready to connect to Tableau!")