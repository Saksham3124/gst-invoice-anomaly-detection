import psycopg2

conn = psycopg2.connect(
    dbname="gst_analytics",
    user="postgres",
    password="Saksham@3124",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

print("=" * 50)
print("LAYER 3 — VENDOR RISK SCORING MODEL")
print("=" * 50)

# ── Signal 1: Anomaly Frequency ──────────────────────
# % of a vendor's invoices that are flagged
print("\n[1] Calculating anomaly frequency per vendor...")

cur.execute("""
    WITH vendor_totals AS (
        SELECT vendor_id, COUNT(*) AS total_invoices
        FROM invoices
        GROUP BY vendor_id
    ),
    vendor_flagged AS (
        SELECT vendor_id, COUNT(DISTINCT invoice_id) AS flagged_invoices
        FROM flags
        GROUP BY vendor_id
    )
    SELECT
        vt.vendor_id,
        vt.total_invoices,
        COALESCE(vf.flagged_invoices, 0) AS flagged_invoices,
        ROUND(
            COALESCE(vf.flagged_invoices, 0) * 100.0 / vt.total_invoices,
        2) AS flag_rate
    FROM vendor_totals vt
    LEFT JOIN vendor_flagged vf ON vt.vendor_id = vf.vendor_id
    ORDER BY flag_rate DESC
""")
frequency_data = cur.fetchall()
print(f"    Calculated for {len(frequency_data)} vendors.")

# ── Signal 2: Deviation Magnitude ────────────────────
# Average z-score of flagged invoices per vendor
print("\n[2] Calculating deviation magnitude per vendor...")

cur.execute("""
    SELECT
        vendor_id,
        AVG(
            CAST(
                SPLIT_PART(
                    SPLIT_PART(details, 'Z-score: ', 2),
                ' |', 1) AS FLOAT
            )
        ) AS avg_zscore
    FROM flags
    WHERE flag_type IN ('STATISTICAL_ZSCORE')
      AND details LIKE 'Z-score:%'
    GROUP BY vendor_id
""")
zscore_data = {row[0]: row[1] for row in cur.fetchall()}
print(f"    Z-score data retrieved for {len(zscore_data)} vendors.")

# ── Signal 3: Validation Failures ────────────────────
# % of vendor invoices with HIGH severity flags
print("\n[3] Calculating validation failure rate per vendor...")

cur.execute("""
    WITH vendor_totals AS (
        SELECT vendor_id, COUNT(*) AS total_invoices
        FROM invoices
        GROUP BY vendor_id
    ),
    high_severity AS (
        SELECT vendor_id, COUNT(DISTINCT invoice_id) AS high_flags
        FROM flags
        WHERE severity = 'HIGH'
        GROUP BY vendor_id
    )
    SELECT
        vt.vendor_id,
        ROUND(
            COALESCE(hs.high_flags, 0) * 100.0 / vt.total_invoices,
        2) AS high_flag_rate
    FROM vendor_totals vt
    LEFT JOIN high_severity hs ON vt.vendor_id = hs.vendor_id
""")
validation_data = {row[0]: float(row[1]) for row in cur.fetchall()}
print(f"    Calculated for {len(validation_data)} vendors.")

# ── Signal 4: Recency Score ───────────────────────────
# Are anomalies increasing in the last 6 months vs before?
print("\n[4] Calculating recency trend per vendor...")

cur.execute("""
    WITH recent AS (
        SELECT f.vendor_id, COUNT(*) AS recent_flags
        FROM flags f
        JOIN invoices i ON f.invoice_id = i.invoice_id
        WHERE i.invoice_date >= '2024-07-01'
        GROUP BY f.vendor_id
    ),
    older AS (
        SELECT f.vendor_id, COUNT(*) AS older_flags
        FROM flags f
        JOIN invoices i ON f.invoice_id = i.invoice_id
        WHERE i.invoice_date < '2024-07-01'
        GROUP BY f.vendor_id
    )
    SELECT
        COALESCE(r.vendor_id, o.vendor_id) AS vendor_id,
        COALESCE(r.recent_flags, 0) AS recent_flags,
        COALESCE(o.older_flags, 0) AS older_flags,
        CASE
            WHEN COALESCE(o.older_flags, 0) = 0 THEN 100
            ELSE ROUND(
                COALESCE(r.recent_flags, 0) * 100.0 /
                NULLIF(o.older_flags, 0),
            2)
        END AS recency_ratio
    FROM recent r
    FULL OUTER JOIN older o ON r.vendor_id = o.vendor_id
""")
recency_data = {row[0]: float(row[3]) for row in cur.fetchall()}
print(f"    Recency trends calculated for {len(recency_data)} vendors.")

# ── Build Composite Score ─────────────────────────────
print("\n[5] Building composite risk scores...")

def normalize(value, min_val, max_val):
    """Normalize a value to 0-100 scale"""
    if max_val == min_val:
        return 0
    return min(100, max(0, (value - min_val) / (max_val - min_val) * 100))

# Get min/max for normalization
flag_rates    = [float(row[3]) for row in frequency_data]
zscores       = list(zscore_data.values())
val_rates     = list(validation_data.values())
recency_rates = list(recency_data.values())

min_fr, max_fr = min(flag_rates), max(flag_rates)
min_zs, max_zs = (min(zscores), max(zscores)) if zscores else (0, 1)
min_vr, max_vr = min(val_rates), max(val_rates)
min_rr, max_rr = min(recency_rates), max(recency_rates)

# Weights
W_FREQUENCY  = 0.30
W_MAGNITUDE  = 0.30
W_VALIDATION = 0.20
W_RECENCY    = 0.20

scores = []
for row in frequency_data:
    vendor_id  = row[0]
    flag_rate  = float(row[3])

    freq_score = normalize(flag_rate, min_fr, max_fr)
    mag_score  = normalize(zscore_data.get(vendor_id, 0), min_zs, max_zs)
    val_score  = normalize(validation_data.get(vendor_id, 0), min_vr, max_vr)
    rec_score  = normalize(recency_data.get(vendor_id, 0), min_rr, max_rr)

    composite = (
        freq_score  * W_FREQUENCY +
        mag_score   * W_MAGNITUDE +
        val_score   * W_VALIDATION +
        rec_score   * W_RECENCY
    )

    risk_tier = (
    'HIGH'   if composite >= 35 else
    'MEDIUM' if composite >= 20 else
    'LOW'
)

    scores.append((
        vendor_id,
        round(freq_score, 2),
        round(mag_score, 2),
        round(val_score, 2),
        round(rec_score, 2),
        round(composite, 2),
        risk_tier
    ))

# ── Insert into vendor_risk_scores ───────────────────
cur.execute("DELETE FROM vendor_risk_scores")  # clear previous runs

cur.executemany("""
    INSERT INTO vendor_risk_scores
        (vendor_id, anomaly_frequency, deviation_magnitude,
        validation_failures, recency_score, composite_score, risk_tier)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""", scores)

conn.commit()

# ── Summary ───────────────────────────────────────────
print("\n" + "=" * 50)
print("LAYER 3 SUMMARY")
print("=" * 50)

cur.execute("""
    SELECT risk_tier, COUNT(*), ROUND(AVG(composite_score), 2)
    FROM vendor_risk_scores
    GROUP BY risk_tier
    ORDER BY AVG(composite_score) DESC
""")
print("\n  Risk tier distribution:")
for row in cur.fetchall():
    print(f"    {row[0]:<10} {row[1]:>4} vendors  |  avg score: {row[2]}")

print("\n  Top 10 highest risk vendors:")
cur.execute("""
    SELECT vr.vendor_id, v.vendor_name, v.category,
            vr.composite_score, vr.risk_tier
    FROM vendor_risk_scores vr
    JOIN vendors v ON vr.vendor_id = v.vendor_id
    WHERE vr.risk_tier = 'HIGH'
    ORDER BY vr.composite_score DESC
    LIMIT 10
""")
print(f"\n  {'ID':<6} {'Vendor':<35} {'Category':<20} {'Score':<8} Tier")
print(f"  {'-'*6} {'-'*35} {'-'*20} {'-'*8} {'-'*6}")
for row in cur.fetchall():
    print(f"  {row[0]:<6} {row[1]:<35} {row[2]:<20} {row[3]:<8} {row[4]}")

cur.close()
conn.close()
print("\n✅ Layer 3 complete. All vendors scored and tiered.")