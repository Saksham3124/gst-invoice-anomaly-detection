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
print("LAYER 2 — STATISTICAL ANOMALY DETECTION")
print("=" * 50)

# ── Analysis 2a: Vendor Baseline Deviation ───────────────────────
print("\n[2a] Detecting vendor baseline deviations (Z-score)...")

cur.execute("""
    WITH vendor_stats AS (
        SELECT
            vendor_id,
            AVG(amount)    OVER (PARTITION BY vendor_id) AS baseline,
            STDDEV(amount) OVER (PARTITION BY vendor_id) AS std_dev,
            amount,
            invoice_id
        FROM invoices
        WHERE validation_status = 'CLEAN'
    ),
    zscore_calc AS (
        SELECT
            invoice_id,
            vendor_id,
            amount,
            baseline,
            std_dev,
            CASE
                WHEN std_dev > 0
                THEN (amount - baseline) / std_dev
                ELSE 0
            END AS z_score
        FROM vendor_stats
    )
    SELECT invoice_id, vendor_id, amount, baseline, std_dev, z_score
    FROM zscore_calc
    WHERE z_score > 2
    ORDER BY z_score DESC
""")
zscore_flags = cur.fetchall()
print(f"    Invoices exceeding 2σ from vendor baseline: {len(zscore_flags)}")

# Insert into flags
for row in zscore_flags:
    invoice_id, vendor_id, amount, baseline, std_dev, z_score = row
    cur.execute("""
        INSERT INTO flags (invoice_id, vendor_id, flag_type, severity, details)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        invoice_id,
        vendor_id,
        'STATISTICAL_ZSCORE',
        'HIGH' if z_score > 3 else 'MEDIUM',
        f'Z-score: {z_score:.2f} | Amount: {amount:.2f} | Baseline: {baseline:.2f} | StdDev: {std_dev:.2f}'
    ))

conn.commit()
print(f"    Flagged in flags table.")

# ── Analysis 2b: Rolling Average Spike Detection ─────────────────
print("\n[2b] Detecting rolling average spikes...")

cur.execute("""
    WITH rolling AS (
        SELECT
            invoice_id,
            vendor_id,
            amount,
            invoice_date,
            AVG(amount) OVER (
                PARTITION BY vendor_id
                ORDER BY invoice_date
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS rolling_avg
        FROM invoices
        WHERE validation_status = 'CLEAN'
    )
    SELECT invoice_id, vendor_id, amount, rolling_avg,
            amount / NULLIF(rolling_avg, 0) AS spike_ratio
    FROM rolling
    WHERE rolling_avg IS NOT NULL
      AND amount > rolling_avg * 3
    ORDER BY spike_ratio DESC
""")
rolling_flags = cur.fetchall()
print(f"    Invoices spiking >3x rolling average: {len(rolling_flags)}")

for row in rolling_flags:
    invoice_id, vendor_id, amount, rolling_avg, spike_ratio = row
    cur.execute("""
        INSERT INTO flags (invoice_id, vendor_id, flag_type, severity, details)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        invoice_id,
        vendor_id,
        'ROLLING_SPIKE',
        'HIGH',
        f'Spike ratio: {spike_ratio:.2f}x | Amount: {amount:.2f} | Rolling avg: {rolling_avg:.2f}'
    ))

conn.commit()
print(f"    Flagged in flags table.")

# ── Analysis 2c: IQR-Based Category Outliers ─────────────────────
print("\n[2c] Detecting category-level IQR outliers...")

cur.execute("""
    WITH category_iqr AS (
        SELECT
            category_id,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) AS q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) AS q3
        FROM invoices
        WHERE validation_status = 'CLEAN'
        GROUP BY category_id
    ),
    iqr_calc AS (
        SELECT
            i.invoice_id,
            i.vendor_id,
            i.amount,
            i.category_id,
            c.q1,
            c.q3,
            (c.q3 - c.q1) AS iqr,
            c.q3 + 1.5 * (c.q3 - c.q1) AS upper_fence
        FROM invoices i
        JOIN category_iqr c ON i.category_id = c.category_id
        WHERE i.validation_status = 'CLEAN'
    )
    SELECT invoice_id, vendor_id, amount, category_id, upper_fence, iqr
    FROM iqr_calc
    WHERE amount > upper_fence
    ORDER BY amount DESC
""")
iqr_flags = cur.fetchall()
print(f"    Invoices exceeding category IQR upper fence: {len(iqr_flags)}")

for row in iqr_flags:
    invoice_id, vendor_id, amount, category_id, upper_fence, iqr = row
    cur.execute("""
        INSERT INTO flags (invoice_id, vendor_id, flag_type, severity, details)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (
        invoice_id,
        vendor_id,
        'IQR_OUTLIER',
        'MEDIUM',
        f'Amount: {amount:.2f} | Upper fence: {upper_fence:.2f} | IQR: {iqr:.2f}'
    ))

conn.commit()
print(f"    Flagged in flags table.")

# ── Update validation status for newly flagged ────────────────────
cur.execute("""
    UPDATE invoices
    SET validation_status = 'FLAGGED'
    WHERE invoice_id IN (
        SELECT DISTINCT invoice_id FROM flags
    )
    AND validation_status = 'CLEAN'
""")
conn.commit()

# ── Summary ───────────────────────────────────────────────────────
print("\n" + "=" * 50)
print("LAYER 2 SUMMARY")
print("=" * 50)

cur.execute("""
    SELECT flag_type, severity, COUNT(*)
    FROM flags
    GROUP BY flag_type, severity
    ORDER BY COUNT(*) DESC
""")
print("\n  All flags so far:")
for row in cur.fetchall():
    print(f"    {row[0]:<25} [{row[1]}]  {row[2]:,}")

cur.execute("SELECT COUNT(*) FROM invoices WHERE validation_status = 'FLAGGED'")
total_flagged = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM invoices")
total = cur.fetchone()[0]

print(f"\n  Total flagged invoices : {total_flagged:,} ({total_flagged/total*100:.1f}%)")

cur.close()
conn.close()
print("\n✅ Layer 2 complete.")