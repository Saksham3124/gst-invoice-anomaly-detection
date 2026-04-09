import psycopg2
import re

conn = psycopg2.connect(
    dbname="gst_analytics",
    user="postgres",
    password="Saksham@3124",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

print("=" * 50)
print("LAYER 1 — DATA VALIDATION & INTEGRITY CHECKS")
print("=" * 50)

# ── Check 1: Duplicate Invoices ──────────────────────
print("\n[1] Checking duplicate invoices...")

cur.execute("""
    SELECT vendor_id, amount, invoice_date, COUNT(*) as occurrences
    FROM invoices
    GROUP BY vendor_id, amount, invoice_date
    HAVING COUNT(*) > 1
""")
duplicates = cur.fetchall()
print(f"    Duplicate groups found: {len(duplicates)}")

# Flag them
cur.execute("""
    INSERT INTO flags (invoice_id, vendor_id, flag_type, severity, details)
    SELECT i.invoice_id, i.vendor_id,
            'DUPLICATE',
            'MEDIUM',
            'Same vendor, amount, and date as another invoice'
    FROM invoices i
    WHERE (i.vendor_id, i.amount, i.invoice_date) IN (
        SELECT vendor_id, amount, invoice_date
        FROM invoices
        GROUP BY vendor_id, amount, invoice_date
        HAVING COUNT(*) > 1
    )
    ON CONFLICT DO NOTHING
""")
print(f"    Flagged in flags table.")

# ── Check 2: GSTIN State Mismatch ────────────────────
print("\n[2] Checking GSTIN state mismatches...")

cur.execute("""
    SELECT i.invoice_id, i.vendor_id, v.state_code as vendor_state,
            i.state_code as invoice_state
    FROM invoices i
    JOIN vendors v ON i.vendor_id = v.vendor_id
    WHERE i.state_code != v.state_code
""")
mismatches = cur.fetchall()
print(f"    GSTIN state mismatches found: {len(mismatches)}")

# Flag them
cur.execute("""
    INSERT INTO flags (invoice_id, vendor_id, flag_type, severity, details)
    SELECT i.invoice_id, i.vendor_id,
            'GSTIN_MISMATCH',
            'HIGH',
            'Invoice state code does not match vendor registered state'
    FROM invoices i
    JOIN vendors v ON i.vendor_id = v.vendor_id
    WHERE i.state_code != v.state_code
    ON CONFLICT DO NOTHING
""")
print(f"    Flagged in flags table.")

# ── Check 3: Invalid Amounts ─────────────────────────
print("\n[3] Checking invalid amounts...")

cur.execute("""
    SELECT COUNT(*) FROM invoices
    WHERE amount <= 0 OR tax_claimed < 0
""")
invalid_amounts = cur.fetchone()[0]
print(f"    Invalid amount records: {invalid_amounts}")

if invalid_amounts > 0:
    cur.execute("""
        INSERT INTO flags (invoice_id, vendor_id, flag_type, severity, details)
        SELECT invoice_id, vendor_id,
                'INVALID_AMOUNT',
                'HIGH',
                'Amount <= 0 or negative tax claimed'
        FROM invoices
        WHERE amount <= 0 OR tax_claimed < 0
        ON CONFLICT DO NOTHING
    """)

# ── Check 4: Missing Critical Fields ─────────────────
print("\n[4] Checking missing critical fields...")

cur.execute("""
    SELECT COUNT(*) FROM invoices
    WHERE invoice_id IS NULL
        OR vendor_id IS NULL
        OR amount IS NULL
        OR invoice_date IS NULL
""")
missing = cur.fetchone()[0]
print(f"    Records with missing fields: {missing}")

# ── Check 5: Update Validation Status ────────────────
print("\n[5] Updating validation status on invoices...")

# Mark flagged invoices
cur.execute("""
    UPDATE invoices
    SET validation_status = 'FLAGGED'
    WHERE invoice_id IN (
        SELECT DISTINCT invoice_id FROM flags
    )
""")

# Mark rest as clean
cur.execute("""
    UPDATE invoices
    SET validation_status = 'CLEAN'
    WHERE validation_status = 'PENDING'
""")

conn.commit()

# ── Summary ───────────────────────────────────────────
print("\n" + "=" * 50)
print("VALIDATION SUMMARY")
print("=" * 50)

cur.execute("SELECT COUNT(*) FROM invoices WHERE validation_status = 'CLEAN'")
clean = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM invoices WHERE validation_status = 'FLAGGED'")
flagged = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM invoices")
total = cur.fetchone()[0]

print(f"  Total invoices : {total:,}")
print(f"  Clean          : {clean:,} ({clean/total*100:.1f}%)")
print(f"  Flagged        : {flagged:,} ({flagged/total*100:.1f}%)")

print("\n  Flags by type:")
cur.execute("""
    SELECT flag_type, severity, COUNT(*)
    FROM flags
    GROUP BY flag_type, severity
    ORDER BY COUNT(*) DESC
""")
for row in cur.fetchall():
    print(f"    {row[0]:<20} [{row[1]}]  {row[2]:,}")

cur.close()
conn.close()
print("\n✅ Layer 1 complete.")