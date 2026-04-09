import psycopg2
import random
import re
from faker import Faker
from datetime import datetime, timedelta
import pandas as pd

fake = Faker('en_IN')

# ── DB Connection ────────────────────────────────────────────────
conn = psycopg2.connect(
    dbname="gst_analytics",
    user="postgres",
    password="Saksham@3124",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# ── Master Data ──────────────────────────────────────────────────
STATE_CODES = ['07', '09', '19', '27', '29', '33', '36', '24', '08', '06']

GST_CATEGORIES = [
    ('Electronics',     18.0, 'Electronic goods and components'),
    ('Pharmaceuticals', 12.0, 'Medicines and medical supplies'),
    ('Textiles',         5.0, 'Fabric and clothing'),
    ('Automobiles',     28.0, 'Vehicles and auto parts'),
    ('Food & Beverages', 5.0, 'Packaged food and drinks'),
    ('Construction',    18.0, 'Building materials'),
    ('IT Services',     18.0, 'Software and IT consulting'),
    ('Logistics',        5.0, 'Transport and freight'),
    ('Chemicals',       18.0, 'Industrial chemicals'),
    ('Agriculture',      0.0, 'Agricultural produce'),
]

# ── Helper Functions ─────────────────────────────────────────────
def generate_gstin(state_code):
    """Generate realistic GSTIN: 2-digit state + 10 PAN chars + 1Z + 1 checksum"""
    pan = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5)) + \
            ''.join(random.choices('0123456789', k=4)) + \
            random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    return f"{state_code}{pan}1Z5"

def generate_invoice_id(index):
    year = random.randint(2022, 2024)
    return f"INV-{year}-{str(index).zfill(6)}"

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# ── Step 1: Insert GST Categories ───────────────────────────────
print("Inserting GST categories...")
category_ids = []
for cat in GST_CATEGORIES:
    cur.execute("""
        INSERT INTO gst_categories (category_name, gst_rate, description)
        VALUES (%s, %s, %s) RETURNING category_id
    """, cat)
    category_ids.append(cur.fetchone()[0])
conn.commit()

# ── Step 2: Insert Vendors ───────────────────────────────────────
print("Inserting vendors...")
vendor_ids = []
vendor_state_map = {}  # vendor_id -> state_code (for GSTIN mismatch injection)

for i in range(210):
    state_code = random.choice(STATE_CODES)
    gstin = generate_gstin(state_code)
    category = random.choice(GST_CATEGORIES)[0]
    reg_date = random_date(datetime(2018, 1, 1), datetime(2022, 1, 1))

    cur.execute("""
        INSERT INTO vendors (vendor_name, gstin, category, state_code, registration_date)
        VALUES (%s, %s, %s, %s, %s) RETURNING vendor_id
    """, (fake.company(), gstin, category, state_code, reg_date))

    vid = cur.fetchone()[0]
    vendor_ids.append(vid)
    vendor_state_map[vid] = state_code

conn.commit()
print(f"  {len(vendor_ids)} vendors inserted.")

# ── Step 3: Simulate Invoices ────────────────────────────────────
print("Simulating invoices...")

start_date = datetime(2022, 1, 1)
end_date   = datetime(2024, 12, 31)

# Per-vendor baseline amounts (realistic variation)
vendor_baselines = {
    vid: random.uniform(50_000, 500_000)
    for vid in vendor_ids
}

invoices        = []
duplicate_pool  = []   # holds real invoices to duplicate later
invoice_index   = 1

TARGET = 50_000
anomaly_counts  = {'duplicate': 0, 'gstin_mismatch': 0, 'spike': 0}

# -- 3a: Generate clean invoices (88%) ───────────────────────────
clean_target = int(TARGET * 0.88)
for _ in range(clean_target):
    vid        = random.choice(vendor_ids)
    cat_id     = random.choice(category_ids)
    baseline   = vendor_baselines[vid]
    amount     = round(random.gauss(baseline, baseline * 0.15), 2)
    amount     = max(1000, amount)
    gst_rate   = random.choice(GST_CATEGORIES)[1] / 100
    tax        = round(amount * gst_rate, 2)
    inv_date   = random_date(start_date, end_date)
    state      = vendor_state_map[vid]

    inv = (
        generate_invoice_id(invoice_index),
        vid, cat_id, inv_date.date(),
        amount, tax, state, 'PENDING'
    )
    invoices.append(inv)
    duplicate_pool.append(inv)
    invoice_index += 1

# -- 3b: Inject duplicates (~3%) ─────────────────────────────────
dup_target = int(TARGET * 0.03)
for _ in range(dup_target):
    original = random.choice(duplicate_pool)
    dup = (
        generate_invoice_id(invoice_index),  # new ID
        original[1], original[2],            # same vendor + category
        original[3],                         # same date
        original[4], original[5],            # same amount + tax
        original[6], 'PENDING'
    )
    invoices.append(dup)
    anomaly_counts['duplicate'] += 1
    invoice_index += 1

# -- 3c: Inject GSTIN mismatches (~3%) ───────────────────────────
gstin_target = int(TARGET * 0.03)
for _ in range(gstin_target):
    vid      = random.choice(vendor_ids)
    cat_id   = random.choice(category_ids)
    baseline = vendor_baselines[vid]
    amount   = round(random.gauss(baseline, baseline * 0.15), 2)
    amount   = max(1000, amount)
    tax      = round(amount * 0.18, 2)
    inv_date = random_date(start_date, end_date)

    # Use a DIFFERENT state code than vendor's registered state
    wrong_state = random.choice([s for s in STATE_CODES if s != vendor_state_map[vid]])

    inv = (
        generate_invoice_id(invoice_index),
        vid, cat_id, inv_date.date(),
        amount, tax, wrong_state, 'PENDING'
    )
    invoices.append(inv)
    anomaly_counts['gstin_mismatch'] += 1
    invoice_index += 1

# -- 3d: Inject statistical spikes (~6%) ─────────────────────────
spike_target = int(TARGET * 0.06)
spike_vendors = random.sample(vendor_ids, min(30, len(vendor_ids)))

for _ in range(spike_target):
    vid      = random.choice(spike_vendors)
    cat_id   = random.choice(category_ids)
    baseline = vendor_baselines[vid]

    # Amount is 4x–8x the vendor baseline — clear statistical outlier
    amount   = round(baseline * random.uniform(4, 8), 2)
    tax      = round(amount * 0.18, 2)
    inv_date = random_date(start_date, end_date)
    state    = vendor_state_map[vid]

    inv = (
        generate_invoice_id(invoice_index),
        vid, cat_id, inv_date.date(),
        amount, tax, state, 'PENDING'
    )
    invoices.append(inv)
    anomaly_counts['spike'] += 1
    invoice_index += 1

# ── Step 4: Bulk Insert Invoices ─────────────────────────────────
print(f"  Inserting {len(invoices):,} invoices...")
cur.executemany("""
    INSERT INTO invoices
        (invoice_id, vendor_id, category_id, invoice_date,
        amount, tax_claimed, state_code, validation_status)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (invoice_id) DO NOTHING
""", invoices)
conn.commit()

# ────────────────────────────────────────────────────────
print("\n✅ Simulation complete:")
print(f"   Total invoices  : {len(invoices):,}")
print(f"   Vendors         : {len(vendor_ids)}")
print(f"   Duplicates      : {anomaly_counts['duplicate']:,}")
print(f"   GSTIN mismatches: {anomaly_counts['gstin_mismatch']:,}")
print(f"   Spike anomalies : {anomaly_counts['spike']:,}")

cur.close()
conn.close()