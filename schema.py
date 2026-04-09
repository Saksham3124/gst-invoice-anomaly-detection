
schema_sql = """
-- Master vendor information
CREATE TABLE vendors (
    vendor_id       SERIAL PRIMARY KEY,
    vendor_name     VARCHAR(100) NOT NULL,
    gstin           VARCHAR(15) NOT NULL,
    category        VARCHAR(50) NOT NULL,
    state_code      VARCHAR(2) NOT NULL,
    registration_date DATE
);

-- GST category master
CREATE TABLE gst_categories (
    category_id     SERIAL PRIMARY KEY,
    category_name   VARCHAR(50) NOT NULL,
    gst_rate        DECIMAL(5,2) NOT NULL,
    description     TEXT
);

-- Core invoice records
CREATE TABLE invoices (
    invoice_id      VARCHAR(20) PRIMARY KEY,
    vendor_id       INT REFERENCES vendors(vendor_id),
    category_id     INT REFERENCES gst_categories(category_id),
    invoice_date    DATE NOT NULL,
    amount          DECIMAL(12,2) NOT NULL,
    tax_claimed     DECIMAL(12,2) NOT NULL,
    state_code      VARCHAR(2) NOT NULL,
    validation_status VARCHAR(20) DEFAULT 'PENDING'
);

-- Anomaly flags output table
CREATE TABLE flags (
    flag_id         SERIAL PRIMARY KEY,
    invoice_id      VARCHAR(20) REFERENCES invoices(invoice_id),
    vendor_id       INT REFERENCES vendors(vendor_id),
    flag_type       VARCHAR(50) NOT NULL,  -- DUPLICATE / GSTIN_MISMATCH / STATISTICAL / IQR_OUTLIER
    severity        VARCHAR(10) NOT NULL,  -- LOW / MEDIUM / HIGH
    flag_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details         TEXT
);

-- Vendor risk scores (populated after scoring model runs)
CREATE TABLE vendor_risk_scores (
    vendor_id           INT REFERENCES vendors(vendor_id),
    anomaly_frequency   DECIMAL(5,2),
    deviation_magnitude DECIMAL(10,4),
    validation_failures DECIMAL(5,2),
    recency_score       DECIMAL(5,2),
    composite_score     DECIMAL(5,2),
    risk_tier           VARCHAR(10),  -- LOW / MEDIUM / HIGH
    scored_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

"""