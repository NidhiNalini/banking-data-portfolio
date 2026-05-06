
CREATE TABLE IF NOT EXISTS customers (
    account_id      TEXT PRIMARY KEY,
    full_name       TEXT NOT NULL,
    age             INTEGER NOT NULL CHECK(age BETWEEN 18 AND 100),
    province        TEXT NOT NULL,
    customer_since  DATE NOT NULL,
    account_type    TEXT NOT NULL CHECK(account_type IN (
                        'CHEQUING','SAVINGS','BUSINESS'))
);

CREATE TABLE IF NOT EXISTS rfm_scores (
    account_id    TEXT PRIMARY KEY REFERENCES customers(account_id),
    recency_days  INTEGER NOT NULL,
    frequency     INTEGER NOT NULL,
    monetary_avg  REAL NOT NULL,
    r_score       INTEGER NOT NULL CHECK(r_score BETWEEN 1 AND 3),
    f_score       INTEGER NOT NULL CHECK(f_score BETWEEN 1 AND 3),
    m_score       INTEGER NOT NULL CHECK(m_score BETWEEN 1 AND 3),
    rfm_total     INTEGER NOT NULL CHECK(rfm_total BETWEEN 3 AND 9),
    segment       TEXT NOT NULL CHECK(segment IN (
                      'HIGH_VALUE','NEW_CUSTOMER','AT_RISK','DORMANT')),
    scored_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);