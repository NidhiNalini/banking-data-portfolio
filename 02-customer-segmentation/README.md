# Customer Segmentation Pipeline — RFM Analysis

A banking-style customer segmentation pipeline that reads clean transaction records from the Week 5 ETL output, computes RFM scores per account, assigns business segments, and loads the results into a relational database with full validation and audit logging.

This project is the second stage of a connected banking data portfolio — the clean data produced by the transaction ETL pipeline flows directly into this segmentation system.

---

## What This Pipeline Does

Banks do not treat all customers the same. They segment them by behaviour so they can protect high-value relationships, re-engage disengaged customers, and allocate resources where they will have the most impact. This pipeline automates that process using RFM analysis — a framework used across retail banking, credit card operations, and wealth management.

---

## RFM Framework

| Dimension | Definition | Scoring |
|---|---|---|
| Recency | Days since last completed transaction | Lower days = score 3 (most recent = best) |
| Frequency | Number of completed transactions | Higher count = score 3 |
| Monetary | Average absolute transaction amount | Higher spend = score 3 |

Each dimension is scored 1–3 using quantile-based tertiles. The RFM total (sum of three scores) ranges from 3 to 9.

---

## Customer Segments

| Segment | Criteria | Bank Action |
|---|---|---|
| HIGH_VALUE | rfm_total ≥ 8 | Retain — premium products, loyalty rewards |
| NEW_CUSTOMER | rfm_total ≥ 6 and r_score = 3 | Nurture — onboarding offers, low-fee products |
| AT_RISK | rfm_total ≥ 4 | Re-engage — personalised outreach, rate offers |
| DORMANT | rfm_total = 3 | Win-back campaign or accept churn |

---

## Results on Sample Dataset

| Metric | Value |
|---|---|
| Accounts scored | 50 |
| Completed transactions analysed | 288 |
| HIGH_VALUE customers | 8 |
| NEW_CUSTOMER | 12 |
| AT_RISK | 26 |
| DORMANT | 4 |
| Top RFM score achieved | 9 (ACC-1006, ACC-1029) |

26 of 50 accounts are classified AT_RISK — the largest segment and the highest-priority target for re-engagement campaigns.

---

## Connection to Week 5

This pipeline reads directly from the SQLite database produced by the transaction ETL pipeline:

```
transactions_clean.db  →  segmentation pipeline  →  segmentation.db
(490 clean records)        (RFM scoring + joins)     (rfm_scores + customers)
```

The two projects form a connected data system — raw transactions flow through cleaning and validation, then into behavioural scoring and segmentation.

---

## Schema Design

Two tables with enforced constraints and referential integrity:

**customers** — account profiles with CHECK constraints on age range and account type

**rfm_scores** — one row per account, with:
- FOREIGN KEY referencing customers — prevents orphaned scores
- CHECK constraints on r_score, f_score, m_score (1–3 only)
- CHECK constraint on rfm_total (3–9 only)
- CHECK constraint on segment (four allowed values only)

---

## Validation and Audit Trail

Before loading — the pipeline reports:
- Segment distribution across all 50 accounts
- RFM total summary statistics
- Top 5 accounts by RFM score
- Count of dormant accounts

After loading — the pipeline queries the database directly to confirm:
- Segment-level averages and counts
- HIGH_VALUE customers joined with full profile data (name, province, account type)

All pipeline actions are logged to `segmentation.log` with timestamps.

---

## What Would Be Added in Production

- **Automated retraining trigger** — re-score all accounts weekly as new transactions arrive
- **Segment drift detection** — alert if the proportion of HIGH_VALUE customers drops more than 10% between scoring runs
- **Configurable thresholds** — segment boundaries and scoring rules loaded from a config file rather than hardcoded
- **Integration with CRM** — push segment assignments directly to the customer relationship management system to trigger outreach campaigns

---

## Project Structure

```
02-customer-segmentation/
├── data/
│   ├── customers.csv          ← synthetic customer profiles
│   └── segmentation.db        ← output SQLite database
├── schema.sql                 ← table definitions with constraints
├── generate_customers.py      ← synthetic data generator
├── segmentation_pipeline.py   ← main pipeline
├── segmentation_queries.sql   ← four business queries
├── segmentation.log           ← audit trail
└── README.md
```

---

## Tools

Python · Pandas · SQLite · NumPy · logging
