# Banking Transaction ETL Pipeline

A complete banking-style ETL pipeline that processes raw transaction data from a CSV source, validates and cleans it against financial industry rules, and loads the results into a SQLite database with a full audit trail.

---

## What This Pipeline Does

Raw transaction data arrives messy — duplicate records, null amounts, inconsistent formats, and missing validations. This pipeline takes that raw CSV, applies structured quality rules at every stage, and produces a clean, queryable database with documented evidence of every decision made along the way.

---

## Pipeline Stages

### Extract
Reads the source CSV and immediately profiles it — logging row counts, column data types, null counts, and unique transaction type values. Problems are visible before a single transformation runs.

### Transform
Applies real financial-industry validation rules in sequence:

- **Deduplication** — removes rows with duplicate `transaction_id` values
- **Standardisation** — normalises `transaction_type` to uppercase and validates against allowed categories (`DEBIT`, `CREDIT`, `TRANSFER`, `FEE`)
- **Date parsing** — handles multiple incoming date formats (`YYYY-MM-DD`, `DD/MM/YYYY`, `MM/DD/YYYY`) and rejects unparseable values
- **Null rejection** — records with missing `amount` values are captured and removed from the clean dataset
- **FINTRAC flagging** — transactions with an absolute value of $5,000 CAD or above are flagged via `is_flagged = 1`, consistent with Canadian large transaction reporting expectations

All rejected records are written to `data/rejected_transactions.csv` with an explicit `rejection_reason` column — nothing is dropped silently.

### Load
Clean records are inserted into a SQLite database with a schema that enforces data integrity through:

- `PRIMARY KEY` on `transaction_id` — prevents duplicates at the database level
- `CHECK` constraints on `transaction_type` and `status` — rejects invalid values before they enter the table
- `loaded_at` timestamp — records exactly when each row entered the system

Loading is **idempotent** — running the pipeline twice produces the same result as running it once. No duplicate records are created on re-run.

### Post-Load Verification
After loading, the pipeline queries the database directly to confirm:
- Total row count matches expectations
- Transaction type distribution is consistent
- All flagged transactions are identifiable and reviewable

---

## Results on Sample Dataset

| Stage | Count |
|---|---|
| Extracted | 505 rows |
| Duplicates removed | 5 |
| Null amounts rejected | 10 |
| Clean rows loaded | 490 |
| Large transactions flagged | 5 |

---

## Audit Trail

Every pipeline run produces two files:

- `pipeline.log` — timestamped record of every action, warning, and result
- `data/rejected_transactions.csv` — all rejected records with rejection reasons

---

## What Would Be Added in Production

- **Volume alerting** — trigger an alert if record counts drop more than 5% from the previous run
- **Schema drift detection** — fail loudly if the source CSV changes column names or types
- **Scheduled execution** via cron or Airflow
- **Parameterised thresholds** — FINTRAC flag amount and rejection rules loaded from config, not hardcoded

---

## Tools

Python · Pandas · SQLite · logging · datetime

---

## Relevance to Banking

Banks process millions of transactions daily under strict regulatory requirements. Pipelines must be auditable, idempotent, and fail loudly rather than silently. This project applies those same principles at small scale — the logic is identical to what production data engineering teams at institutions like RBC and TD implement on larger infrastructure.
