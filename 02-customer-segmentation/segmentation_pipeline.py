import pandas as pd
import sqlite3
import logging
import numpy as np
from datetime import date
from pathlib import Path

# -------------------------------------------------------------------
# LOGGING CONFIG
# -------------------------------------------------------------------
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("segmentation.log"),
            logging.StreamHandler()
        ]
    )

log = logging.getLogger("segmentation")

# -------------------------------------------------------------------
# CONSTANTS
# -------------------------------------------------------------------
TRANSACTIONS_DB = "../01-transaction-etl/data/transactions_clean.db"
SEGMENTATION_DB = "data/segmentation.db"
CUSTOMERS_CSV   = "data/customers.csv"
REFERENCE_DATE  = date(2024, 6, 30)

# -------------------------------------------------------------------
# EXTRACT
# -------------------------------------------------------------------
def load_transactions():
    log.info(f"EXTRACT | Reading: {TRANSACTIONS_DB}")
    conn = sqlite3.connect(TRANSACTIONS_DB)

    df = pd.read_sql("""
        SELECT account_id, transaction_date, amount
        FROM transactions
        WHERE status = 'COMPLETED'
    """, conn)

    conn.close()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    log.info(f"EXTRACT SUCCESS | {len(df)} completed transactions")
    return df

# -------------------------------------------------------------------
# TRANSFORM
# -------------------------------------------------------------------
def compute_rfm(df):
    log.info("TRANSFORM | Computing RFM")

    rfm = df.groupby("account_id").agg(
        last_txn=("transaction_date", "max"),
        frequency=("transaction_date", "count"),
        monetary_avg=("amount", lambda x: x.abs().mean())
    ).reset_index()

    rfm["recency_days"] = (
        pd.Timestamp(REFERENCE_DATE) - rfm["last_txn"]
    ).dt.days

    rfm.drop(columns=["last_txn"], inplace=True)

    log.info(f"TRANSFORM SUCCESS | RFM computed for {len(rfm)} accounts")
    return rfm


def score_rfm(rfm):
    log.info("TRANSFORM | Scoring RFM")

    rfm["r_score"] = pd.qcut(
        rfm["recency_days"],
        q=3,
        labels=[3, 2, 1]
    ).astype("Int64")

    rfm["f_score"] = pd.qcut(
        rfm["frequency"],
        q=3,
        labels=[1, 2, 3],
        duplicates="drop"
    ).astype("Int64")

    rfm["m_score"] = pd.qcut(
        rfm["monetary_avg"],
        q=3,
        labels=[1, 2, 3],
        duplicates="drop"
    ).astype("Int64")

    rfm[["f_score", "m_score"]] = rfm[["f_score", "m_score"]].fillna(1)

    rfm["rfm_total"] = (
        rfm["r_score"] + rfm["f_score"] + rfm["m_score"]
    )

    return rfm


def assign_segment(row):
    if row["rfm_total"] >= 8:
        return "HIGH_VALUE"
    elif row["rfm_total"] >= 6 and row["r_score"] == 3:
        return "NEW_CUSTOMER"
    elif row["rfm_total"] >= 4:
        return "AT_RISK"
    else:
        return "DORMANT"

# -------------------------------------------------------------------
# LOAD
# -------------------------------------------------------------------
def create_schema(conn):
    schema_path = Path("schema.sql")
    if not schema_path.exists():
        raise FileNotFoundError("schema.sql file not found")

    with open(schema_path) as f:
        conn.executescript(f.read())

    conn.commit()
    log.info("LOAD | Schema created successfully")


def load_customers(conn):
    pd.read_csv(CUSTOMERS_CSV).to_sql(
        "customers", conn, if_exists="replace", index=False
    )
    log.info("LOAD | Customers loaded")


def load_rfm_scores(rfm, conn):
    cols = [
        "account_id", "recency_days", "frequency", "monetary_avg",
        "r_score", "f_score", "m_score", "rfm_total", "segment"
    ]
    rfm[cols].to_sql(
        "rfm_scores", conn, if_exists="replace", index=False
    )
    log.info(f"LOAD | {len(rfm)} RFM records loaded")

# -------------------------------------------------------------------
# QUALITY CHECKS
# -------------------------------------------------------------------
def run_rfm_check(rfm):
    print("\n=== RFM QUALITY REPORT ===")

    print("\nSegment distribution:")
    print(rfm["segment"].value_counts())

    print("\nRFM total summary:")
    print(rfm["rfm_total"].describe())

    print("\nTop 5 accounts by RFM score:")
    print(
        rfm.sort_values("rfm_total", ascending=False)
        [["account_id", "rfm_total", "segment"]]
        .head(5)
    )

    dormant_count = (rfm["segment"] == "DORMANT").sum()
    print(f"\nDormant accounts: {dormant_count}")


def run_post_load_check(conn):
    print("\n=== POST-LOAD VERIFICATION ===")

    seg_summary = pd.read_sql("""
        SELECT
            segment,
            AVG(rfm_total) AS avg_score,
            COUNT(*)       AS count
        FROM rfm_scores
        GROUP BY segment
        ORDER BY avg_score DESC
    """, conn)

    print("\nSegment summary:")
    print(seg_summary)

    high_value = pd.read_sql("""
        SELECT
            r.account_id,
            c.full_name,
            c.province,
            c.account_type,
            r.rfm_total,
            r.segment
        FROM rfm_scores r
        JOIN customers c
          ON r.account_id = c.account_id
        WHERE r.segment = 'HIGH_VALUE'
        ORDER BY r.rfm_total DESC
    """, conn)

    print("\nHigh-value customers:")
    print(high_value)

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
if __name__ == "__main__":
    log.info("=" * 50 + " SEGMENTATION START")

    transactions = load_transactions()
    rfm = compute_rfm(transactions)
    rfm = score_rfm(rfm)
    rfm["segment"] = rfm.apply(assign_segment, axis=1)

    run_rfm_check(rfm)

    conn = sqlite3.connect(SEGMENTATION_DB)
    create_schema(conn)
    load_customers(conn)
    load_rfm_scores(rfm, conn)
    run_post_load_check(conn)
    conn.close()

    log.info("SEGMENTATION COMPLETE")
