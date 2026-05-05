import pandas as pd
import logging
from datetime import datetime
import sqlite3

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("banking_etl")


def extract(filepath: str) -> pd.DataFrame:
    log.info(f"EXTRACT | Reading source: {filepath}")

    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        log.error(f"EXTRACT FAILED | File not found: {filepath}")
        raise

    if df.empty:
        log.error("EXTRACT FAILED | File loaded but is empty")
        raise ValueError("Empty source file")

    log.info(f"EXTRACT SUCCESS | {len(df)} rows | {list(df.columns)}")
    return df


# ---------------------------------------------------------
# TODO SECTION — Completed
# ---------------------------------------------------------

def run_extract_check(df: pd.DataFrame) -> None:
    print("\n=== EXTRACT PROFILE ===")

    # 1. Shape
    print("Shape:", df.shape)

    # 2. Data types
    print("\nData Types:")
    print(df.dtypes)

    # 3. Null counts
    print("\nNull Counts:")
    print(df.isnull().sum())

    # 4. Unique transaction types
    print("\nUnique transaction types:")
    print(df["transaction_type"].unique())


# --- Entry point ---
if __name__ == "__main__":
    raw_df = extract("data/transactions_raw.csv")
    run_extract_check(raw_df)


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    log.info(f"TRANSFORM | Starting with {len(df)} rows")
    rejected = []

    # Step 1: Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["transaction_id"])
    dupes_removed = before - len(df)
    log.info(f"TRANSFORM | Removed {dupes_removed} duplicate transaction_ids")

    # Step 2: Standardise transaction_type
    df["transaction_type"] = df["transaction_type"].str.upper().str.strip()

    valid_types = {"DEBIT", "CREDIT", "TRANSFER", "FEE"}
    invalid_type_mask = ~df["transaction_type"].isin(valid_types)
    if invalid_type_mask.any():
        bad = df[invalid_type_mask].copy()
        bad["rejection_reason"] = "invalid_transaction_type"
        rejected.append(bad)
        df = df[~invalid_type_mask]
        log.warning(f"TRANSFORM | Rejected {len(bad)} rows: invalid transaction type")

    # Step 3: Parse dates
    def parse_date(date_str):
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"]:
            try:
                return pd.to_datetime(date_str, format=fmt)
            except:
                continue
        return pd.NaT

    df["transaction_date"] = df["transaction_date"].apply(parse_date)
    bad_dates = df[df["transaction_date"].isna()].copy()
    if len(bad_dates) > 0:
        bad_dates["rejection_reason"] = "unparseable_date"
        rejected.append(bad_dates)
        df = df.dropna(subset=["transaction_date"])
        log.warning(f"TRANSFORM | Rejected {len(bad_dates)} rows: unparseable date")

    # Step 4: Null amounts
    null_amounts = df[df["amount"].isna()].copy()
    if len(null_amounts) > 0:
        null_amounts["rejection_reason"] = "null_amount"
        rejected.append(null_amounts)
        df = df.dropna(subset=["amount"])
        log.warning(f"TRANSFORM | Rejected {len(null_amounts)} rows: null amount")

    # Step 5: Flag large transactions
    df["is_flagged"] = (df["amount"].abs() >= 5000).astype(int)
    flagged_count = df["is_flagged"].sum()
    log.info(f"TRANSFORM | Flagged {flagged_count} large transactions (>=5000 CAD)")

    rejected_df = pd.concat(rejected, ignore_index=True) if rejected else pd.DataFrame()

    log.info(f"TRANSFORM SUCCESS | Clean: {len(df)} | Rejected: {len(rejected_df)}")
    return df, rejected_df


# ---------------------------------------------------------
# TODO SECTION — Completed
# ---------------------------------------------------------

def run_transform_check(clean_df: pd.DataFrame, rejected_df: pd.DataFrame) -> None:
    print("\n=== TRANSFORM QUALITY REPORT ===")

    # 5. Clean vs rejected
    print(f"Clean rows: {len(clean_df)}")
    print(f"Rejected rows: {len(rejected_df)}")

    # 6. Rejection reasons
    if len(rejected_df) > 0:
        print("\nRejection reasons:")
        print(rejected_df.groupby("rejection_reason").size())

    # 7. Flagged transactions
    print("\nFlagged transactions (is_flagged == 1):")
    print(clean_df["is_flagged"].value_counts())

    # 8. Date range
    print("\nDate range of clean transactions:")
    print("Min date:", clean_df["transaction_date"].min())
    print("Max date:", clean_df["transaction_date"].max())


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id    TEXT PRIMARY KEY,
            account_id        TEXT NOT NULL,
            transaction_date  DATE NOT NULL,
            amount            REAL NOT NULL,
            transaction_type  TEXT NOT NULL
                CHECK(transaction_type IN ('DEBIT','CREDIT','TRANSFER','FEE')),
            merchant_category TEXT,
            status            TEXT NOT NULL
                CHECK(status IN ('COMPLETED','PENDING','FAILED','REVERSED')),
            is_flagged        INTEGER DEFAULT 0
                CHECK(is_flagged IN (0, 1)),
            loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    log.info("LOAD | Schema verified / created")


def load(df: pd.DataFrame, db_path: str) -> int:
    log.info(f"LOAD | Connecting to: {db_path}")
    conn = sqlite3.connect(db_path)
    create_schema(conn)

    df = df.copy()
    df["transaction_date"] = df["transaction_date"].dt.strftime("%Y-%m-%d")

    before_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

    df.to_sql("transactions", conn, if_exists="append", index=False, method="multi")

    after_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    rows_inserted = after_count - before_count

    conn.close()
    log.info(f"LOAD SUCCESS | Inserted {rows_inserted} new rows | Total: {after_count}")
    return rows_inserted


# ---------------------------------------------------------
# TODO SECTION — Completed
# ---------------------------------------------------------

def run_post_load_check(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    print("\n=== POST-LOAD VERIFICATION ===")

    # 9. Total rows
    total = pd.read_sql("SELECT COUNT(*) AS total FROM transactions", conn)
    print("\nTotal rows in DB:")
    print(total)

    # 10. Transactions by type
    tx_by_type = pd.read_sql("""
        SELECT transaction_type, COUNT(*) AS count
        FROM transactions
        GROUP BY transaction_type
    """, conn)
    print("\nTransactions by type:")
    print(tx_by_type)

    # 11. Flagged transactions
    flagged = pd.read_sql("""
        SELECT transaction_id, account_id, amount, transaction_date
        FROM transactions
        WHERE is_flagged = 1
    """, conn)
    print("\nFlagged transactions:")
    print(flagged)

    conn.close()


if __name__ == "__main__":
    log.info("=" * 50)
    log.info("PIPELINE START")
    log.info("=" * 50)

    # Stage 1 — Extract
    raw_df = extract("data/transactions_raw.csv")
    run_extract_check(raw_df)

    # Stage 2 — Transform
    clean_df, rejected_df = transform(raw_df)
    run_transform_check(clean_df, rejected_df)

    # Save rejected records — audit trail
    if len(rejected_df) > 0:
        rejected_df.to_csv("data/rejected_transactions.csv", index=False)
        log.info(f"AUDIT | Rejected records saved to data/rejected_transactions.csv")

    # Stage 3 — Load
    rows_inserted = load(clean_df, "data/transactions_clean.db")

    # Post-load verification
    run_post_load_check("data/transactions_clean.db")

    log.info("=" * 50)
    log.info(f"PIPELINE COMPLETE | {rows_inserted} rows loaded")
    log.info("=" * 50)