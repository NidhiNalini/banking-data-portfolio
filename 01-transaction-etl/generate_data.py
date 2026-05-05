import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# --- Configuration ---
NUM_RECORDS = 500
ACCOUNTS = [f"ACC-{i:04d}" for i in range(1001, 1051)]  # 50 accounts
TYPES = ["DEBIT", "CREDIT", "TRANSFER", "FEE"]
CATEGORIES = ["GROCERY", "DINING", "GAS", "ONLINE", "SALARY", "RENT", "ATM"]
STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED"]
START_DATE = datetime(2024, 1, 1)

# --- Generate clean records ---
records = []
for i in range(NUM_RECORDS):
    txn_date = START_DATE + timedelta(days=random.randint(0, 180))
    txn_type = random.choices(TYPES, weights=[60, 25, 10, 5])[0]

    # Amount logic: credits are positive, everything else negative
    if txn_type == "CREDIT":
        amount = round(random.uniform(50, 3000), 2)
    elif txn_type == "FEE":
        amount = round(random.uniform(-15, -2), 2)
    else:
        amount = round(random.uniform(-500, -5), 2)

    records.append({
        "transaction_id": f"TXN-{i+1:05d}",
        "account_id": random.choice(ACCOUNTS),
        "transaction_date": txn_date.strftime("%Y-%m-%d"),
        "amount": amount,
        "transaction_type": txn_type,
        "merchant_category": random.choice(CATEGORIES),
        "status": random.choice(STATUSES),
    })

df = pd.DataFrame(records)

# --- Inject realistic dirty data (10% of records) ---
dirty_indices = random.sample(range(NUM_RECORDS), 50)

for idx in dirty_indices[:10]:    # Missing amounts
    df.at[idx, "amount"] = None

for idx in dirty_indices[10:20]:  # Bad date formats
    df.at[idx, "transaction_date"] = "15/03/2024"  # wrong format

for idx in dirty_indices[20:30]:  # Lowercase type (inconsistent)
    df.at[idx, "transaction_type"] = df.at[idx, "transaction_type"].lower()

for idx in dirty_indices[30:35]:  # Suspicious large transactions
    df.at[idx, "amount"] = round(random.uniform(-9500, -9000), 2)

for idx in dirty_indices[35:40]:  # Duplicate transactions
    df = pd.concat([df, df.iloc[[idx]]], ignore_index=True)

# Save to CSV
df.to_csv("data/transactions_raw.csv", index=False)
print(f"Generated {len(df)} rows → data/transactions_raw.csv")
print(f"Dirty records injected: ~50 (nulls, bad dates, duplicates, large txns)")
