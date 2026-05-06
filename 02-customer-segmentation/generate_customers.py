import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)
ACCOUNTS = [f"ACC-{i:04d}" for i in range(1001, 1051)]
FIRST = ["James","Priya","Liam","Aisha","Noah","Sofia","Ethan","Mei","Lucas","Fatima"]
LAST  = ["Smith","Patel","Johnson","Chen","Williams","Kumar","Brown","Nguyen","Davis","Ali"]
PROVS = ["AB","BC","ON","QC","MB"]
TYPES = ["CHEQUING","CHEQUING","SAVINGS","BUSINESS"]

records = []
for acc in ACCOUNTS:
    since = datetime(2018,1,1) + timedelta(days=random.randint(0,2000))
    records.append({
        "account_id":    acc,
        "full_name":     f"{random.choice(FIRST)} {random.choice(LAST)}",
        "age":           random.randint(22,68),
        "province":      random.choice(PROVS),
        "customer_since":since.strftime("%Y-%m-%d"),
        "account_type":  random.choice(TYPES),
    })

pd.DataFrame(records).to_csv("data/customers.csv", index=False)
print(f"Generated {len(records)} customer profiles → data/customers.csv")
