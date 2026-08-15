"""临时：找出 R2 中缺失的 ETF 代码。"""
import os
import sys

os.environ["R2_ACCOUNT_ID"] = "8e43ef2043266e0898cf9e02ca53df2f"
os.environ["R2_ACCESS_KEY_ID"] = "f281f625faec33df8dec94264fc99aaf"
os.environ["R2_SECRET_ACCESS_KEY"] = "e991989df172668dac0f4d61f5ff0b2419711f9fb60616e8c620d5b55ab93919"
os.environ["R2_BUCKET"] = "stocksmarkets"

sys.path.insert(0, ".")
sys.path.insert(0, "scripts")
from scripts import r2store, marketlib  # noqa: E402

s3 = r2store.get_client()
existing = set()
for page in s3.get_paginator("list_objects_v2").paginate(Bucket="stocksmarkets", Prefix="etf/kline/"):
    for o in page.get("Contents", []):
        existing.add(o["Key"].split("/")[-1].replace(".csv", ""))

all_etf = set(marketlib.load_symbols("etf"))
missing = sorted(all_etf - existing)
print(f"缺失 {len(missing)} 只: {missing}")
