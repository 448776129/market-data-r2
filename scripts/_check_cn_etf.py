"""临时：确认中国 ETF 各周期数据入库情况。"""
import os
import sys

os.environ["R2_ACCOUNT_ID"] = "8e43ef2043266e0898cf9e02ca53df2f"
os.environ["R2_ACCESS_KEY_ID"] = "f281f625faec33df8dec94264fc99aaf"
os.environ["R2_SECRET_ACCESS_KEY"] = "e991989df172668dac0f4d61f5ff0b2419711f9fb60616e8c620d5b55ab93919"
os.environ["R2_BUCKET"] = "stocksmarkets"

sys.path.insert(0, ".")
sys.path.insert(0, "scripts")
from scripts import r2store  # noqa: E402

s3 = r2store.get_client()
for sd in ["kline", "kline_1m", "kline_1h", "kline_5m", "kline_15m", "kline_30m", "meta"]:
    prefix = f"cn_etf/{sd}/"
    count = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="stocksmarkets", Prefix=prefix):
        count += len(page.get("Contents", []))
    print(f"  cn_etf/{sd}: {count}")

# 验证一个中国ETF数据
raw = r2store.get_bytes("cn_etf/kline/510300.SS.csv")
if raw:
    import gzip
    t = gzip.decompress(raw).decode("utf-8-sig")
    lines = t.strip().splitlines()
    print(f"\n510300.SS 日K: {len(lines)} 行")
    print("首行:", lines[0])
    print("末行:", lines[-1])
