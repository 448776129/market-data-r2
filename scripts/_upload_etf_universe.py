"""临时：上传新的 ETF 清单到 R2 universe/ 前缀。"""
import os
import sys

os.environ["R2_ACCOUNT_ID"] = "8e43ef2043266e0898cf9e02ca53df2f"
os.environ["R2_ACCESS_KEY_ID"] = "f281f625faec33df8dec94264fc99aaf"
os.environ["R2_SECRET_ACCESS_KEY"] = "e991989df172668dac0f4d61f5ff0b2419711f9fb60616e8c620d5b55ab93919"
os.environ["R2_BUCKET"] = "stocksmarkets"

sys.path.insert(0, ".")
sys.path.insert(0, "scripts")
from scripts import r2store  # noqa: E402

text = open("data/universe/etf.csv", encoding="utf-8").read()
n = len([l for l in text.splitlines() if l.strip()])
r2store.put_universe("etf", text)
print(f"上传 universe/etf.csv ({n} 只)")
