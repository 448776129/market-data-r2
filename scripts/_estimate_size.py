"""临时：估算全量数据（纯文本）体积是否在 R2 10GB 免费额度内。

依据 AAPL 解压后各周期文件大小，按股票数量外推。
"""
import sys

sys.path.insert(0, ".")
sys.path.insert(0, "scripts")
from scripts import marketlib, yahoo_chart  # noqa: E402

# 各周期单只股票平均大小（基于 AAPL 实测 + 经验估计，单位 KB）
# 日K 5y ~ 1255 行; 1m 5d ~ 4800 行; 1h 6mo ~ 2100 行; 派生 5m/15m/30m
SIZES_KB = {
    "1d": 150,   # 1255 行 ~ 150KB
    "1m": 340,   # 4800 行 ~ 340KB
    "1h": 150,   # 2100 行 ~ 150KB
    "5m": 70,    # 960 行
    "15m": 25,   # 320 行
    "30m": 13,   # 160 行
}
PER_STOCK_KB = sum(SIZES_KB.values())
print(f"单只股票全部周期 ≈ {PER_STOCK_KB} KB (纯文本)")

for region in ["us", "cn", "hk", "kr"]:
    n = len(marketlib.load_symbols(region))
    size_mb = n * PER_STOCK_KB / 1024
    print(f"  {region}: {n} 只 × {PER_STOCK_KB}KB = {size_mb:.0f} MB")

total_mb = sum(len(marketlib.load_symbols(r)) for r in ["us", "cn", "hk", "kr"]) * PER_STOCK_KB / 1024
print(f"\n总计 ≈ {total_mb:.0f} MB (纯文本)")
print(f"R2 免费 10GB = 10240 MB → 剩余 {10240 - total_mb:.0f} MB")
