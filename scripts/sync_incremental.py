"""增量同步脚本（唯一的定时 action）。

在 fetch_history.py 全量入库之后运行，只获取新增数据并同步到 R2：
    - 日K：从已有最后日期往回看缓冲段拉取增量，合并去重
    - 分钟K：只拉已有最后时间点之后的新数据（含回看缓冲）
    - 5m/15m/30m：由 1m 增量数据重采样合并
    - 美股 1m/1h 含盘前盘后延长时段

增量优化：
    - 交易时段查重：市场休市（周末/非交易时段）且数据已最新时跳过请求
    - 只读 R2 已有对象（gzip 解压）判断最后时间点，避免每天全量重拉
    - 并发上传（scripts/r2store.py）

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=...
    python scripts/sync_incremental.py                 # 全部区域
    python scripts/sync_incremental.py --region us     # 仅美股
    python scripts/sync_incremental.py --region cn --batch 0 --batches 10
"""

from __future__ import annotations

import argparse
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402
import yahoo_chart  # noqa: E402

COLS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
DATE_COL = "Date"
DT_COL = "Datetime"

SOURCE_INTERVALS = ["1m", "1h"]
# 各周期 R2 key 前缀
SUBDIR = {
    "1d": config.KLINE_SUBDIR,
    "1m": config.INTRADAY_M1_SUBDIR,
    "5m": config.INTRADAY_M5_SUBDIR,
    "15m": config.INTRADAY_M15_SUBDIR,
    "30m": config.INTRADAY_M30_SUBDIR,
    "1h": config.INTRADAY_M1H_SUBDIR,
}
# 日K增量回看缓冲天数（覆盖除权/分红修订）
DAILY_BUFFER_DAYS = 7
# 日K增量：已有数据距今超过该天数视为缺数据，强制全量补拉最近窗口
STALE_DAYS = 3


def key_for(region: str, symbol: str, interval: str) -> str:
    return f"{region}/{SUBDIR[interval]}/{symbol}.csv"


def load_existing(region: str, symbol: str, interval: str, index_col: str) -> pd.DataFrame | None:
    """从 R2 读取该股票该周期已有数据（自动解压 gzip）。"""
    text = r2store.get_csv_text(key_for(region, symbol, interval))
    if text is None:
        return None
    df = pd.read_csv(io.StringIO(text), index_col=index_col, parse_dates=True)
    df.index = pd.to_datetime(df.index)
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    if index_col == DATE_COL:
        df.index = df.index.normalize()
    return df


def merge_and_upload(region: str, symbol: str, interval: str, fresh: pd.DataFrame) -> int:
    """与 R2 已有数据合并去重后写回，返回新增行数。"""
    index_col = DATE_COL if interval == "1d" else DT_COL
    existing = load_existing(region, symbol, interval, index_col)

    fresh = fresh[COLS].copy()
    fresh.index = pd.to_datetime(fresh.index)
    if getattr(fresh.index, "tz", None) is not None:
        fresh.index = fresh.index.tz_localize(None)
    if index_col == DATE_COL:
        fresh.index = fresh.index.normalize()

    if existing is None or existing.empty:
        merged = fresh
    else:
        merged = pd.concat([existing, fresh])
        merged = merged[~merged.index.duplicated(keep="last")].sort_index()

    before = len(existing) if existing is not None else 0
    added = len(merged) - before
    if added > 0:
        csv_text = merged.to_csv()
        r2store.put_csv(key_for(region, symbol, interval), csv_text)
    return max(added, 0)


def fetch_incremental(region: str, symbol: str, interval: str) -> int:
    """拉取单只股票指定周期增量并合并上传，返回新增行数。"""
    index_col = DATE_COL if interval == "1d" else DT_COL
    existing = load_existing(region, symbol, interval, index_col)

    now = marketlib.region_now(region)

    # ---- 增量查重：休市且数据最新则跳过 ----
    if existing is not None and not existing.empty:
        last_ts = existing.index.max()
        if interval == "1d":
            # 日K：仅在非交易时段跳过（避免盘中跳过导致收盘bar停留在实时值）
            recent_cutoff = now.date() - timedelta(days=2)
            if (
                not marketlib.is_market_session(region, now)
                and last_ts.date() >= recent_cutoff
            ):
                return 0
            # 精确增量：从最后日期往回看缓冲段
            start = (last_ts - pd.Timedelta(days=DAILY_BUFFER_DAYS)).date()
            fresh = yahoo_chart.fetch_kline(
                symbol, interval="1d", start=start, prepost=False
            )
        else:
            # 分钟K：交易时段外整批跳过由 run() 处理；此处只做时间增量
            start = last_ts - pd.Timedelta(days=config.INTRADAY_BUFFER_DAYS)
            fresh = yahoo_chart.fetch_kline(
                symbol, interval=interval, start=start, prepost=True
            )
    else:
        # 无已有数据：全量拉取该周期
        fresh = yahoo_chart.fetch_kline(
            symbol,
            interval=interval,
            period=period_for(interval),
            prepost=(interval != "1d"),
        )

    if fresh is None or fresh.empty:
        return 0
    return merge_and_upload(region, symbol, interval, fresh)


def period_for(interval: str) -> str:
    """该周期首次全量拉取的 period。"""
    if interval == "1d":
        return config.HISTORY_PERIOD
    return config.INTRADAY_PERIOD[interval]


def sync_minute_and_derived(region: str, symbol: str) -> int:
    """拉取 1m + 1h 增量，并由 1m 增量重采样合并 5m/15m/30m。返回新增行数。"""
    added = 0
    for interval in SOURCE_INTERVALS:
        added += fetch_incremental(region, symbol, interval)

    # 由 1m 增量合并后的数据重采样派生 5m/15m/30m
    m1 = load_existing(region, symbol, "1m", DT_COL)
    if m1 is not None and not m1.empty:
        for target, rule in config.INTRADAY_DERIVED.items():
            agg = m1.resample(rule).agg(
                {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}
            )
            agg = agg.dropna(subset=["Close"])
            if agg.empty:
                continue
            agg["Adj Close"] = m1["Close"].resample(rule).last()
            agg = agg[COLS]
            # 与已有合并去重
            index_col = DT_COL
            existing = load_existing(region, symbol, target, index_col)
            agg.index = pd.to_datetime(agg.index)
            if existing is None or existing.empty:
                merged = agg
            else:
                merged = pd.concat([existing, agg])
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
            before = len(existing) if existing is not None else 0
            if len(merged) > before:
                r2store.put_csv(key_for(region, symbol, target), merged.to_csv())
                added += len(merged) - before
    return added


def _process_one(reg: str, symbol: str, do_minute: bool) -> tuple[str, int, str]:
    """并发处理单只股票：返回 (symbol, added, err_msg)。err_msg 空表示成功。"""
    try:
        added_d = fetch_incremental(reg, symbol, "1d")
        added_m = 0
        if do_minute:
            added_m = sync_minute_and_derived(reg, symbol)
        return symbol, added_d + added_m, ""
    except Exception as exc:  # noqa: BLE001
        return symbol, 0, str(exc)


def run(region: str | None, batch: int = 0, batches: int = 1) -> int:
    regions = [region] if region else list(config.REGIONS)
    # 并发线程数（可用环境变量 FETCH_CONCURRENCY 覆盖）
    concurrency = int(os.environ.get("FETCH_CONCURRENCY", "6"))

    # 分钟K：先判断各市场是否处于交易时段，休市市场跳过
    active_regions = set()
    for reg in regions:
        if marketlib.is_market_session(reg):
            active_regions.add(reg)
        else:
            print(f"[跳过] {reg}: 当前不在交易时段（周末/休市），跳过分钟K", flush=True)

    total_added = 0
    failed: list[str] = []

    for reg in regions:
        symbols = marketlib.load_symbols(reg)
        symbols = marketlib.slice_batch(symbols, batch, batches)
        if not symbols:
            continue
        print(f"[区域] {reg} ({len(symbols)} 只, 批 {batch+1}/{batches}, 并发 {concurrency})", flush=True)

        do_minute = reg in active_regions
        done = 0
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(_process_one, reg, sym, do_minute): sym for sym in symbols}
            for fut in as_completed(futures):
                sym, added, err = fut.result()
                done += 1
                if err:
                    failed.append(f"{reg}:{sym}")
                elif added:
                    total_added += added
                if done % 25 == 0 or done == len(symbols):
                    print(
                        f"  [{done}/{len(symbols)}] {reg} 已处理，累计新增 {total_added} 行，失败 {len(failed)}",
                        flush=True,
                    )

    r2store.put_status(
        {
            "mode": "incremental",
            "completed_at": r2store.now_iso(),
            "regions": regions,
            "regions_minute": regions_minute,
            "added": total_added,
            "failed": failed[:100],
            "fail_count": len(failed),
        }
    )
    print(f"增量完成: 新增 {total_added} 行, 失败 {len(failed)} 项")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description="增量同步（定时，新增数据入库 R2）")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅处理指定区域")
    parser.add_argument("--batch", type=int, default=0, help="当前批次（0 起）")
    parser.add_argument("--batches", type=int, default=1, help="总批次数")
    args = parser.parse_args()
    return run(args.region, args.batch, args.batches)


if __name__ == "__main__":
    sys.exit(main())
