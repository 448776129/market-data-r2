"""采集股票媒体/行情快照信息入库 R2（股价K线之外的基本面快照）。

从 Yahoo chart API 的 meta 字段采集（名称/代码/币种/交易所/52周高低/
实时价/当日高低/成交量/上市日期/涨跌幅），存为 JSON 到 R2：
    {region}/meta/{symbol}.json

供 API 的 /quote 和 /price 补全 name/currency 等字段。

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=stocksmarkets
    python scripts/fetch_meta.py                  # 全部区域
    python scripts/fetch_meta.py --region us      # 仅美股
    python scripts/fetch_meta.py --region us --batch 0 --batches 4

说明：
    - 并发拉取（FETCH_CONCURRENCY 控制，默认 6）
    - 单只失败不中断整体（记录到 _status.json）
    - meta 体积小，一次性采集；后续可在增量工作流中低频刷新
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import marketlib  # noqa: E402
import r2store  # noqa: E402
import yahoo_meta  # noqa: E402


def upload_meta(region: str, symbol: str, meta: dict) -> bool:
    """上传单只股票的 meta JSON 到 R2。"""
    key = f"{region}/meta/{symbol}.json"
    data = json.dumps(meta, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        s3 = r2store.get_client()
        s3.put_object(
            Bucket=r2store.get_bucket(),
            Key=key,
            Body=data,
            ContentType="application/json; charset=utf-8",
        )
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  [上传失败] {symbol}: {exc}", flush=True)
        return False


def _process_one(region: str, symbol: str) -> tuple[str, bool, dict]:
    """并发处理单只股票：采集 meta 并上传。返回 (symbol, ok, meta)。"""
    try:
        meta = yahoo_meta.fetch_meta(symbol)
        if not meta:
            return symbol, False, {}
        ok = upload_meta(region, symbol, meta)
        return symbol, ok, meta
    except Exception as exc:  # noqa: BLE001
        print(f"  [失败] {region}:{symbol}: {exc}", flush=True)
        return symbol, False, {}


def run(region: str | None, batch: int = 0, batches: int = 1) -> int:
    regions = [region] if region else list(config.REGIONS)
    concurrency = int(os.environ.get("FETCH_CONCURRENCY", "6"))
    ok_count = 0
    failed: list[str] = []

    for reg in regions:
        symbols = marketlib.load_symbols(reg)
        symbols = marketlib.slice_batch(symbols, batch, batches)
        if not symbols:
            print(f"[警告] {reg}: 无符号", flush=True)
            continue
        print(f"[区域] {reg} ({len(symbols)} 只, 批 {batch+1}/{batches}, 并发 {concurrency})", flush=True)

        done = 0
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(_process_one, reg, sym): sym for sym in symbols}
            for fut in as_completed(futures):
                sym, ok, _ = fut.result()
                done += 1
                if ok:
                    ok_count += 1
                else:
                    failed.append(f"{reg}:{sym}")
                if done % 50 == 0 or done == len(symbols):
                    print(f"  [{done}/{len(symbols)}] {reg} 完成，成功 {ok_count}，失败 {len(failed)}", flush=True)

    r2store.put_status(
        {
            "mode": "meta",
            "completed_at": r2store.now_iso(),
            "regions": regions,
            "ok": ok_count,
            "failed": failed[:100],
            "fail_count": len(failed),
        }
    )
    print(f"meta 采集完成: 成功 {ok_count}, 失败 {len(failed)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="采集股票媒体/行情快照信息入库 R2")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅处理指定区域")
    parser.add_argument("--batch", type=int, default=0, help="当前批次（0 起）")
    parser.add_argument("--batches", type=int, default=1, help="总批次数")
    args = parser.parse_args()
    return run(args.region, args.batch, args.batches)


if __name__ == "__main__":
    sys.exit(main())
