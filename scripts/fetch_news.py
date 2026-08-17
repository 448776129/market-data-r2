"""采集股票新闻（独立于 K 线 / meta 的第三套采集）。

从 Yahoo /v1/finance/search（经反代，免认证）采集每只股票的相关新闻，
附带行情快照（公司名/板块/行业/证券类型），存为 JSON 到 R2：
    {region}/news/{symbol}.json

与 K 线（{region}/kline*/）、基本面 meta（{region}/meta/）完全分离。

用法：
    export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=stocksmarkets
    python scripts/fetch_news.py                  # 全部区域
    python scripts/fetch_news.py --region us      # 仅美股
    python scripts/fetch_news.py --region us --batch 0 --batches 4

说明：
    - 并发拉取（FETCH_CONCURRENCY 控制，默认 6）
    - 单只失败不中断整体（记录到 _status.json）
    - 新闻体积小，建议低频（如每天一次）刷新
"""

from __future__ import annotations

import argparse
import hashlib
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
import state  # noqa: E402
import yahoo_news  # noqa: E402


def upload_news(region: str, symbol: str, data: dict) -> bool:
    """上传单只股票的新闻 JSON 到 R2。"""
    key = f"{region}/news/{symbol}.json"
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        s3 = r2store.get_client()
        s3.put_object(
            Bucket=r2store.get_bucket(),
            Key=key,
            Body=body,
            ContentType="application/json; charset=utf-8",
        )
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  [上传失败] {symbol}: {exc}", flush=True)
        return False


def _url_fingerprint(data: dict) -> str | None:
    """基于新闻 link 集合生成稳定指纹。新闻内容是否变化只看 url 集合。

    返回 None 表示本次没有任何新闻（无内容可比），此时不进行 diff。
    """
    links = []
    for n in data.get("news") or []:
        u = n.get("link")
        if u:
            links.append(str(u))
    if not links:
        return None
    links.sort()
    body = "\n".join(links).encode("utf-8")
    return hashlib.md5(body).hexdigest()


def _process_one(
    region: str, symbol: str, known: dict | None
) -> tuple[str, bool, str | None]:
    """并发处理单只股票：采集新闻，url 集合有新增才写 R2。

    返回 (symbol, ok, new_fingerprint_or_None)。
    - known 为状态里存的 {symbol: {"h": 指纹}} 中该股的条目。
    - 返回的 new_fingerprint：仅在真正写入 R2 时返回新的 url 指纹，否则 None，
      调用方据此更新本地 state；未变化时跳过写，省掉一次 R2 写操作。
    """
    try:
        data = yahoo_news.fetch_news(symbol)
        n = len(data.get("news") or [])
        if data.get("quote") is None and n == 0:
            # 没有任何数据返回：视为失败，不更新状态
            return symbol, False, None
        h = _url_fingerprint(data)
        if h is not None and known is not None and known.get("h") == h:
            # url 集合没变：新闻没有新增，不写 R2
            return symbol, True, None
        ok = upload_news(region, symbol, data)
        # 只有真正写成功才推进指纹，避免失败时状态被污染
        return symbol, ok, h if ok and h is not None else None
    except Exception as exc:  # noqa: BLE001
        print(f"  [失败] {region}:{symbol}: {exc}", flush=True)
        return symbol, False, None


def run(region: str | None, batch: int = 0, batches: int = 1) -> int:
    regions = [region] if region else list(config.REGIONS)
    concurrency = int(os.environ.get("FETCH_CONCURRENCY", "6"))
    ok_count = 0
    changed_count = 0
    failed: list[str] = []

    for reg in regions:
        symbols = marketlib.load_symbols(reg)
        symbols = marketlib.slice_batch(symbols, batch, batches)
        if not symbols:
            print(f"[警告] {reg}: 无符号", flush=True)
            continue
        # 该 (region, batch) 独立的状态清单；只在有新闻新增时才触碰 R2
        snap = state.read("news", reg, batch)
        print(f"[区域] {reg} ({len(symbols)} 只, 批 {batch+1}/{batches}, 并发 {concurrency})", flush=True)

        done = 0
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {
                pool.submit(_process_one, reg, sym, snap.get(sym)): sym for sym in symbols
            }
            for fut in as_completed(futures):
                sym, ok, fp = fut.result()
                done += 1
                if ok:
                    ok_count += 1
                    if fp is not None:
                        snap[sym] = {"h": fp}
                        changed_count += 1
                else:
                    failed.append(f"{reg}:{sym}")
                if done % 50 == 0 or done == len(symbols):
                    print(
                        f"  [{done}/{len(symbols)}] {reg} 成功 {ok_count}，新增写入 {changed_count}，失败 {len(failed)}",
                        flush=True,
                    )
        # 仅当本轮确实产生 / 刷新过该批状态时才写回，避免无谓写操作
        if snap:
            state.write("news", reg, batch, snap)

    r2store.put_status(
        {
            "mode": "news",
            "completed_at": r2store.now_iso(),
            "regions": regions,
            "ok": ok_count,
            "changed": changed_count,
            "failed": failed[:100],
            "fail_count": len(failed),
        }
    )
    print(f"新闻采集完成: 成功 {ok_count}, 新增 {changed_count}, 失败 {len(failed)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="采集股票新闻入库 R2")
    parser.add_argument("--region", choices=list(config.REGIONS), help="仅处理指定区域")
    parser.add_argument("--batch", type=int, default=0, help="当前批次（0 起）")
    parser.add_argument("--batches", type=int, default=1, help="总批次数")
    args = parser.parse_args()
    return run(args.region, args.batch, args.batches)


if __name__ == "__main__":
    sys.exit(main())
