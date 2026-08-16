"""Yahoo Finance 新闻采集（经反代访问 /v1/finance/search）。

使用 Yahoo 免费公开的 search 接口（无需 crumb 认证，实测可用）获取
指定股票的相关新闻。同时附带返回该股票的行情快照字段（公司名/板块/行业/
证券类型等），一并整理返回。

存储设计（独立于 K 线 / meta）：
    {region}/news/{symbol}.json
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from typing import Any

import config  # noqa: E402

_ORIGIN = "https://query1.finance.yahoo.com/v1/finance/search"
_PROXY_BASE = config.YAHOO_CHART_PROXY


def fetch_news(
    symbol: str,
    news_count: int = 8,
    retries: int | None = None,
    delay: float | None = None,
) -> dict[str, Any]:
    """拉取单只股票的新闻与行情快照。

    返回：
    {
        "symbol": "...",
        "news": [{"title","publisher","providerPublishTime","link","type","relatedTickers"}, ...],
        "quote": {symbol, shortname, longname, quoteType, exchange, sector, industry},
        "collected_at": "ISO 时间",
    }
    失败重试后仍失败抛出异常。
    """
    query = urllib.parse.urlencode(
        {"q": symbol, "quotesCount": 1, "newsCount": news_count}
    )
    raw_url = f"{_ORIGIN}?{query}"
    url = f"{_PROXY_BASE.rstrip('/')}/{raw_url}"
    retries = config.MAX_RETRIES if retries is None else retries
    delay = config.REQUEST_DELAY if delay is None else delay

    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return _normalize(symbol, data)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries - 1:
                wait = delay * (2**attempt)
                print(f"    重试 {attempt+1}/{retries-1}（等 {wait:.0f}s）：{exc}", flush=True)
                time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    return {"symbol": symbol, "news": [], "quote": None}


def _normalize(symbol: str, data: dict) -> dict[str, Any]:
    """把 search 返回整理为新闻 + 行情快照。"""
    news_raw = data.get("news") or []
    news = []
    for n in news_raw[:20]:
        item = {
            "title": n.get("title"),
            "publisher": n.get("publisher"),
            "providerPublishTime": n.get("providerPublishTime"),  # unix 秒
            "link": n.get("link"),
            "type": n.get("type"),
            "relatedTickers": n.get("relatedTickers"),
        }
        news.append(item)

    quotes_raw = data.get("quotes") or []
    quote = None
    if quotes_raw:
        q = quotes_raw[0]
        quote = {
            "symbol": q.get("symbol"),
            "shortname": q.get("shortname"),
            "longname": q.get("longname"),
            "quoteType": q.get("quoteType"),
            "exchange": q.get("exchange"),
            "exchDisp": q.get("exchDisp"),
            "sector": q.get("sector"),
            "sectorDisp": q.get("sectorDisp"),
            "industry": q.get("industry"),
            "industryDisp": q.get("industryDisp"),
        }

    return {
        "symbol": symbol,
        "news": news,
        "quote": quote,
        "collected_at": __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ).isoformat(),
    }
