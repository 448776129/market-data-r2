var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// src/index.js
var REPO_OWNER = "448776129";
var REPO_NAME = "market-data-pipeline";
var REPO_BRANCH = "master";
var API_BASE = "https://stockapi.365200.xyz";
var INTERVAL_DIR = {
  "1d": "kline",
  "1m": "kline_1m",
  "5m": "kline_5m",
  "15m": "kline_15m",
  "30m": "kline_30m",
  "1h": "kline_1h"
};
var REGION_LABEL = {
  cn: "A\u80A1",
  us: "\u7F8E\u80A1",
  hk: "\u6E2F\u80A1",
  kr: "\u97E9\u80A1"
};
var INDEX_LABEL = {
  csi300: "\u6CAA\u6DF1300",
  csi500: "\u4E2D\u8BC1500",
  nasdaq100: "\u7EB3\u65AF\u8FBE\u514B100",
  sp500: "\u6807\u666E500",
  hsi: "\u6052\u751F\u6307\u6570",
  cn: "A\u80A1\u5168\u90E8",
  us: "\u7F8E\u80A1\u5168\u90E8",
  hk: "\u6E2F\u80A1\u5168\u90E8",
  kr: "\u97E9\u80A1\u5168\u90E8"
};
function inferRegion(symbol) {
  const s = symbol.toUpperCase();
  if (s.endsWith(".HK"))
    return "hk";
  if (s.endsWith(".KS") || s.endsWith(".KQ"))
    return "kr";
  if (s.endsWith(".SS") || s.endsWith(".SZ"))
    return "cn";
  return "us";
}
__name(inferRegion, "inferRegion");
function parseCSV(text) {
  const lines = text.split(/\r?\n/).filter((l) => l.trim() !== "");
  if (lines.length === 0)
    return [];
  const header = splitLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const cells = splitLine(lines[i]);
    if (cells.length === 0)
      continue;
    const obj = {};
    for (let j = 0; j < header.length; j++) {
      obj[header[j]] = cells[j] !== void 0 ? cells[j] : "";
    }
    rows.push(obj);
  }
  return rows;
}
__name(parseCSV, "parseCSV");
function splitLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      out.push(cur);
      cur = "";
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}
__name(splitLine, "splitLine");
function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Cache-Control": "public, max-age=60"
  };
}
__name(corsHeaders, "corsHeaders");
function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8", ...corsHeaders() }
  });
}
__name(json, "json");
function error(msg, status = 400) {
  return json({ error: msg }, status);
}
__name(error, "error");
function html(text, status = 200) {
  return new Response(text, {
    status,
    headers: { "Content-Type": "text/html; charset=utf-8", ...corsHeaders() }
  });
}
__name(html, "html");
async function fetchUpstream(path, env) {
  if (env && env.MARKET_DATA_R2) {
    try {
      const r2key = path.replace(/^data\//, "");
      const obj = await env.MARKET_DATA_R2.get(r2key);
      if (obj) {
        const bytes = await obj.arrayBuffer();
        const data = new Uint8Array(bytes);
        const gzipMagic = data.length >= 2 && data[0] === 31 && data[1] === 139;
        const enc = obj.httpMetadata && (obj.httpMetadata.contentEncoding || "");
        if (gzipMagic || enc === "gzip" || r2key.endsWith(".gz")) {
          const ds = new DecompressionStream("gzip");
          const stream = new Blob([data]).stream().pipeThrough(ds);
          const buf = await new Response(stream).arrayBuffer();
          return new TextDecoder().decode(buf);
        }
        return new TextDecoder().decode(data);
      }
    } catch (e) {
      console.warn(`R2 read failed for ${path}: ${e.message}`);
    }
  }
  const url = `https://raw.githubusercontent.com/${REPO_OWNER}/${REPO_NAME}/${REPO_BRANCH}/${path}`;
  let resp;
  try {
    resp = await fetch(url);
  } catch (e) {
    throw new Error(`upstream fetch failed: ${e.message}`);
  }
  if (resp.status === 404) {
    return null;
  }
  if (!resp.ok) {
    throw new Error(`upstream error: ${resp.status}`);
  }
  return await resp.text();
}
__name(fetchUpstream, "fetchUpstream");
function tsOf(value) {
  if (value === void 0 || value === null || value === "")
    return null;
  const d = new Date(value);
  return isNaN(d.getTime()) ? null : d.getTime();
}
__name(tsOf, "tsOf");
async function handleKline(params, env) {
  const symbol = (params.get("symbol") || "").trim().toUpperCase();
  if (!symbol) {
    return json({
      usage: {
        endpoint: `${API_BASE}/kline`,
        description: "\u67E5\u8BE2\u4EFB\u610F\u80A1\u7968K\u7EBF\u6570\u636E\uFF08\u65E5K / 1m / 5m / 15m / 30m / 1h\uFF09",
        params: {
          symbol: "\u80A1\u7968\u4EE3\u7801\uFF08\u5FC5\u586B\uFF09\uFF0C\u5982 AAPL / 0700.HK / 600519.SS / 000001.SZ",
          interval: `\u5468\u671F\uFF0C\u9ED8\u8BA4 1d\u3002\u53EF\u9009\uFF1A${Object.keys(INTERVAL_DIR).join(" / ")}`,
          start: "\u8D77\u59CB\u65E5\u671F YYYY-MM-DD\uFF08\u542B\uFF09",
          end: "\u7ED3\u675F\u65E5\u671F YYYY-MM-DD\uFF08\u542B\uFF09",
          limit: "\u6700\u591A\u8FD4\u56DE\u884C\u6570\uFF1B\u9ED8\u8BA4\u8FD4\u56DE\u65F6\u95F4\u4E0A\u6700\u65B0 N \u6761",
          order: "asc(\u9ED8\u8BA4\uFF0C\u65F6\u95F4\u5347\u5E8F) / desc(\u6700\u65B0\u5728\u524D)",
          format: "json(\u9ED8\u8BA4) / csv"
        },
        example: `${API_BASE}/kline?symbol=AAPL&interval=1d&start=2024-01-01&end=2024-12-31`
      }
    });
  }
  const interval = (params.get("interval") || "1d").toLowerCase();
  if (!INTERVAL_DIR[interval]) {
    return error(`Invalid interval: ${interval}. Allowed: ${Object.keys(INTERVAL_DIR).join(", ")}`);
  }
  const region = (params.get("region") || inferRegion(symbol)).toLowerCase();
  const limit = parseInt(params.get("limit") || "0", 10);
  const format = (params.get("format") || "json").toLowerCase();
  const startTs = tsOf(params.get("start"));
  const endTs = tsOf(params.get("end"));
  const dir = INTERVAL_DIR[interval];
  const text = await fetchUpstream(`data/${region}/${dir}/${symbol}.csv`, env);
  if (text === null) {
    return error(
      `No data for ${symbol} (${interval}). File not found: data/${region}/${dir}/${symbol}.csv`,
      404
    );
  }
  if (format === "csv") {
    return new Response(text, {
      status: 200,
      headers: { "Content-Type": "text/csv; charset=utf-8", ...corsHeaders() }
    });
  }
  let rows = parseCSV(text);
  const indexCol = interval === "1d" ? "Date" : "Datetime";
  if (startTs !== null || endTs !== null) {
    rows = rows.filter((r) => {
      const t = tsOf(r[indexCol]);
      if (t === null)
        return true;
      if (startTs !== null && t < startTs)
        return false;
      if (endTs !== null && t > endTs)
        return false;
      return true;
    });
  }
  const order = (params.get("order") || "asc").toLowerCase();
  if (order === "desc") {
    rows.reverse();
  }
  if (limit > 0) {
    rows = order === "desc" ? rows.slice(0, limit) : rows.slice(-limit);
  }
  return json({ symbol, region, interval, count: rows.length, order, data: rows });
}
__name(handleKline, "handleKline");
async function handleQuote(params, env) {
  const symbol = (params.get("symbol") || "").trim().toUpperCase();
  if (!symbol) {
    return json({
      usage: {
        endpoint: `${API_BASE}/quote`,
        description: "\u67E5\u8BE2\u4E2A\u80A1\u5143\u6570\u636E\uFF08\u516C\u53F8\u540D/\u884C\u4E1A/\u5E02\u503C/\u6700\u65B0\u4EF7\u7B49\uFF09",
        params: { symbol: "\u80A1\u7968\u4EE3\u7801\uFF08\u5FC5\u586B\uFF09" },
        example: `${API_BASE}/quote?symbol=AAPL`
      }
    });
  }
  const region = (params.get("region") || inferRegion(symbol)).toLowerCase();
  const text = await fetchUpstream(`data/${region}/meta/${symbol}.json`, env);
  if (text === null) {
    return error(`No meta for ${symbol}. File not found: data/${region}/meta/${symbol}.json`, 404);
  }
  let meta;
  try {
    meta = JSON.parse(text);
  } catch {
    return error(`Invalid meta JSON for ${symbol}`, 502);
  }
  const info = meta.info || {};
  const pick = [
    "longName",
    "shortName",
    "sector",
    "industry",
    "country",
    "exchange",
    "currency",
    "marketCap",
    "currentPrice",
    "open",
    "previousClose",
    "dayHigh",
    "dayLow",
    "regularMarketPrice",
    "regularMarketPreviousClose",
    "fiftyTwoWeekHigh",
    "fiftyTwoWeekLow",
    "trailingPE",
    "forwardPE",
    "priceToBook",
    "dividendYield",
    "dividendRate",
    "trailingEps",
    "fiftyDayAverage",
    "twoHundredDayAverage",
    "totalRevenue",
    "freeCashflow"
  ];
  const quote = {};
  for (const k of pick) {
    if (info[k] !== void 0 && info[k] !== null)
      quote[k] = info[k];
  }
  return json({
    symbol: meta.symbol,
    region: meta.region,
    name: meta.name,
    currency: meta.currency,
    exchange: meta.exchange,
    isin: meta.isin || null,
    quote
  });
}
__name(handleQuote, "handleQuote");
async function handleUniverse(params, env) {
  const index = (params.get("index") || "").trim().toLowerCase();
  if (!index) {
    return json({
      usage: {
        endpoint: `${API_BASE}/universe`,
        description: "\u83B7\u53D6\u6307\u5B9A\u6307\u6570/\u6E05\u5355\u7684\u6210\u5206\u80A1\u4EE3\u7801",
        params: {
          index: `\u5FC5\u586B\u3002\u53EF\u9009\uFF1A${Object.keys(INDEX_LABEL).join(" / ")}`
        },
        example: `${API_BASE}/universe?index=csi300`
      }
    });
  }
  if (!INDEX_LABEL[index]) {
    return error(`Invalid index: ${index}. Allowed: ${Object.keys(INDEX_LABEL).join(", ")}`);
  }
  const text = await fetchUpstream(`data/universe/${index}.csv`, env);
  if (text === null) {
    return error(`No universe file: data/universe/${index}.csv`, 404);
  }
  const symbols = text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l !== "" && !l.startsWith("#"));
  return json({
    index,
    name: INDEX_LABEL[index],
    count: symbols.length,
    symbols
  });
}
__name(handleUniverse, "handleUniverse");
async function handleIndices(env) {
  const names = Object.keys(INDEX_LABEL);
  const items = [];
  for (const name of names) {
    try {
      const text = await fetchUpstream(`data/universe/${name}.csv`, env);
      const count = text === null ? 0 : text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l !== "" && !l.startsWith("#")).length;
      items.push({ index: name, name: INDEX_LABEL[name], count });
    } catch {
      items.push({ index: name, name: INDEX_LABEL[name], count: 0 });
    }
  }
  return json({ base: API_BASE, indices: items });
}
__name(handleIndices, "handleIndices");
async function handleSymbols(params, env) {
  const region = (params.get("region") || "").trim().toLowerCase() || "us";
  if (!REGION_LABEL[region]) {
    return error(`Invalid region: ${region}. Allowed: ${Object.keys(REGION_LABEL).join(", ")}`);
  }
  const limit = Math.min(parseInt(params.get("limit") || "100", 10), 1e3);
  const offset = Math.max(parseInt(params.get("offset") || "0", 10), 0);
  const text = await fetchUpstream(`data/universe/${region}.csv`, env);
  if (text === null) {
    return error(`No universe file: data/universe/${region}.csv`, 404);
  }
  const symbols = text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l !== "" && !l.startsWith("#"));
  const page = symbols.slice(offset, offset + limit);
  return json({
    region,
    region_name: REGION_LABEL[region],
    total: symbols.length,
    offset,
    limit,
    count: page.length,
    symbols: page
  });
}
__name(handleSymbols, "handleSymbols");
function handleStatus() {
  return json({
    service: "StockAPI",
    base: API_BASE,
    repo: `${REPO_OWNER}/${REPO_NAME}@${REPO_BRANCH}`,
    endpoints: {
      kline: `${API_BASE}/kline`,
      quote: `${API_BASE}/quote`,
      universe: `${API_BASE}/universe`,
      indices: `${API_BASE}/indices`,
      symbols: `${API_BASE}/symbols`
    },
    intervals: Object.keys(INTERVAL_DIR),
    regions: Object.keys(REGION_LABEL),
    indexes: Object.keys(INDEX_LABEL),
    note: "\u6570\u636E\u7531 GitHub Actions \u81EA\u52A8\u589E\u91CF\u91C7\u96C6\uFF0C5m/15m/30m \u7531 1m \u91CD\u91C7\u6837\u8BA1\u7B97\uFF0C1h \u4E3A\u96C5\u864E\u539F\u751F\u5C0F\u65F6K\u7EBF\u3002"
  });
}
__name(handleStatus, "handleStatus");
var HOME_HTML = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="description" content="StockAPI \u2014 \u514D\u8D39\u884C\u60C5K\u7EBF\u6570\u636E\u63A5\u53E3\uFF0C\u57FA\u4E8E GitHub Actions \u81EA\u52A8\u91C7\u96C6 + Cloudflare Workers \u8FB9\u7F18\u5206\u53D1">
<title>StockAPI \xB7 \u514D\u8D39\u884C\u60C5K\u7EBF\u63A5\u53E3</title>
<style>
  :root{
    --bg:#070a0f; --panel:#0e141d; --panel2:#121a26; --line:#1e293b;
    --text:#e6edf3; --muted:#8b98a9; --dim:#5b6675;
    --accent:#34d399; --accent2:#22d3a5; --amber:#fbbf24; --red:#f87171; --blue:#60a5fa;
    --mono:ui-monospace,"SF Mono","SFMono-Regular",Menlo,Consolas,monospace;
    --sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  html{scroll-behavior:smooth}
  body{background:var(--bg);color:var(--text);font-family:var(--sans);line-height:1.6;-webkit-font-smoothing:antialiased}
  .wrap{max-width:1080px;margin:0 auto;padding:0 24px}
  a{color:var(--accent2);text-decoration:none}
  a:hover{text-decoration:underline}
  code{font-family:var(--mono)}
  ::selection{background:rgba(52,211,153,.25)}

  nav{position:sticky;top:0;z-index:50;background:rgba(7,10,15,.85);backdrop-filter:blur(10px);border-bottom:1px solid var(--line)}
  .nav{display:flex;align-items:center;justify-content:space-between;height:60px}
  .brand{display:flex;align-items:center;gap:10px;font-weight:700;font-size:17px}
  .brand .dot{width:10px;height:10px;border-radius:50%;background:var(--accent);box-shadow:0 0 12px var(--accent)}
  .brand b{font-family:var(--mono)}
  .nav-links{display:flex;gap:22px;font-size:14px;color:var(--muted)}
  .nav-links a{color:var(--muted)}
  .nav-links a:hover{color:var(--text);text-decoration:none}

  .hero{padding:88px 0 52px}
  .badge{display:inline-flex;align-items:center;gap:8px;font-family:var(--mono);font-size:12px;color:var(--accent);border:1px solid rgba(52,211,153,.3);background:rgba(52,211,153,.08);padding:5px 12px;border-radius:999px;margin-bottom:22px}
  .badge .pulse{width:7px;height:7px;border-radius:50%;background:var(--accent)}
  h1{font-size:clamp(30px,5vw,52px);line-height:1.1;letter-spacing:-.02em;font-weight:800}
  h1 .grad{background:linear-gradient(90deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;color:transparent}
  .sub{margin-top:18px;font-size:17px;color:var(--muted);max-width:680px}
  .codes{margin-top:30px;display:grid;gap:12px}
  .code{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px 16px;font-family:var(--mono);font-size:13.5px;overflow-x:auto;white-space:nowrap}
  .code .cmt{color:var(--dim)}
  .code .cmd{color:var(--accent)}
  .code .url{color:var(--text)}
  .stat-band{margin-top:40px;display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
  .stat{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px}
  .stat .n{font-family:var(--mono);font-size:26px;font-weight:700;color:var(--accent)}
  .stat .l{font-size:12.5px;color:var(--muted);margin-top:2px}

  section{padding:52px 0}
  .sec-head{display:flex;align-items:baseline;gap:12px;margin-bottom:26px}
  .sec-head .idx{font-family:var(--mono);color:var(--accent);font-size:13px}
  .sec-head h2{font-size:24px;font-weight:700;letter-spacing:-.01em}
  .sec-head .tag{font-size:12px;color:var(--dim);font-family:var(--mono)}

  .chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:22px}
  .chip{font-family:var(--mono);font-size:12px;color:var(--muted);border:1px solid var(--line);background:var(--panel);padding:5px 11px;border-radius:999px}
  .chip b{color:var(--accent)}

  .demo{background:var(--panel);border:1px solid var(--line);border-radius:16px;overflow:hidden}
  .demo-tabs{display:flex;gap:4px;padding:10px 12px 0;border-bottom:1px solid var(--line);flex-wrap:wrap}
  .demo-tab{font-family:var(--mono);font-size:12.5px;color:var(--muted);padding:8px 14px;border-radius:8px 8px 0 0;cursor:pointer;border-bottom:2px solid transparent}
  .demo-tab.on{color:var(--accent);border-bottom-color:var(--accent);background:rgba(52,211,153,.06)}
  .demo-body{display:grid;grid-template-columns:340px 1fr}
  .demo-form{padding:20px;border-right:1px solid var(--line);display:flex;flex-direction:column;gap:14px}
  .field label{display:block;font-size:12px;color:var(--muted);margin-bottom:6px;font-family:var(--mono)}
  .field input,.field select{width:100%;background:var(--panel2);border:1px solid var(--line);border-radius:8px;color:var(--text);padding:10px 12px;font-family:var(--mono);font-size:13px;outline:none}
  .field input:focus,.field select:focus{border-color:var(--accent)}
  .run{background:var(--accent);color:#03251a;border:none;border-radius:8px;padding:11px;font-family:var(--mono);font-weight:700;font-size:14px;cursor:pointer;transition:filter .15s}
  .run:hover{filter:brightness(1.08)}
  .run:active{transform:translateY(1px)}
  .demo-out{padding:0;margin:0;background:#0a0f16;font-family:var(--mono);font-size:12.5px;line-height:1.7;overflow:auto;max-height:460px}
  .demo-out pre{padding:20px;white-space:pre-wrap;word-break:break-word}
  .demo-out .ok{color:var(--accent)}
  .demo-out .err{color:var(--red)}
  .demo-out .dim{color:var(--dim)}

  .table-wrap{overflow-x:auto;border:1px solid var(--line);border-radius:12px}
  table{width:100%;border-collapse:collapse;font-size:14px;min-width:640px}
  th,td{text-align:left;padding:11px 16px;border-bottom:1px solid var(--line)}
  th{font-family:var(--mono);font-size:12px;color:var(--muted);background:var(--panel);text-transform:uppercase;letter-spacing:.04em}
  tr:last-child td{border-bottom:none}
  td code{color:var(--accent2);background:rgba(34,211,165,.08);padding:1px 6px;border-radius:5px;font-size:12.5px}
  td .req{color:var(--red);font-weight:700}
  td .opt{color:var(--dim)}

  .ep-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
  .ep{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px 18px}
  .ep .m{font-family:var(--mono);color:var(--accent);font-size:13px;font-weight:700}
  .ep .d{font-size:13px;color:var(--muted);margin-top:4px}
  .ep .ex{font-family:var(--mono);font-size:12px;color:var(--blue);margin-top:8px;background:var(--panel2);padding:6px 10px;border-radius:8px;overflow-x:auto;white-space:nowrap}

  .fields{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  .fcard{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:18px}
  .fcard h3{font-size:15px;margin-bottom:12px;font-family:var(--mono)}
  .fcard h3 .tag{font-size:11px;color:var(--dim);font-weight:400}
  .fcard ul{list-style:none}
  .fcard li{display:flex;align-items:baseline;gap:10px;padding:7px 0;border-bottom:1px dashed var(--line);font-size:13.5px}
  .fcard li:last-child{border-bottom:none}
  .fcard .k{font-family:var(--mono);color:var(--accent2);min-width:96px}
  .fcard .d{color:var(--muted)}

  footer{padding:40px 0 56px;border-top:1px solid var(--line);color:var(--dim);font-size:13px}
  .foot{display:flex;flex-wrap:wrap;gap:8px;justify-content:space-between;align-items:center}

  @media(max-width:860px){
    .demo-body{grid-template-columns:1fr}
    .demo-form{border-right:none;border-bottom:1px solid var(--line)}
    .fields,.ep-grid{grid-template-columns:1fr}
    .stat-band{grid-template-columns:1fr 1fr}
    .nav-links{display:none}
    .hero{padding:60px 0 36px}
  }
</style>
</head>
<body>
<nav><div class="wrap nav">
  <div class="brand"><span class="dot"></span><b>StockAPI</b></div>
  <div class="nav-links">
    <a href="#demo">\u5728\u7EBF\u6F14\u793A</a>
    <a href="#endpoints">\u63A5\u53E3\u4E00\u89C8</a>
    <a href="#api">API \u6587\u6863</a>
    <a href="#fields">\u6570\u636E\u5B57\u6BB5</a>
    <a href="#examples">\u793A\u4F8B</a>
  </div>
</div></nav>

<header class="hero"><div class="wrap">
  <div class="badge"><span class="pulse"></span> \u514D\u8D39 \xB7 \u65E0\u9700 Key \xB7 \u5168\u7403\u5E02\u573A \xB7 \u8FB9\u7F18\u5206\u53D1</div>
  <h1>\u514D\u8D39\u884C\u60C5 K \u7EBF<br>\u6570\u636E <span class="grad">\u63A5\u53E3</span></h1>
  <p class="sub">\u7531 <b>GitHub Actions \u81EA\u52A8\u91C7\u96C6</b> A\u80A1 / \u7F8E\u80A1 / \u6E2F\u80A1 / \u97E9\u80A1 \u6838\u5FC3\u6307\u6570\u6210\u5206\u80A1\u7684 <b>\u65E5K\u30011\u5206\u949F\u30015\u300115\u300130\u5206\u949F\u30011\u5C0F\u65F6</b> K\u7EBF\uFF08\u7F8E\u80A1\u542B\u76D8\u524D\u76D8\u540E\u5EF6\u957F\u65F6\u6BB5\uFF09\uFF0Cgzip \u538B\u7F29\u5B58\u5165 <b>Cloudflare R2</b>\uFF0C\u7531 <b>Cloudflare Workers</b> \u5728\u8FB9\u7F18\u8282\u70B9\u81EA\u52A8\u89E3\u538B\u5E76\u8F6C\u6210 JSON / CSV \u8FD4\u56DE\uFF0C\u96F6\u670D\u52A1\u5668\u6210\u672C\uFF0C\u4F9B\u91CF\u5316\u7CFB\u7EDF\u76F4\u63A5\u8C03\u7528\u3002</p>
  <div class="codes">
    <div class="code"><span class="cmt"># \u4E00\u884C\u8BF7\u6C42\uFF0C\u8FD4\u56DE AAPL \u6700\u8FD1 5 \u6761\u65E5K</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=AAPL&amp;interval=1d&amp;limit=5</span>"</div>
    <div class="code"><span class="cmt"># \u4E2A\u80A1\u4FE1\u606F\uFF08\u540D\u79F0 / \u884C\u4E1A / \u5E02\u503C / \u6700\u65B0\u4EF7\uFF09</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/quote?symbol=600519.SS</span>" &nbsp; <span class="cmd">curl</span> "<span class="url">${API_BASE}/universe?index=csi300</span>"</div>
  </div>
  <div class="stat-band">
    <div class="stat"><div class="n">8000+</div><div class="l">\u6210\u5206\u80A1\uFF08\u6CAA\u6DF1300/\u4E2D\u8BC1500/\u7EB3\u6307100/\u6807\u666E500/\u6052\u751F\uFF09</div></div>
    <div class="stat"><div class="n">6</div><div class="l">\u5468\u671F\uFF08\u65E5K/1m/5m/15m/30m/1h\uFF09</div></div>
    <div class="stat"><div class="n">4</div><div class="l">\u5E02\u573A\uFF08A\u80A1/\u7F8E\u80A1/\u6E2F\u80A1/\u97E9\u80A1\uFF09</div></div>
    <div class="stat"><div class="n">0</div><div class="l">\u8D39\u7528\uFF08\u516C\u5F00\u4ED3\u5E93 + Workers \u514D\u8D39\u989D\u5EA6\uFF09</div></div>
  </div>
</div></header>

<section id="demo"><div class="wrap">
  <div class="sec-head"><span class="idx">01</span><h2>\u5728\u7EBF\u6F14\u793A</h2><span class="tag">GET \xB7 JSON</span></div>
  <div class="demo">
    <div class="demo-tabs">
      <div class="demo-tab on" data-t="kline">kline</div>
      <div class="demo-tab" data-t="quote">quote</div>
      <div class="demo-tab" data-t="universe">universe</div>
    </div>
    <div class="demo-body">
      <div class="demo-form">
        <div class="field" data-f="kline"><label>symbol</label><input id="sym" value="AAPL" spellcheck="false"></div>
        <div class="field" data-f="kline"><label>interval</label>
          <select id="itv">
            <option value="1d" selected>1d \xB7 \u65E5\u7EBF</option>
            <option value="1h">1h \xB7 1\u5C0F\u65F6</option>
            <option value="30m">30m \xB7 \u534A\u5C0F\u65F6</option>
            <option value="15m">15m \xB7 15\u5206\u949F</option>
            <option value="5m">5m \xB7 5\u5206\u949F</option>
            <option value="1m">1m \xB7 1\u5206\u949F</option>
          </select>
        </div>
        <div class="field" data-f="kline"><label>limit</label><input id="lim" value="10" type="number" min="1"></div>
        <div class="field" data-f="quote" style="display:none"><label>symbol</label><input id="qsym" value="0700.HK" spellcheck="false"></div>
        <div class="field" data-f="universe" style="display:none"><label>index</label>
          <select id="uidx">
            <option value="csi300" selected>csi300 \xB7 \u6CAA\u6DF1300</option>
            <option value="csi500">csi500 \xB7 \u4E2D\u8BC1500</option>
            <option value="nasdaq100">nasdaq100 \xB7 \u7EB3\u6307100</option>
            <option value="sp500">sp500 \xB7 \u6807\u666E500</option>
            <option value="hsi">hsi \xB7 \u6052\u751F\u6307\u6570</option>
          </select>
        </div>
        <button class="run" id="go">\u8FD0\u884C\u8BF7\u6C42</button>
      </div>
      <div class="demo-out"><pre id="out"><span class="dim">// \u5728\u5DE6\u4FA7\u8F93\u5165\u53C2\u6570\uFF0C\u70B9\u51FB\u300C\u8FD0\u884C\u8BF7\u6C42\u300D\u67E5\u770B\u8FD4\u56DE\u7ED3\u679C\u3002&#10;// kline\uFF1AAAPL / 0700.HK / 600519.SS / 000001.SZ&#10;// quote\uFF1A\u83B7\u53D6\u4E2A\u80A1\u540D\u79F0\u3001\u884C\u4E1A\u3001\u5E02\u503C\u3001\u6700\u65B0\u4EF7\u7B49&#10;// universe\uFF1A\u83B7\u53D6\u6307\u6570\u6210\u5206\u80A1\u4EE3\u7801\u6E05\u5355</span></pre></div>
    </div>
  </div>
</div></section>

<section id="endpoints"><div class="wrap">
  <div class="sec-head"><span class="idx">02</span><h2>\u63A5\u53E3\u4E00\u89C8</h2></div>
  <div class="ep-grid">
    <div class="ep"><div class="m">GET /kline</div><div class="d">K\u7EBF\u6570\u636E\uFF08\u65E5K / 1m / 5m / 15m / 30m / 1h\uFF09</div><div class="ex">/kline?symbol=AAPL&amp;interval=1d&amp;limit=5</div></div>
    <div class="ep"><div class="m">GET /quote</div><div class="d">\u4E2A\u80A1\u5143\u6570\u636E\uFF08\u540D\u79F0/\u884C\u4E1A/\u5E02\u503C/\u6700\u65B0\u4EF7/52\u5468\u9AD8\u4F4E\u2026\uFF09</div><div class="ex">/quote?symbol=600519.SS</div></div>
    <div class="ep"><div class="m">GET /universe</div><div class="d">\u6307\u6570\u6210\u5206\u80A1\u6E05\u5355\uFF08csi300/csi500/nasdaq100/sp500/hsi\uFF09</div><div class="ex">/universe?index=csi300</div></div>
    <div class="ep"><div class="m">GET /indices</div><div class="d">\u5168\u90E8\u53EF\u7528\u6307\u6570/\u6E05\u5355\u53CA\u5176\u6210\u5206\u6570\u91CF</div><div class="ex">/indices</div></div>
    <div class="ep"><div class="m">GET /symbols</div><div class="d">\u6309\u533A\u57DF\u5217\u51FA\u80A1\u7968\u4EE3\u7801\uFF08\u652F\u6301\u5206\u9875\uFF09</div><div class="ex">/symbols?region=cn&amp;limit=10</div></div>
    <div class="ep"><div class="m">GET /status</div><div class="d">\u670D\u52A1\u914D\u7F6E\u4FE1\u606F\uFF08\u533A\u95F4/\u533A\u57DF/\u6307\u6570\uFF09</div><div class="ex">/status</div></div>
  </div>
</div></section>

<section id="api"><div class="wrap">
  <div class="sec-head"><span class="idx">03</span><h2>API \u6587\u6863</h2></div>
  <div class="table-wrap">
    <table>
      <tr><th>\u53C2\u6570</th><th>\u5FC5\u586B</th><th>\u9ED8\u8BA4</th><th>\u8BF4\u660E</th></tr>
      <tr><td><code>symbol</code></td><td><span class="req">\u662F</span></td><td>\u2014</td><td>\u80A1\u7968\u4EE3\u7801\uFF1A<code>AAPL</code> / <code>0700.HK</code> / <code>600519.SS</code> / <code>000001.SZ</code></td></tr>
      <tr><td><code>interval</code></td><td><span class="opt">\u5426</span></td><td><code>1d</code></td><td>\u5468\u671F\uFF1A<code>1d</code>(\u65E5\u7EBF) <code>1m</code>(1\u5206\u949F) <code>5m</code>(5\u5206\u949F) <code>15m</code>(15\u5206\u949F) <code>30m</code>(\u534A\u5C0F\u65F6) <code>1h</code>(1\u5C0F\u65F6)</td></tr>
      <tr><td><code>start</code></td><td><span class="opt">\u5426</span></td><td>\u2014</td><td>\u8D77\u59CB\u65E5\u671F <code>YYYY-MM-DD</code>\uFF08\u542B\uFF09</td></tr>
      <tr><td><code>end</code></td><td><span class="opt">\u5426</span></td><td>\u2014</td><td>\u7ED3\u675F\u65E5\u671F <code>YYYY-MM-DD</code>\uFF08\u542B\uFF09</td></tr>
      <tr><td><code>limit</code></td><td><span class="opt">\u5426</span></td><td>\u5168\u90E8</td><td>\u6700\u591A\u8FD4\u56DE\u884C\u6570\uFF1B\u9ED8\u8BA4\u8FD4\u56DE\u6700\u65B0 N \u6761</td></tr>
      <tr><td><code>order</code></td><td><span class="opt">\u5426</span></td><td><code>asc</code></td><td><code>asc</code> \u65F6\u95F4\u5347\u5E8F / <code>desc</code> \u6700\u65B0\u5728\u524D</td></tr>
      <tr><td><code>format</code></td><td><span class="opt">\u5426</span></td><td><code>json</code></td><td><code>json</code> / <code>csv</code>\uFF08\u8FD4\u56DE\u539F\u59CB CSV \u6587\u672C\uFF09</td></tr>
    </table>
  </div>
  <p style="margin-top:14px;font-size:13px;color:var(--muted)">\u533A\u57DF\u81EA\u52A8\u8BC6\u522B\uFF1A\u88F8\u4EE3\u7801\u2192\u7F8E\u80A1\uFF0C<code>.HK</code>\u2192\u6E2F\u80A1\uFF0C<code>.SS/.SZ</code>\u2192A\u80A1\uFF0C<code>.KS/.KQ</code>\u2192\u97E9\u80A1\u3002\u4E5F\u53EF\u7528 <code>region</code> \u53C2\u6570\u663E\u5F0F\u6307\u5B9A\u3002</p>
</div></section>

<section id="fields"><div class="wrap">
  <div class="sec-head"><span class="idx">04</span><h2>\u6570\u636E\u5B57\u6BB5</h2></div>
  <div class="fields">
    <div class="fcard">
      <h3>\u65E5\u7EBF <span class="tag">interval=1d</span></h3>
      <ul>
        <li><span class="k">Date</span><span class="d">\u4EA4\u6613\u65E5\u671F YYYY-MM-DD</span></li>
        <li><span class="k">Open</span><span class="d">\u5F00\u76D8\u4EF7</span></li>
        <li><span class="k">High</span><span class="d">\u6700\u9AD8\u4EF7</span></li>
        <li><span class="k">Low</span><span class="d">\u6700\u4F4E\u4EF7</span></li>
        <li><span class="k">Close</span><span class="d">\u6536\u76D8\u4EF7</span></li>
        <li><span class="k">Adj Close</span><span class="d">\u590D\u6743\u6536\u76D8\u4EF7\uFF08\u542B\u9664\u6743\u9664\u606F\u8C03\u6574\uFF09</span></li>
        <li><span class="k">Volume</span><span class="d">\u6210\u4EA4\u91CF\uFF08\u80A1\uFF09</span></li>
      </ul>
    </div>
    <div class="fcard">
      <h3>\u5206\u949F\u7EBF <span class="tag">interval=1m / 5m / 15m / 30m / 1h</span></h3>
      <ul>
        <li><span class="k">Datetime</span><span class="d">\u65F6\u95F4\u6233\uFF08\u7CBE\u786E\u5230\u5206\u949F\uFF09</span></li>
        <li><span class="k">Open</span><span class="d">\u5F00\u76D8\u4EF7</span></li>
        <li><span class="k">High</span><span class="d">\u6700\u9AD8\u4EF7</span></li>
        <li><span class="k">Low</span><span class="d">\u6700\u4F4E\u4EF7</span></li>
        <li><span class="k">Close</span><span class="d">\u6536\u76D8\u4EF7</span></li>
        <li><span class="k">Adj Close</span><span class="d">\u590D\u6743\u6536\u76D8\u4EF7</span></li>
        <li><span class="k">Volume</span><span class="d">\u6210\u4EA4\u91CF\uFF08\u80A1\uFF09</span></li>
      </ul>
      <p style="margin-top:12px;font-size:12px;color:var(--dim);border-top:1px dashed var(--line);padding-top:10px">5m / 15m / 30m \u7531\u91C7\u96C6\u7AEF\u7528 1m \u6570\u636E\u91CD\u91C7\u6837\u8BA1\u7B97\uFF08Open \u9996 / High \u6700\u9AD8 / Low \u6700\u4F4E / Close \u672B / Volume \u6C42\u548C\uFF09\u3002</p>
    </div>
  </div>
</div></section>

<section id="examples"><div class="wrap">
  <div class="sec-head"><span class="idx">05</span><h2>\u793A\u4F8B</h2></div>
  <div class="codes">
    <div class="code"><span class="cmt"># \u6700\u8FD1 5 \u6761\u65E5K\uFF08\u9ED8\u8BA4\u5347\u5E8F\uFF0Climit \u53D6\u6700\u65B0\uFF09</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=MSFT&amp;interval=1d&amp;limit=5</span>"</div>
    <div class="code"><span class="cmt"># \u6307\u5B9A\u65E5\u671F\u533A\u95F4 + \u6700\u65B0\u5728\u524D</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=600519.SS&amp;start=2025-01-01&amp;end=2025-12-31&amp;order=desc</span>"</div>
    <div class="code"><span class="cmt"># \u6E2F\u80A1 1 \u5C0F\u65F6K\u7EBF\uFF0C\u8FD4\u56DE CSV</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/kline?symbol=9988.HK&amp;interval=1h&amp;limit=100&amp;format=csv</span>"</div>
    <div class="code"><span class="cmt"># \u4E2A\u80A1\u4FE1\u606F / \u6307\u6570\u6210\u5206\u80A1 / \u533A\u57DF\u4EE3\u7801\u5206\u9875</span><br><span class="cmd">curl</span> "<span class="url">${API_BASE}/quote?symbol=0700.HK</span>" &nbsp; <span class="cmd">curl</span> "<span class="url">${API_BASE}/universe?index=sp500</span>" &nbsp; <span class="cmd">curl</span> "<span class="url">${API_BASE}/symbols?region=us&amp;limit=5</span>"</div>
  </div>
</div></section>

<footer><div class="wrap foot">
  <span>StockAPI \xB7 \u6570\u636E\u7531 GitHub Actions \u81EA\u52A8\u91C7\u96C6\uFF0C\u7ECF\u7531 Cloudflare Workers \u5206\u53D1</span>
  <span>\u6570\u636E\u6765\u81EA Yahoo Finance\uFF0C\u4EC5\u4F9B\u5B66\u4E60\u7814\u7A76</span>
</div></footer>

<script>
(function(){
  var out=document.getElementById("out");
  var tabs=document.querySelectorAll(".demo-tab");
  var active="kline";
  function esc(s){ return s.replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
  function setTab(t){
    active=t;
    tabs.forEach(function(tb){ tb.classList.toggle("on", tb.getAttribute("data-t")===t); });
    document.querySelectorAll(".demo-form .field").forEach(function(f){
      f.style.display = f.getAttribute("data-f")===t ? "" : "none";
    });
  }
  tabs.forEach(function(tb){ tb.addEventListener("click", function(){ setTab(tb.getAttribute("data-t")); }); });
  function run(){
    var q;
    if(active==="quote"){
      q="/quote?symbol="+encodeURIComponent(document.getElementById("qsym").value.trim()||"0700.HK");
    }else if(active==="universe"){
      q="/universe?index="+encodeURIComponent(document.getElementById("uidx").value);
    }else{
      var s=document.getElementById("sym").value.trim()||"AAPL";
      var i=document.getElementById("itv").value;
      var l=document.getElementById("lim").value||"10";
      q="/kline?symbol="+encodeURIComponent(s)+"&interval="+i+"&limit="+l;
    }
    out.innerHTML="<span class=\\"dim\\">// GET "+esc(q)+"</span>\\n";
    fetch(q).then(function(r){
      if(!r.ok){ throw new Error("HTTP "+r.status); }
      return r.json();
    }).then(function(d){
      var html="<span class=\\"ok\\">// "+esc(q)+" \u2192 "+d.count+"</span>\\n";
      html+=JSON.stringify(d,null,2);
      out.innerHTML=html;
    }).catch(function(e){
      out.innerHTML="<span class=\\"err\\">// \u8BF7\u6C42\u5931\u8D25\uFF1A"+esc(e.message)+"</span>";
    });
  }
  document.getElementById("go").addEventListener("click",run);
  document.addEventListener("DOMContentLoaded",function(){ setTab("kline"); run(); });
})();
<\/script>
</body>
</html>`;
var src_default = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
    if (path === "/" || path === "") {
      return html(HOME_HTML);
    }
    const params = url.searchParams;
    switch (path) {
      case "/kline":
        return await handleKline(params, env);
      case "/quote":
        return await handleQuote(params, env);
      case "/universe":
        return await handleUniverse(params, env);
      case "/indices":
        return await handleIndices(env);
      case "/symbols":
        return await handleSymbols(params, env);
      case "/status":
        return handleStatus();
      default:
        return error(
          "Not found. Use /, /kline, /quote, /universe, /indices, /symbols, /status",
          404
        );
    }
  }
};
export {
  src_default as default
};
//# sourceMappingURL=index.js.map
