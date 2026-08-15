# Market Data Pipeline

通过 **GitHub Actions 自动采集股票行情数据 → Cloudflare R2 存储 → Cloudflare Worker 提供 API** 的项目。

数据经 **Yahoo chart API + 反代** 拉取，gzip 压缩后存入 **Cloudflare R2**（5G），由 Cloudflare Worker 边缘分发为 JSON 接口，供量化系统调用。

## 核心特性

- **全自动**：GitHub Actions 定时增量同步，无需人工干预
- **全量 + 增量分离**：
  - `Fetch History (Full)`：手动触发一次，全量历史入库（日K 5y / 1m 5d / 1h 6mo / 延长时段）
  - `Sync Data (Incremental)`：**唯一的定时 action**，每 1 小时只同步新增数据
- **区域驱动**：美股 / 沪深A股 / 港股 / 韩股 四个市场，范围来自用户提供的持仓清单
- **多周期**：日K、1m、5m、15m、30m、1h 分开存放（5m/15m/30m 由 1m 重采样派生）
- **延长时段**：美股 1m/5m/15m/30m/1h 均含盘前盘后（4:00–20:00 美东）
- **增量查重**：休市（非交易时段/周末）跳过请求，数据最新则不再拉取，大幅减少 Actions 时间
- **R2 高效入库**：gzip 压缩（约节省 70~80% 容量）+ 多线程并发上传
- **动态接口**：Cloudflare Worker 读取 R2 转 JSON，免费、无需 Key

## 股票范围

| 市场 | 代码 | 来源 | 数量 |
| ---- | ---- | ---- | ---- |
| 美股 | `us` | iShares Russell 1000 ETF 持仓（IWB） | 1022 只 |
| 沪深A股 | `cn` | 沪市 GPLIST + 深市 A股列表（全市场） | 4595 只 |
| 港股 | `hk` | 恒生指数全部成分股 | 88 只 |
| 韩股 | `kr` | KOSPI 200 前 50 核心成分股 | 48 只 |

> 清单文件位于 `data/universe/{region}.csv`，由 `scripts/build_universe.py` 生成后提交到仓库。

## 数据流架构

```
GitHub Actions（采集）
  ├─ Fetch History (Full)   ← 手动触发 · 一次性全量历史
  └─ Sync Data (Incremental) ← 每 1 小时 · 增量同步
        │  Yahoo chart API + 反代 → gzip → 并发上传
        ▼
Cloudflare R2（5G 存储）
  ├─ universe/{region}.csv              # 清单（不压缩）
  ├─ {region}/kline/{symbol}.csv.gz     # 日K
  ├─ {region}/kline_1m/{symbol}.csv.gz  # 1分钟K
  ├─ {region}/kline_5m/{symbol}.csv.gz  # 5分钟K（派生）
  ├─ {region}/kline_15m/{symbol}.csv.gz
  ├─ {region}/kline_30m/{symbol}.csv.gz
  ├─ {region}/kline_1h/{symbol}.csv.gz  # 1小时K
  └─ _status.json                       # 采集状态
        │
        ▼
Cloudflare Worker（stockapi.365200.xyz）
  /kline /quote /universe /indices /symbols /status
```

## 目录结构

```
.
├── config.py                     # 区域、股票范围、反代、交易时段配置
├── requirements.txt              # Python 依赖
├── scripts/
│   ├── fetch_history.py          # 批量历史全量入库 R2（手动）
│   ├── sync_incremental.py       # 增量同步入库 R2（定时，唯一 action）
│   ├── r2store.py                # Cloudflare R2 存储客户端（gzip + 并发）
│   ├── yahoo_chart.py            # Yahoo chart API 客户端（经反代访问，支持延长时段）
│   ├── build_universe.py         # 从本地文件生成各区域股票清单
│   ├── fetch_universe.py         # 指数成分股清单拉取（恒生指数等）
│   └── marketlib.py              # 共享工具（列表解析 + 分批 + 交易时段判断）
├── api/                          # Cloudflare Worker 动态接口
│   ├── src/index.js              # 从 R2 读取（fallback GitHub raw）
│   └── wrangler.toml             # R2 binding 配置
├── API.md                        # API 使用文档
├── SECRETS.md                    # 部署凭据说明（勿提交真实密钥）
└── .github/workflows/
    ├── fetch_history.yml         # 全量历史（手动触发）
    └── sync_data.yml             # 增量同步（定时，每 1 小时）
```

## 配置（首次部署）

### 1. 配置 GitHub Secrets

在仓库 **Settings → Secrets and variables → Actions** 配置：

| Secret | 值 |
| ---- | ---- |
| `R2_ACCOUNT_ID` | Cloudflare 账户 ID（如 `8e43ef...`） |
| `R2_ACCESS_KEY_ID` | R2 S3 API Access Key ID |
| `R2_SECRET_ACCESS_KEY` | R2 S3 API Secret Access Key |
| `R2_BUCKET` | R2 bucket 名（如 `stocksmarkets`） |

### 2. 首次全量入库

在 **Actions** 页手动触发 `Fetch History (Full)`：
- `region=all` 拉取全部市场；大区域自动分批（cn 10 批 / us 4 批）
- 或选单区域先测试

> 首次运行会拉取全部历史（日K 5y、1m 5d、1h 6mo 及延长时段）并写入 R2，之后由增量 action 自动更新。

### 3. 定时增量

`Sync Data (Incremental)` 已配置 **每 1 小时** 定时运行，无需操作。

## 本地运行

```bash
pip install -r requirements.txt

# 配置 R2 凭据（Windows 可用 $env:，Linux 用 export）
export R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... R2_BUCKET=stocksmarkets

# 全量历史入库 R2（--region 可选；--batch/--batches 分批）
python scripts/fetch_history.py --region us

# 增量同步
python scripts/sync_incremental.py --region us
```

## 数据口径说明

- **时间戳按 K 线起始时间标注**：1h 最后一根标为 `15:30`（覆盖 15:30–16:00），`Close` 即 16:00 官方收盘价。
- **美股延长时段**：1m/5m/15m/30m/1h 含盘前盘后（4:00–20:00 美东）；1d 不含。
- **派生周期**：5m/15m/30m 由 1m 重采样（Open 首 / High 最高 / Low 最低 / Close 末 / Volume 求和）。
- **反代访问**：国内直连 Yahoo 被 403，所有请求经 `config.YAHOO_CHART_PROXY`（`https://img2.365200.xyz`）转发。

## API 使用

完整接口文档见 [API.md](API.md)。快速开始：

```bash
curl "https://stockapi.365200.xyz/kline?symbol=AAPL&interval=1d&limit=5"
curl "https://stockapi.365200.xyz/quote?symbol=600519.SS"
curl "https://stockapi.365200.xyz/universe?index=us"
```

## 说明

- 数据仅供学习研究，来自 Yahoo Finance。
- R2 免费 10GB 存储（本项目数据 gzip 后约 1~2GB），Worker 免费 10 万次/天。
- `fetch_history.py` 全量跑完后，`sync_incremental.py` 只拉增量，重复运行不会重复写入。
