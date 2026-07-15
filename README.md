# Data Collectors

> GIS 基礎設施三部曲的**持續收集層**。定期抓取台灣各類地理 / 即時資料寫入 Supabase + S3。

## 三部曲（強綁定）

```
taipei-gis-analytics  →  gis-platform  ←  data-collectors
     探索 & 開發          儲存 & 呈現       持續收集（本專案）
```

| Repo | 路徑 | 職責 |
|------|------|------|
| **taipei-gis-analytics** | `../taipei-gis-analytics/` | 探索開放資料、開發清洗 pipeline、catalog 治理 |
| **gis-platform** | `../gis-platform/` | Supabase（含 schema migrations）+ 前端 + 資料清冊 |
| **data-collectors**（本專案） | `.` | Zeabur + HiCloud VM 上 24/7 收集，寫入共用 Supabase |

**資料清冊唯一真相**：[`../gis-platform/docs/data-inventory.md`](../gis-platform/docs/data-inventory.md)

---

## 部署位置

| 位置 | 跑什麼 | 為什麼 |
|---|---|---|
| **Zeabur（主力）** | 絕大多數 collector（`deployment: zeabur`）+ `tasks/daily_report.py` + `tasks/archive.py` | PaaS 24/7 託管，與 Supabase / S3 同生態圈 |
| **HiCloud VM（HiNet IP）** | 需 Taiwan IP 的 collector（`deployment: hicloud_vm`）：`ship_ais`、`waste_positions`、`cdc_public_health_weekly` | 來源 API（航港局 / 高雄市府 / 台南市府 / 疾管署）對國際雲段 IP 封鎖或 timeout。詳見 [docs/EXTERNAL_COLLECTORS.md](docs/EXTERNAL_COLLECTORS.md) |

> 各位置實際跑哪些 collector 以 [`config/cross_layer_map.yaml`](config/cross_layer_map.yaml) 的 `deployment` 欄位為準（勿在本文件寫死數字）。
| **本機 Mac launchd** | 1 支 audit push（每天 10:00 推 catalog 健康度給 daily_report） | 純監測，不影響線上資料 |

新增需「Taiwan IP」的 collector 走 SOP：[docs/EXTERNAL_COLLECTORS.md](docs/EXTERNAL_COLLECTORS.md) §「新增外部 collector」。

---

## 收集器總覽

所有 collector 都可獨立啟停。**完整清單 + 預設狀態以 [`config/cross_layer_map.yaml`](config/cross_layer_map.yaml) 與 `config._COLLECTOR_TOGGLES` 為準**（勿在本文件寫死數字；兩者由 [`scripts/sync_cross_layer_map.py`](scripts/sync_cross_layer_map.py) 保持同步、`--check` 擋 drift）。

| 類別 | 代表 collector | 部署 |
|---|---|---|
| 交通 | YouBike / Bus / TRA / Freeway VD / Road Event | Zeabur |
| 航運 | **Ship AIS** | **HiCloud VM** |
| 航空 | FR24 / FR24 Zone / OpenSky | Zeabur |
| 氣象 | CWA Weather / Temperature / CWA Satellite | Zeabur |
| 地震 / 災害 | Earthquake / NCDR Alerts | Zeabur |
| 新聞 | News Events（RSS ×29 + Gemini Flash-Lite 地名抽取，LLM 不吐座標） | Zeabur |
| 太空 | Satellite / Launch | Zeabur |
| 環境 | Air Quality / Micro Sensors / AQI Imagery | Zeabur |
| 水利 | Reservoir / River / Rain Gauge / Groundwater / IoT WRA / USWG | Zeabur |
| 環境衛生 | **Waste Positions** / Waste Match | **HiCloud VM** / Zeabur |
| 急診 / 電力 | ER Hospital / Power Taipower | Zeabur |
| 文化 | TPML 圖書館座位 | Zeabur |
| POI | Foursquare | Zeabur |

⚠️ 粗體 = 跑在 HiCloud VM（[`external/ship_ais_vm/`](external/ship_ais_vm/) + [`external/waste_positions_vm/`](external/waste_positions_vm/)）。

每個 collector 的 enabled / interval / Supabase 表 / S3 prefix 真相在：
- [`config/cross_layer_map.yaml`](config/cross_layer_map.yaml) — collector → 各層對應
- [`config/realtime_tables.yaml`](config/realtime_tables.yaml) — Supabase 50 表清冊

---

## 執行架構（Zeabur 端）

### CollectorScheduler

所有 collector 走統一 `ThreadPoolExecutor`（`scheduler.py`）：

- **獨立 thread**：互不阻塞
- **Skip-if-running**：同一 collector 還沒跑完，下個 tick 自動跳過
- **`schedule` 套件只觸發**，實際執行交給 pool
- **Pool 大小**：`max(10, collector 數 + 2)`

### Supabase 旁路寫入

每次 collector 結束後：

1. 寫本地檔案 (`LocalStorage`)
2. 旁路推 `SupabaseWriter`（PostGIS）
3. DB 失敗會 buffer 到 `data/buffer/*.json`，每 5 分鐘 flush

### 每日歸檔

`tasks/archive.py` 每天 03:00 把昨天的本地 JSON 打包成 `tar.gz` 上 S3，刪本地。
S3 設 lifecycle：30 天 → IA、90 天 → Glacier IR（見 [docs/AWS_INVENTORY.md](docs/AWS_INVENTORY.md)）。

### 航空三源互補

| Collector | 角色 | 飛機數/輪 | 優勢 | 缺失 |
|---|---|---|---|---|
| Flight FR24 | 台灣起降航班 | ~20-50 | 完整 trail 軌跡 | 只追台灣機場 |
| FR24 Zone | 空域快照 | ~120 | 最多 + 有 origin/dest | 無軌跡 |
| OpenSky | 空域快照 | ~65 | 精確高度 / 垂直速率 | 無 origin/dest |

---

## 監控系統

> **單一 Telegram 日報蓋全部**。每天看一則訊息知道整套基礎設施健康度。

詳細架構、yaml 維護、災難復原見 **[docs/MONITORING.md](docs/MONITORING.md)**。摘要：

`tasks/daily_report.py` 12 段：

1. Collector 心跳
2. **Supabase 50 表寫入新鮮度**（RPC `realtime.health_snapshot()` 一次撈）
3. **S3 archives 每 collector 心跳**（解 silent fail）
4. **跨層一致性**（collector × SB × S3 三向交叉診斷）
5. **HiCloud VM 健康**（VM cron 07:00 推 snapshot 到 S3）
6. **異常 7 天趨勢**（D1/D3/D7 去重）
7-10. 檔案 / S3 / 歸檔 / 系統
11. **成本趨勢**（CloudWatch 月增 > 20% 警告）
12. **今日 Action**（規則式 1-3 件最該做的事）

範例輸出 + 故障排除見 [docs/MONITORING.md](docs/MONITORING.md)。

---

## 專案結構

```
data-collectors/
├── README.md                       # ← 你正在看
├── requirements.txt
├── Dockerfile                      # Zeabur 用
├── zeabur.json
├── .env.example
│
├── main.py                         # Zeabur 主入口（scheduler 啟動）
├── scheduler.py                    # CollectorScheduler（ThreadPoolExecutor）
├── config.py                       # _COLLECTOR_TOGGLES + 環境變數
│
├── collectors/                     # 所有 collector（清單見 config/cross_layer_map.yaml）
│   ├── base.py                     # BaseCollector
│   ├── registry.py                 # COLLECTOR_REGISTRY（單一真相）
│   ├── ship_ais.py                 # ⚠️ schema 真相，實際跑在 external/ship_ais_vm/
│   ├── waste_positions.py          # ⚠️ 同上
│   └── ...                         # 其他 30+ 個
│
├── storage/
│   ├── local.py                    # 本地檔案儲存（按日期分目錄）
│   ├── s3.py                       # AWS S3 歸檔（每日 tar.gz）
│   ├── supabase_writer.py          # 即時旁路寫入（含 transform）
│   └── supabase_tables.py          # TABLE_MAP（每 collector 寫哪些表）
│
├── tasks/
│   ├── archive.py                  # 每日 03:00 S3 歸檔
│   ├── daily_report.py             # Telegram 日報（12 段）
│   ├── monitoring.py               # ⭐ 監控 helper（yaml + RPC + S3 + anomaly state）
│   └── mini_taipei_publish.py
│
├── config/                         # ⭐ 監控真相來源
│   ├── cross_layer_map.yaml        # collector ↔ SB ↔ S3 對應
│   └── realtime_tables.yaml        # 50 張 Supabase 表清冊
│
├── external/                       # ⭐ HiCloud VM 上跑的鏡像
│   ├── ship_ais_vm/                # 單檔 collector + S3 archiver + setup script
│   ├── waste_positions_vm/         # 同上
│   └── _shared/                    # 跨 collector 共用（health_report.py）
│
├── scripts/                        # 維運 / 一次性腳本
│   ├── local_audit_push.py         # ⭐ 本機 Mac launchd 用，推 audit 到 S3
│   ├── com.gis.local_audit.plist   # launchd 範本
│   ├── backfill_*.py
│   └── seed_*.py
│
├── api/                            # HTTP API（健康檢查 / 下載資料）
│   └── server.py                   # Flask
│
├── utils/
│   ├── auth.py                     # TDX / CWA 認證
│   └── notify.py                   # Telegram / Webhook
│
├── tests/                          # pytest 單元測試
│
├── data/                           # 本地資料（.gitignore，含 buffer/）
│
└── docs/
    ├── MONITORING.md               # ⭐ 監控系統完整文件
    ├── EXTERNAL_COLLECTORS.md      # ⭐ HiCloud VM SOP
    ├── ARCHITECTURE.md             # Phase 1+ 架構
    ├── API.md                      # HTTP API
    ├── AWS_INVENTORY.md            # S3 bucket + lifecycle
    ├── S3_SETUP.md                 # S3 設定步驟
    ├── TDX_RATE_LIMITING.md        # TDX 全域節流器
    ├── BUS_NIGHT_PAUSE.md          # bus collector 夜間策略分析
    ├── sql/                        # 預聚合 matview RPC
    └── _archive/                   # 已完成計畫 / 研究紀錄
```

---

## 快速開始

### 本地開發

```bash
pip3 install -r requirements.txt
cp .env.example .env
# 編輯 .env（至少填 TDX_CLIENT_ID / TDX_CLIENT_SECRET / CWA_API_KEY）
python3 main.py
```

### 部署到 Zeabur

Push 到 main 自動部署。需設定的環境變數見 [`.env.example`](.env.example) 與下方「環境變數」。

### 部署外部 VM collector

新增「需 Taiwan IP 的 collector」依 [`docs/EXTERNAL_COLLECTORS.md`](docs/EXTERNAL_COLLECTORS.md) §「新增外部 collector」走，現有兩個 collector 的災難復原見：

- [`external/ship_ais_vm/README.md`](external/ship_ais_vm/README.md)
- [`external/waste_positions_vm/README.md`](external/waste_positions_vm/README.md)
- [`external/_shared/README.md`](external/_shared/README.md)（health snapshot）

### 安裝本機 audit push

```bash
mkdir -p ~/.config
# 編輯 ~/.config/.gis-audit-env 填 S3_BUCKET / S3_REGION / S3_ACCESS_KEY / S3_SECRET_KEY（chmod 600）

GIS_REPO_PATH=$(pwd)
sed -e "s|\${USER}|$USER|g" -e "s|\${GIS_REPO_PATH}|$GIS_REPO_PATH|g" \
    scripts/com.gis.local_audit.plist \
    > ~/Library/LaunchAgents/com.gis.local_audit.plist
launchctl load ~/Library/LaunchAgents/com.gis.local_audit.plist
```

### 套用新 migration

跨 repo（`gis-platform`）：

```bash
psql "$SUPABASE_DB_URL" -f ../gis-platform/migrations/NNN_xxx.sql
```

---

## 環境變數

完整清單見 [`.env.example`](.env.example)。**最常用的 14 個**：

### 必填（最小可用）

| 變數 | 用途 |
|---|---|
| `TDX_CLIENT_ID` / `TDX_CLIENT_SECRET` | TDX OAuth2 |
| `CWA_API_KEY` | 中央氣象署 |

### 強烈推薦

| 變數 | 用途 |
|---|---|
| `SUPABASE_ENABLED=true` + `SUPABASE_DB_URL` | 即時旁路寫 PostGIS |
| `S3_BUCKET` + `S3_ACCESS_KEY` + `S3_SECRET_KEY` + `S3_REGION` | 歸檔冷儲存 |
| `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` | 日報送達 |
| `DAILY_REPORT_ENABLED=true` + `DAILY_REPORT_TIME=10:30` | 啟用日報 |
| `INSTANCE_NAME` | 多實例 deploy 區分（顯示在日報 header） |

### 個別 collector

`<NAME>_ENABLED=true|false` 啟停、`<NAME>_INTERVAL=N` 改頻率。預設見 `config.py` 的 `_COLLECTOR_TOGGLES`。

> **HiCloud VM collector** (`SHIP_AIS_ENABLED` / `WASTE_POSITIONS_ENABLED`) **必須在 Zeabur 維持 `false`**，否則會與 VM 雙跑 → append-only 表雙 INSERT 污染資料。

---

## 開發新收集器

照 [`.claude/CLAUDE.md`](.claude/CLAUDE.md) 「新增 Collector 的必要步驟」走（10 步、跨 3 repo）。

關鍵步驟摘要：

1. `gis-platform/migrations/NNN_xxx.sql` 建表
2. `collectors/xxx.py` 寫 collector（繼承 BaseCollector）
3. `collectors/registry.py` 加 entry
4. `config.py` 的 `_COLLECTOR_TOGGLES` 加一筆
5. `storage/supabase_tables.py` 加 `TABLE_MAP[xxx]`
6. `storage/supabase_writer.py` 加 `_transform_xxx`
7. **`config/cross_layer_map.yaml` 加 entry**（漏寫日報會漏報）— 可跑 `python3 scripts/sync_cross_layer_map.py` 自動從 config + TABLE_MAP 回填缺的 entry（再人工複核 TODO 欄位）；`--check` 會擋 drift，`tests/test_cross_layer_sync.py` 已納入測試
8. **`config/realtime_tables.yaml` 加表條目**（漏寫 RPC 撈不到）
9. `gis-platform/docs/data-inventory.md` 更新清冊（含部署位置欄）
10. `taipei-gis-analytics/docs/data-sources.md` 更新

需要 HiCloud VM 出口：步驟 4 預設 `enabled=False`、步驟 7 `deployment: hicloud_vm`，並補 `external/<name>_vm/`。

---

## HTTP API

[詳細 API 文件 → docs/API.md](docs/API.md)

```bash
# 健康檢查
curl https://<your-app>.zeabur.app/health

# 列出 collector
curl -H "X-API-Key: $API_KEY" https://<your-app>.zeabur.app/api/collectors

# 取最新資料
curl -H "X-API-Key: $API_KEY" "https://<your-app>.zeabur.app/api/data/youbike?date=latest"
```

---

## 文件導覽

| 文件 | 用途 |
|---|---|
| [`docs/MONITORING.md`](docs/MONITORING.md) | ⭐ 日報架構 / yaml 維護 / 災難復原 |
| [`docs/EXTERNAL_COLLECTORS.md`](docs/EXTERNAL_COLLECTORS.md) | ⭐ HiCloud VM pattern + 新增 SOP |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | scheduler + storage 設計 |
| [`docs/API.md`](docs/API.md) | HTTP API 規格 |
| [`docs/AWS_INVENTORY.md`](docs/AWS_INVENTORY.md) | S3 bucket / lifecycle / 用量 |
| [`docs/S3_SETUP.md`](docs/S3_SETUP.md) | S3 設定步驟 |
| [`docs/TDX_RATE_LIMITING.md`](docs/TDX_RATE_LIMITING.md) | TDX 全域節流器設計 |
| [`docs/BUS_NIGHT_PAUSE.md`](docs/BUS_NIGHT_PAUSE.md) | bus 夜間策略分析 |
| [`docs/sql/`](docs/sql/) | 預聚合 matview RPC |
| [`docs/_archive/`](docs/_archive/) | 已完成計畫 / 歷史研究 |
| [`.claude/CLAUDE.md`](.claude/CLAUDE.md) | 開發 SOP（新增 collector 10 步） |
| [`.claude/principles.md`](.claude/principles.md) | 程式碼慣例 |
| [`.claude/pitfalls/`](.claude/pitfalls/) | 踩坑紀錄 |

---

## 授權

Internal use only.
