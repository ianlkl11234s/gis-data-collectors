# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.

---

> **與上方 Karpathy 4 條的優先級**：以下為本專案具體化規則。遇衝突時，下方規則 override 上方通則（特別是 §2 Simplicity 對「強制多步驟」、§3 Surgical 對「跨檔修改」的例外）。不確定算不算違反專案鐵則時，先問用戶。

# Data Collectors - Claude 專案指引

## 專案概述

Zeabur 雲端持續資料收集服務，定期抓取各來源資料並寫入 Supabase。

## GIS 基礎設施三部曲（強綁定）

本專案是 GIS 基礎設施的**持續收集層**，與另外兩個 repo 緊密連動：

```
taipei-gis-analytics  →  gis-platform  ←  data-collectors
     探索 & 開發          儲存 & 呈現       持續收集（本專案）
```

| Repo | 路徑 | 職責 |
|------|------|------|
| **taipei-gis-analytics** | `../taipei-gis-analytics/` | 探索開放資料、開發清洗 pipeline |
| **gis-platform** | `../gis-platform/` | Supabase 資料庫 + 前端 + 資料清冊 |
| **data-collectors**（本專案） | `.` | Zeabur 雲端持續收集，寫入 Supabase |

### 資料清冊（唯一真相來源）

**`../gis-platform/docs/data-inventory.md`** 是所有資料來源的唯一清冊。

### 新增 Collector 的必要步驟（跨 repo）

重構後（registry + config dataclass + TABLE_MAP 拆出），每次新增需改以下檔案：

| 步驟 | Repo | 檔案 | 說明 |
|------|------|------|------|
| 1 | gis-platform | `migrations/NNN_{name}.sql` | 建 Supabase 表 |
| 2 | data-collectors | `collectors/{name}.py` | Collector 實作（繼承 BaseCollector） |
| 3 | data-collectors | `collectors/registry.py` | `COLLECTOR_REGISTRY` 加一筆（class + display_name + config_prefix + required_env） |
| 4 | data-collectors | `config.py` | `_COLLECTOR_TOGGLES` 加一筆 (prefix, enabled_default, interval_default)；如需額外變數（CITIES/AIRPORTS/API Key）另外宣告 |
| 5 | data-collectors | `storage/supabase_tables.py` | 若需寫 Supabase，加一筆 `TABLE_MAP[name]` |
| 6 | data-collectors | `storage/supabase_writer.py` | 若需寫 Supabase，加 `_transform_{name}` 並註冊到 `TRANSFORMERS` |
| 7 | data-collectors | `config/cross_layer_map.yaml` | **新增 collector 條目**（enabled / deployment / supabase_tables / s3_prefixes / critical）— 漏寫日報會漏報 |
| 8 | data-collectors | `config/realtime_tables.yaml` | **新增所有 Supabase 表條目**（每張 history / current / multi-table 都要列）— 漏寫 RPC 撈不到 |
| 9 | gis-platform | `docs/data-inventory.md` | 更新資料清冊（含 `部署位置` 欄：Zeabur / HiCloud VM / Disabled） |
| 10 | taipei-gis-analytics | `docs/data-sources.md` | 更新 pipeline 狀態 |

> 步驟 7 + 8 是監控系統（daily_report）的真相來源，漏寫不會立刻爆炸但會默默漏報，加新 collector 時請務必同步。

> 若新 collector 因目標 API 對國際雲商 IP 封鎖需走 HiCloud VM：
> - 步驟 4 的 `_COLLECTOR_TOGGLES` 預設 `enabled=False`
> - 步驟 7 的 `deployment: hicloud_vm`
> - 額外加 `external/{name}_vm/` 鏡像實作（見 [docs/EXTERNAL_COLLECTORS.md](../docs/EXTERNAL_COLLECTORS.md) SOP）

Registry + toggle list 自動處理：
- `config.XXX_ENABLED` / `config.XXX_INTERVAL` 變數（來自 `_COLLECTOR_TOGGLES`）
- `collectors/__init__.py` 的 import / `__all__`（來自 `COLLECTOR_REGISTRY`）
- `main.py` 的啟動迴圈（走 registry）

**Transform 邏輯仍在 `storage/supabase_writer.py` 內**（下放到 collector 尚未實作）。

**漏掉任何一步都會造成斷鏈。**

---

## 常見踩雷（新增 collector 前必讀）

- **TDX 必用 `TDXSession`**，不要 `requests.Session`，否則 429（5 req/sec/金鑰）— ref: `reference_tdx_api_limits.md`
- **`required_env` 必須在 `config.py` 用 `os.getenv` 宣告**，否則 `main.py` 會 silent skip（USWG 燒一小時的雷）— ref: `feedback_collector_required_env_silent_skip.md`
- **Zeabur env 變更後必須 restart service** 才生效（程式啟動才讀 env）— ref: `reference_zeabur_env_needs_restart.md`
- **部分 Zeabur project 出口 IP 被高雄/台南 SOA WAF 擋**（100% timeout 穩定發生）— 換 project 才解，retry 救不回 — ref: `feedback_zeabur_ip_blocked_by_gov.md`
- **政府憑證缺 SKI** → requests 用 `verify=False`（NHI ER / 台電核安 / IoW USWG 都踩過）
- **二進位資料走 base64 轉接 Supabase**（CWA imagery 模式）— 不要直接 bytea
- **Supabase retention 與 S3 archive 是兩件獨立的事**：ArchiveTask 03:00 → S3 自動歸檔；Supabase 刪舊資料走 pg_cron `cleanup_*_daily`，要分別設定 — ref: `reference_zeabur_archive_pattern.md`

---

## 架構

### Collector 模式

```python
class MyCollector(BaseCollector):
    name = "my_collector"
    interval_minutes = config.MY_INTERVAL

    def collect(self) -> dict:
        # 抓資料
        return {'data': [...], 'count': N}
```

### 排程模型（Phase 1 後統一）

所有 collector 都交由 `CollectorScheduler`（`scheduler.py`）以 `ThreadPoolExecutor` 平行執行：

- **`schedule` 套件只負責觸發**（cron-like 時間排程）
- **CollectorScheduler 負責執行**：每個 collector 獨立線程、不互相阻塞
- **Skip-if-running**：同一 collector 上一輪還沒跑完，下個 tick 自動 skip 並記錄 warning
- **Pool 大小**：`max(10, collector 數量 + 2)`

舊的「前景主 thread + 背景 daemon thread」雙軌模型、以及 `BACKGROUND_COLLECTORS` 集合已淘汰。耗時的 collector（如 `flight_fr24`, `foursquare_poi`, `satellite`, `cwa_satellite`）會在 pool 內自己一條 thread 跑完，不影響其他人。

### Supabase 寫入流程

```
collect() → BaseCollector.run() → supabase_writer.write()
                                    ├─ _transform_{name}()                          → records
                                    │    （在 SupabaseWriter class 內，TRANSFORMERS dispatch）
                                    └─ _write_to_db()                               → INSERT/UPSERT
                                         ├─ TABLE_MAP['history']  → 歷史分區表
                                         └─ TABLE_MAP['current']  → 最新狀態表
                                           （TABLE_MAP 在 storage/supabase_tables.py）
```

### 特殊 TABLE_MAP 模式

| 模式 | 設定 | 範例 |
|------|------|------|
| 一般（history + current） | `history` + `current` + `current_key` | youbike, bus |
| 只有 history（upsert） | `history` + `upsert_key` | earthquake, foursquare_poi |
| Reference 表 | `is_reference: True` | rail_timetable |
| 多表 | `is_multi_table: True` | freeway_vd, flight_fr24 |

---

## 環境

- **部署**：Zeabur 長駐容器（Python 3.11-slim）
- **連線**：Supavisor transaction pool（port 6543）
- **設定**：環境變數（Zeabur 平台 or `.env`）
- **共用 Supabase**：`utcmcikhvxnohbxchbrs`（同 gis-platform）
- **HiCloud VM 備援**：部分 collector 因目標 API 對國際雲商 IP 封鎖（白名單只放台灣 ISP AS），改在中華電信 HiCloud VM 跑；單檔鏡像版住在 [`external/`](../external/)，完整 pattern 與 SOP 見 [`docs/EXTERNAL_COLLECTORS.md`](../docs/EXTERNAL_COLLECTORS.md)。目前外部清單：`ship_ais`（航港局 AIS）。Zeabur 上對應 `*_ENABLED` 一律設 `false`。

---

## 開發注意事項

0. **新增 collector 屬 Karpathy §2/§3 的合法例外** — 10 步全跑、不要嫌多，跨檔修改是本工作型態的必要複雜度，不是 over-engineering
1. **新 collector 預設 ENABLED=false**，避免部署後立即啟動
2. **排程**：無須特別標記，pool 內每個 collector 都獨立一條 thread；耗時 collector 記得設 `COLLECT_TIMEOUT`（只是觀察值，不強制中斷）
3. **SupabaseWriter 是單例**，所有 collector 共用，內部有 `RLock` 保護
4. **Buffer 機制**：DB 寫入失敗會暫存到 `data/buffer/`，每 5 分鐘重試
5. **測試**：本地 `python3 main.py` 可執行，只要有 `.env`
6. **AWS 變動同步**：動到 S3 storage class / lifecycle / prefix 增刪後，**務必更新 [docs/AWS_INVENTORY.md](../docs/AWS_INVENTORY.md)**（避免日後忘記哪些資料在哪個 tier、為何而存）

---

## 上線前驗收三連

對齊 Karpathy §4「Goal-Driven Execution」— 新 collector 上線前三件事都要綠：

1. **本地跑一輪**：`python3 main.py` 不報錯，看到 collector 被 schedule + collect 成功 log
2. **DB 有新 row**：`select count(*) from {table} where collected_at > now() - interval '1h'` > 0
3. **日報列入**：`daily_report` 排程列出該 collector（步驟 7 `cross_layer_map.yaml` 寫對才會生效）

任何一條沒綠都算未上線。

---

## 相關文件

- [.claude/principles.md](principles.md) — 開發慣例
- [.claude/pitfalls/](pitfalls/) — 踩坑紀錄
- [../gis-platform/docs/data-inventory.md](../../gis-platform/docs/data-inventory.md) — 資料清冊
- [docs/AWS_INVENTORY.md](../docs/AWS_INVENTORY.md) — AWS S3 用量清冊（改 AWS 後必更新）
