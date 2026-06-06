# Data Collectors - Claude 專案指引

## 專案概述

Zeabur 雲端持續資料收集服務，定期抓取各來源資料並寫入 Supabase。

部分 collector 因目標 API 對國際雲商 IP 做封鎖（白名單只放台灣 ISP AS），改在中華電信 HiCloud VM 跑，
單檔鏡像版住在 [`external/`](../external/)。完整 pattern 與 SOP 見 [`docs/EXTERNAL_COLLECTORS.md`](../docs/EXTERNAL_COLLECTORS.md)。
目前外部清單：`ship_ais`（航港局 AIS）。Zeabur 上對應 `*_ENABLED` 一律設 `false`。

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

---

## 開發注意事項

1. **新 collector 預設 ENABLED=false**，避免部署後立即啟動
2. **排程**：無須特別標記，pool 內每個 collector 都獨立一條 thread；耗時 collector 記得設 `COLLECT_TIMEOUT`（只是觀察值，不強制中斷）
3. **SupabaseWriter 是單例**，所有 collector 共用，內部有 `RLock` 保護
4. **Buffer 機制**：DB 寫入失敗會暫存到 `data/buffer/`，每 5 分鐘重試
5. **測試**：本地 `python3 main.py` 可執行，只要有 `.env`
6. **AWS 變動同步**：動到 S3 storage class / lifecycle / prefix 增刪後，**務必更新 [docs/AWS_INVENTORY.md](../docs/AWS_INVENTORY.md)**（避免日後忘記哪些資料在哪個 tier、為何而存）

---

## 相關文件

- [.claude/principles.md](principles.md) — 開發慣例
- [.claude/pitfalls/](pitfalls/) — 踩坑紀錄
- [../gis-platform/docs/data-inventory.md](../../gis-platform/docs/data-inventory.md) — 資料清冊
- [docs/AWS_INVENTORY.md](../docs/AWS_INVENTORY.md) — AWS S3 用量清冊（改 AWS 後必更新）
