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

每次新增一個 collector，**必須同步修改以下檔案**：

| 步驟 | Repo | 檔案 | 說明 |
|------|------|------|------|
| 1 | gis-platform | `migrations/NNN_{name}.sql` | 建 Supabase 表 |
| 2 | data-collectors | `collectors/{name}.py` | Collector 實作（繼承 BaseCollector） |
| 3 | data-collectors | `collectors/__init__.py` | 註冊 import + __all__ |
| 4 | data-collectors | `config.py` | `{NAME}_ENABLED` + `{NAME}_INTERVAL` + API keys |
| 5 | data-collectors | `main.py` | 排程註冊區段 + BACKGROUND_COLLECTORS（若耗時） |
| 6 | data-collectors | `storage/supabase_writer.py` | `_transform_{name}` + `TRANSFORMERS` + `TABLE_MAP` |
| 7 | gis-platform | `docs/data-inventory.md` | 更新資料清冊 |
| 8 | taipei-gis-analytics | `docs/data-sources.md` | 更新 pipeline 狀態 |

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

### 排程模型

| 類型 | 方式 | 範例 |
|------|------|------|
| 前景（預設） | 主 thread `schedule` 庫輪詢 | youbike, bus, weather |
| 背景 | 獨立 daemon thread | flight_fr24, foursquare_poi, satellite |

**耗時超過 1 分鐘的 collector 必須放到 `BACKGROUND_COLLECTORS`**，否則會阻塞公車等高頻 collector。

### Supabase 寫入流程

```
collect() → BaseCollector.run() → supabase_writer.write()
                                    ├─ _transform_{name}()  → records
                                    └─ _write_to_db()       → INSERT/UPSERT
                                         ├─ TABLE_MAP['history']  → 歷史分區表
                                         └─ TABLE_MAP['current']  → 最新狀態表
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
2. **背景 thread**：長時間執行的 collector 必須加入 `BACKGROUND_COLLECTORS`
3. **SupabaseWriter 是單例**，所有 collector 共用，內部有 `RLock` 保護
4. **Buffer 機制**：DB 寫入失敗會暫存到 `data/buffer/`，每 5 分鐘重試
5. **測試**：本地 `python3 main.py` 可執行，只要有 `.env`

---

## 相關文件

- [.claude/principles.md](principles.md) — 開發慣例
- [.claude/pitfalls/](pitfalls/) — 踩坑紀錄
- [../gis-platform/docs/data-inventory.md](../../gis-platform/docs/data-inventory.md) — 資料清冊
