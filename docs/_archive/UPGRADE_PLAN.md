# Data Collectors 升級方案 (Phase 1)

> 目標：為即將大量新增的 collector（MOENV 環境資料、各縣市公車等）做好架構準備，
> 確保互相不阻塞、有超時保護、資料收集狀況可追溯。

**狀態：✅ 已完成（2026-04-16 規劃，2026-04-16 落地；後續於 2026-04-23 再做 registry / config / TABLE_MAP 拆出）**
本文件保留為歷史紀錄，勿依此判斷當前架構（以 `README.md` 與 `.claude/CLAUDE.md` 為準）。

最後更新：2026-04-16

---

## 一、現況問題

目前架構有三個結構性瓶頸，隨著 collector 數量增加會惡化：

### 問題 1：主排程單線程，collector 互相阻塞

```python
# main.py:574-576（現況）
while running:
    schedule.run_pending()   # ← schedule 庫是同步的，所有 collector 排隊執行
    time.sleep(1)
```

當 bus collector 跑 30 秒，其他所有 foreground collector 全部等待。

### 問題 2：背景 collector 硬編碼

```python
# main.py:347
BACKGROUND_COLLECTORS = {'flight_fr24', 'foursquare_poi', 'satellite'}
```

新增 collector 要手動決定放前景或背景，規則不清楚。

### 問題 3：無 timeout 保護

`BaseCollector.run()` 的 `collect()` 呼叫沒有外層超時守門員。若 HTTP 請求卡住（TCP hang、SSL 握手慢），整個主線程凍結。

---

## 二、升級目標

1. **每個 collector 獨立線程執行** — 互不阻塞
2. **Skip-if-running 保護** — 同一 collector 上一輪沒跑完，新一輪 skip（不要疊加）
3. **外層 timeout** — 超過 `COLLECT_TIMEOUT` 記 warning，不影響其他 collector
4. **強制 HTTP timeout** — 所有 `requests.get/post` 都帶 `timeout=REQUEST_TIMEOUT`
5. **公車擴充到全台 22 縣市** — 含離島

不在 Phase 1 範圍（之後才做）：
- ❌ Supabase 連線池（維持單連線 + RLock）
- ❌ Watchdog health check
- ❌ 併發 DB 寫入

---

## 三、新架構

```
┌──────────────────────────────────────────────────────┐
│                    main.py                           │
│  ┌────────────────────────────────────────────────┐  │
│  │ schedule 庫（只負責 cron 觸發時機）            │  │
│  │   every(1min).do(scheduler.submit, bus)        │  │
│  │   every(10min).do(scheduler.submit, freeway)   │  │
│  └────────────────────────────────────────────────┘  │
│                    │                                 │
│                    ▼ submit（立刻返回）              │
│  ┌────────────────────────────────────────────────┐  │
│  │ CollectorScheduler (ThreadPoolExecutor)        │  │
│  │   - skip if running                            │  │
│  │   - thread_name = "collector-{name}"           │  │
│  │   - timeout 觀察                               │  │
│  └────────────────────────────────────────────────┘  │
│            │                │                │       │
│            ▼                ▼                ▼       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐            │
│  │ bus.run  │  │weather.. │  │ship_ais.run│          │
│  │ (thread) │  │ (thread) │  │ (thread) │            │
│  └──────────┘  └──────────┘  └──────────┘            │
│            │                │                │       │
│            └────────────────┼────────────────┘       │
│                             ▼                        │
│  ┌────────────────────────────────────────────────┐  │
│  │ SupabaseWriter (單連線 + RLock，Phase 2 再改)  │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

---

## 四、改動清單

### 新檔案

| 檔案 | 用途 |
|------|------|
| `scheduler.py`（新增） | `CollectorScheduler` 類別：ThreadPoolExecutor + skip-if-running |

### 修改檔案

| 檔案 | 改動 |
|------|------|
| `main.py` | 移除 `BACKGROUND_COLLECTORS` 硬編碼；所有 collector 統一透過 scheduler 提交 |
| `collectors/base.py` | 加 `COLLECT_TIMEOUT` class attribute；條件式 `gc.collect()`（>30s 才做） |
| `collectors/bus.py` | 城市迴圈改為 ThreadPoolExecutor 平行抓取（避免 22 城市耗時 40s+） |
| `config.py` | `BUS_CITIES` 預設改為全台 22 縣市；`BUS_INTERVAL` 預設 1 → 2 分鐘；新增 `BUS_FETCH_WORKERS` |
| `.env.example` | 補完 `BUS_*` 環境變數說明 |

---

## 五、公車全台擴充細節

### 5.1 全台 TDX 城市代碼

```python
BUS_CITIES = [
    # 直轄市
    'Taipei', 'NewTaipei', 'Taoyuan', 'Taichung', 'Tainan', 'Kaohsiung',
    # 省轄市
    'Keelung', 'Hsinchu', 'Chiayi',
    # 縣
    'HsinchuCounty', 'MiaoliCounty', 'ChanghuaCounty', 'NantouCounty',
    'YunlinCounty', 'ChiayiCounty', 'PingtungCounty', 'YilanCounty',
    'HualienCounty', 'TaitungCounty',
    # 離島
    'PenghuCounty', 'KinmenCounty', 'LienchiangCounty',
]
# 共 22 個
```

### 5.2 TDX 配額考量

- TDX 免費方案：**50 req/sec，10,000 req/日**
- 22 城市 × 每 2 分鐘 = 每日 15,840 req — **超過免費上限**
- 付費方案（或申請提升）：**1,000,000 req/日** — 充足
- **結論**：預設 `BUS_INTERVAL=2` 分鐘，使用者可依帳號額度調高或調低

### 5.3 城市內平行抓取

即使外層 scheduler 讓多個 collector 平行，bus 單一 collector 內部 22 城市 API 呼叫也要平行化，否則即使獨立線程也要耗時 40-50 秒才收完：

```python
# bus.py 新架構
from concurrent.futures import ThreadPoolExecutor, as_completed

def collect(self) -> dict:
    with ThreadPoolExecutor(max_workers=config.BUS_FETCH_WORKERS) as pool:
        futures = {pool.submit(self._fetch_city, city): city for city in config.BUS_CITIES}
        for future in as_completed(futures):
            city = futures[future]
            try:
                buses = future.result(timeout=config.REQUEST_TIMEOUT + 5)
                # ... 累計
            except Exception as e:
                # 單城錯誤不影響其他城
                pass
```

預設 `BUS_FETCH_WORKERS=5`，避免單 collector 吃太多 TDX rate limit。

---

## 六、風險與緩解

| 風險 | 影響 | 緩解 |
|------|------|------|
| 線程爆炸 | 記憶體 OOM | `ThreadPoolExecutor(max_workers=10)` 上限 |
| Supabase 寫入序列化（Phase 1 保留） | 大量 collector 同時寫會排隊 | Phase 2 改連線池；Phase 1 可接受 |
| 公車 22 城市超出 TDX quota | API 429 | `BUS_INTERVAL=2` 預設；申請付費 key |
| log 交錯難 debug | 追流程變難 | 線程名稱標準化 `collector-{name}` |
| 上線後才發現問題 | 部分 collector 沒寫入 | 保留原 fallback（JSON 本地儲存永遠成功） |

---

## 七、驗證計畫

### 本地測試

1. 只開 `BUS_ENABLED=true`, `WEATHER_ENABLED=true`, `SHIP_AIS_ENABLED=true`
2. 跑 10 分鐘，觀察：
   - Log 是否有 `skip 本次` 警告（代表 skip-if-running 啟動）
   - 每個 collector 的 `run_count` 是否成長
   - 線程名稱是否正確 `collector-bus`、`collector-weather`
3. 故意改某 collector 讓它 sleep 5 分鐘，驗證不會阻塞其他 collector

### Zeabur 上線

1. 先用一個 feature branch 部署
2. 觀察 1-2 天（看第一次每日歸檔是否正常）
3. 確認無問題後 merge 到 main

---

## 八、Phase 2 預告（不在這次做）

以下等 Phase 1 穩定運行 1-2 週後再評估：

| Phase 2 項目 | 前置條件 |
|--------------|----------|
| `SupabaseWriter` 改 `ThreadedConnectionPool` | Phase 1 跑穩後；觀察是否真的有寫入瓶頸 |
| 加 watchdog（偵測線程靜默掛死） | collector 數量 > 25 個後必做 |
| Per-collector metric（每階段耗時） | 需要 metric export 到 Telegram 或 Grafana |
| S3 Lifecycle（冷熱分層） | 獨立於本次升級，任何時候都可以做 |

---

## 九、Phase 1 補充：TDX Rate Limiter（2026-04-16 事後加入）

Phase 1 首次部署後發現 **22 城市公車 17 個被 TDX 429** 擋下。
根本原因：多 collector 共用出口 IP，TDX 對單金鑰 5 req/sec 限制。

**解法**：全域 `TDXSession` + `RateLimiter`（4 req/sec 預設）。

詳細規則與新增 TDX collector 的 checklist：**[docs/TDX_RATE_LIMITING.md](./TDX_RATE_LIMITING.md)** ⭐

> **新增 TDX collector 前必讀該文件**，避免重蹈覆轍。

---

## 九、執行順序

1. [x] 建立 `scheduler.py`
2. [x] 改 `collectors/base.py` 加 timeout class attribute（`COLLECT_TIMEOUT`）
3. [x] 改 `collectors/bus.py` 加城市內部平行抓取
4. [x] 改 `config.py` 擴充 BUS_CITIES、新增 BUS_FETCH_WORKERS
5. [x] 改 `main.py` 移除 BACKGROUND_COLLECTORS、改用 scheduler.submit
6. [x] 改 `.env.example` 補完 BUS_* 說明
7. [x] 本地驗證
8. [x] 部署

## 十、Phase 2（2026-04-23 完成）

Phase 1 之後的延伸重構，進一步降低樣板：

1. [x] `collectors/registry.py` — `COLLECTOR_REGISTRY` 集中註冊 30 個 collector 的 class / display_name / config_prefix / required_env
2. [x] `main.py` 的 `run_collectors()` 改為 registry 迴圈（644 → 309 行）
3. [x] `config.py` 的 `_COLLECTOR_TOGGLES` 迴圈生成 `*_ENABLED/_INTERVAL`，砍掉 30 組樣板
4. [x] `storage/supabase_tables.py` 拆出 `TABLE_MAP`（writer 1477 → 1278 行）
5. [x] `tasks/daily_report.py` S3 費用估算按 Lifecycle storage class 分層
