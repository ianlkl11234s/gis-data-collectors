# TDX API Rate Limiting — 長期防撞規則

> 本文件記錄 TDX API 節流機制的運作原理、設定方式、與新增 TDX collector 時的必備 checklist。
> **只要依照此 checklist 新增 collector，就不會再遇到 2026-04-16 的 17/22 城市 429 事件。**

最後更新：2026-04-16

---

## 一、背景：為何需要全域節流？

### 事件經過（2026-04-16）

公車 collector 擴充到全台 22 縣市（原 6 都），部署後立刻遭遇：

```
✓ 成功 (5):  Taipei, NewTaipei, Taoyuan, Taichung, Tainan
✗ 429 (17): Kaohsiung, Keelung, Hsinchu, Chiayi, 所有離島縣, 所有花東縣...
```

同時間 `tra_train` 也 429：
```
[tra_train] ✗ 錯誤: 429 Client Error: Too Many Requests for url: .../TrainLiveBoard
```

### 根本原因

TDX 限制：**5 req/sec/金鑰**（預設，各方案可能不同）。

我們踩到的陷阱：
1. **單 collector 內部平行抓取** — `BUS_FETCH_WORKERS=5` 瞬間 5 個 concurrent request
2. **多 collector 共用同一個出口 IP** — bus、tra_train、freeway_vd、youbike... 全部在打 TDX
3. **Token refresh 也算請求** — TDXAuth token 過期時會打 POST /auth/token，也算入 rate limit
4. **schedule tick 對齊** — 整數分鐘觸發時，多個 collector 會在同一秒 burst

沒有任何一個單獨改動能根治 — **必須做全域（process-wide）節流**。

---

## 二、解法：`utils/rate_limiter.py` + `TDXSession`

### 架構

```
所有 TDX collector (bus, youbike, tra_train, ...)
          │
          ▼
    self._session = TDXSession()   ← requests.Session 子類
          │
          ▼
    TDXSession.request() ───────► get_tdx_rate_limiter().acquire()
          │                              │
          ▼                              ▼
    requests.Session.request()      固定間隔節流（預設 250ms = 4 req/sec）
```

### 設定

| 設定 | 位置 | 預設 | 說明 |
|------|------|------|------|
| `TDX_RATE_LIMIT` | `config.py` / env | `4` | req/sec，預設低於 TDX 金鑰上限 5，留 1 req/sec buffer |

調整方式：在 `.env` 加 `TDX_RATE_LIMIT=3`（更保守）或 `TDX_RATE_LIMIT=4.5`（貼近上限）。

### 全域 singleton

所有 TDX collector 共用 `get_tdx_rate_limiter()` 回傳的同一個 `RateLimiter` 實例。
無論多少個 collector、多少個 thread，每秒送出的 TDX 請求都不會超過 `TDX_RATE_LIMIT`。

---

## 三、新增 TDX Collector 的 Checklist ⭐

**以下 3 步驟是強制的，缺一不可。**

### Step 1: session 改用 `TDXSession`

```python
# ❌ 錯誤：會繞過 rate limiter
from utils.auth import TDXAuth
import requests

class MyNewCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self._session = requests.Session()    # ← 沒有節流！
        self.auth = TDXAuth(session=self._session)
```

```python
# ✅ 正確：所有 request 都自動節流
from utils.auth import TDXAuth
from utils.tdx_session import TDXSession

class MyNewCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self._session = TDXSession()          # ← 自動節流
        self.auth = TDXAuth(session=self._session)
```

### Step 2: 所有 HTTP 呼叫都要帶 timeout

```python
response = self._session.get(
    url,
    headers=headers,
    timeout=config.REQUEST_TIMEOUT,   # ← 必填
)
```

### Step 3: 平行抓取（ThreadPoolExecutor）需注意

即使有全域節流，內部 `ThreadPoolExecutor(max_workers=N)` 也不要設太大：
- `max_workers=5` 和 `max_workers=2` 實際 throughput 一樣（都被 rate limiter 擋到 4 req/sec）
- 設太高只會增加線程 context switch 成本，沒有好處

**建議**：`max_workers ≤ TDX_RATE_LIMIT + 1`（例如 rate=4 就用 workers=2-5）

---

## 四、新增 Collector 之前：容量規劃

新增 TDX collector 前先算一下對總配額的影響：

```
新 collector 每次 run 的 request 數 × (60 / interval_分鐘) = 每小時 req 數
```

| collector | 每次 req | 間隔 | 每小時 req |
|-----------|---------|------|-----------|
| bus (22 城) | 22 | 2 min | 660 |
| bus_intercity | 1 | 2 min | 30 |
| tra_train | 1 | 2 min | 30 |
| freeway_vd | 2 | 10 min | 12 |
| youbike (3 城) | 3 | 15 min | 12 |
| parking (3 城) | 3 | 15 min | 12 |
| 其他零星 | ~5 | 不定 | ~30 |
| **合計** | — | — | **~786/hr** |

平均 `786 / 3600 = 0.22 req/sec`，遠低於 4 req/sec 上限。
**問題從來不是總量，而是瞬間 burst**。全域 limiter 正是防這個。

---

## 五、排查 429 錯誤流程

如果未來又看到 TDX 429 log：

### Step 1: 確認是誰

```bash
# 在 Zeabur deployment log 找：
npx zeabur@latest deployment log --service-id <id> -t runtime \
  | grep "429\|Too Many Requests"
```

看錯誤訊息的 URL，確認是哪個 endpoint。

### Step 2: 確認該 collector 用了 `TDXSession`

```bash
grep -rn "self._session = requests.Session()" collectors/
```

**如果任何 TDX collector 還在用 `requests.Session()` 就是漏改** — 立刻改成 `TDXSession()`。

### Step 3: 確認 rate limiter stats

在某個 collector 裡加 debug log：
```python
from utils.rate_limiter import get_tdx_rate_limiter
print(get_tdx_rate_limiter().get_stats())
# {'name': 'tdx', 'rate_per_sec': 4, 'acquire_count': 1234,
#  'total_wait_sec': 56.7, 'avg_wait_ms': 45.9}
```

- `avg_wait_ms` 很低（< 50ms）→ 節流沒觸發，可能另有原因
- `avg_wait_ms` 接近 250ms（= 1/4 秒）→ 節流在努力工作，可能要調降 `TDX_RATE_LIMIT`

### Step 4: 調整 `TDX_RATE_LIMIT`

如果還是 429，在 Zeabur 儀表板加 env var：
```
TDX_RATE_LIMIT=3
```

重啟即可（不需改 code）。

---

## 六、測試保證

`tests/test_rate_limiter.py` + `tests/test_tdx_session.py` 共 16 個測試，覆蓋：

| 場景 | 測試 |
|------|------|
| 第一次 acquire 不延遲 | `test_first_acquire_no_wait` |
| 連續 acquire 被節流 | `test_rate_limit_enforced` |
| 10 線程同時 acquire 也被節流 | `test_multi_thread_acquires_are_serialized` |
| 22 線程 burst 不會超過 rate | `test_concurrent_burst_does_not_exceed_rate` |
| 跨 TDXSession 實例仍共用 limiter | `test_parallel_session_requests_serialize_through_limiter` |
| TDXSession 是 requests.Session 子類 | `test_tdx_session_is_requests_session_subclass` |
| singleton 行為 | `test_singleton_returns_same_instance` |

任何對節流邏輯的修改都必須先跑測試：
```bash
pytest tests/test_rate_limiter.py tests/test_tdx_session.py -v
```

---

## 七、其他 API 的節流需求

目前只有 TDX 有這個限制，但未來可能：

- **MOENV** — 不確定是否限流，先不加（有問題再加 `get_moenv_rate_limiter()`）
- **CWA** — 不同 endpoint 限制不同，不一定需要全域
- **FR24** — 已經有 `FLIGHT_FR24_TRAIL_DELAY` 手動延遲機制

若未來其他 API 也需要節流：
1. `utils/rate_limiter.py` 的 `RateLimiter` 類別可重用
2. 新增一個 `get_moenv_rate_limiter()` singleton
3. 仿造 `TDXSession` 建立 `MOENVSession`
4. 更新本文件的「其他 API」章節

---

## 八、變更記錄

| 日期 | 事件 |
|------|------|
| 2026-04-16 | 公車 6→22 城擴充後遭遇 17/22 城 429，建立全域 TDX rate limiter |
