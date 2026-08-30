# TDX API Rate Limiting — 長期防撞規則

> 本文件記錄 TDX API 節流機制的運作原理、設定方式、與新增 TDX collector 時的必備 checklist。
> **只要依照此 checklist 新增 collector，就不會再遇到 2026-04-16 的 17/22 城市 429 事件。**

最後更新：2026-08-30

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

## 六、401 認證失敗排查（跟 429 是兩回事）

### 症狀判別：401 vs 429

| | 429 Too Many Requests | 401 Unauthorized（在 token 端點） |
|---|---|---|
| 位置 | 業務 API endpoint（如 `/TrainLiveBoard`） | `/auth/realms/TDXConnect/protocol/openid-connect/token` |
| 性質 | 流量問題，會自癒 | 認證問題，**不會**因為降頻、等待自己好 |
| 對應章節 | 見上方「五、排查 429 錯誤流程」 | 本節 |

**關鍵陷阱：429 可能只是 401 的連鎖症狀，不是第二個獨立問題。**
2026-08-30 之前的 `utils/auth.py` 拿 token 失敗時直接 `raise`、不 cache——如果 token 本身壞了（例如平台端重置了 secret），
每一支 collector 每一輪都會重打 token 端點，形成 retry storm；這股 storm 裡有些請求會先被 `TDX_RATE_LIMIT`
擋下變成 429（token 請求也算在 5 req/sec 配額內）。看到「token 端點 401 + 業務端點 429 同時出現、且 401 完全不會恢復」
時，先假設是認證問題，429 只是噪音，不要往「金鑰上限不夠」的方向排查。
（現在的程式已有全域 backoff 止住 storm，見下方「程式面防護」，但判讀原則不變。）

### Keycloak error body 判讀表

`raise_for_status()` 只會留下「401 Client Error: Unauthorized for url: …/token」，**看不到真正的 error body**。
要看懂根因，必須自己印出 response body（見下方重放測試）。Keycloak 常見回應：

| `error` | `error_description`（常見） | 意義 | 處置 |
|---|---|---|---|
| `invalid_client` | client not found / 格式錯 | `TDX_APP_ID` 不存在或打錯 | 檢查 `.env` / Zeabur env 的 `TDX_APP_ID` 有沒有誤植 |
| `unauthorized_client` | `Invalid client secret` | **client_id 存在，但 secret 對不上**——通常是 TDX 平台端把這把金鑰的 secret 重置了 | 上 TDX 會員中心查看金鑰、換新 secret（見下方修復 SOP） |
| （帳號/應用層錯誤，訊息含 disabled / suspended） | — | 金鑰或會員帳號被停用 | 聯繫 TDX 客服或直接到會員中心確認金鑰狀態 |

### 本地重放測試：一鎚定音判別法

不要用線上 log 猜，直接本地重放 token 請求，看 response body（**絕不印出 secret**）：

```python
import requests
env = {}
with open('.env') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, _, v = line.partition('=')
            env[k.strip()] = v.strip().strip('"').strip("'")
r = requests.post(
    'https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token',
    data={'grant_type': 'client_credentials',
          'client_id': env['TDX_APP_ID'], 'client_secret': env['TDX_APP_KEY']},
    timeout=15)
print('HTTP', r.status_code)
print(r.text[:300] if not r.ok else 'token OK, expires_in=%s' % r.json().get('expires_in'))
```

判讀：
- `unauthorized_client` + `Invalid client secret` → client_id 還在，secret 對不上 = **平台端重置**。
  排除掉「IP 被封鎖」（那會是 403 或 timeout，不會走到 Keycloak 回應）與「純限流」
  （463 次連敗不可能一次都不成功——429 會偶爾放行，401 不會）。
- 拿到 `token OK` → 問題不在金鑰本身，回頭查 collector 端的 session/cache 邏輯或環境變數是否有沒生效。

### 修復 SOP

1. 到 TDX 會員中心查看金鑰狀態，取得新的 client secret
2. 更新本地 `.env` 的 `TDX_APP_KEY`
3. 同步更新 Zeabur 主站對應 service 的環境變數
4. **重啟／redeploy**（env 改了不會自動生效，這點跟其他 Zeabur env 一樣）
5. 驗證三件事都綠：Zeabur runtime log 首輪出現對應 collector 的成功 log、DB 對應 table 有新 row（`collected_at` 在重啟後）、告警停止重複發送

### 程式面防護（2026-08-30 起）

上述事故後，認證層加了三道防護（`tests/test_auth.py` / `tests/test_notify.py` 有測試保證）：

1. **Token 端點錯誤帶完整 body**（`utils/auth.py`）：token 端點回 4xx/5xx 時，Keycloak 的
   error body（截斷 200 字）會併入例外訊息並寫入 log——上面判讀表要的 `error_description`
   直接看 log 就有，不用再本地重放才看得到。
2. **全域 token 失敗 backoff**（`utils/auth.py`）：token 端點連續失敗時，所有 TDXAuth instance
   共用一個 class-level backoff（首次 60s、倍增、上限 600s；429 帶 `Retry-After` 時尊重伺服器
   要求不受上限限制），backoff 期間直接 fail fast 不打網路，止住 retry storm。任一 instance
   成功即歸零。Token cache 本身仍是 per-instance。
3. **資料 API 401 自動換 token retry 一次**（`utils/tdx_session.py`）：token 在效期內被伺服器端
   撤銷時，不再抱著死 token 空轉到本地 expiry（最長 24h），收到 401 立即 invalidate → 重取
   token → 重新 acquire rate limiter → retry 該請求一次。

另外告警端（`utils/notify.py`）同日改為指數收斂：連續錯誤只在門檻的 2^k 倍（3, 6, 12, 24…次）
發 Telegram 告警，恢復時發一則 ✅ 通知——463 連錯從 460+ 則洗版變 8 則。

### 參考事故

2026-08-30：TDX 平台端重置了共用金鑰的 secret，16 支共用同一組 `TDX_APP_ID`/`TDX_APP_KEY` 的 collector
全數 401，連續 15.4 小時（463 次 × 2 分鐘 interval）才被判別出來並修復。
詳見 [`.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md`](../.claude/pitfalls/2026-08-30-tdx-secret-reset-401.md)。

---

## 七、測試保證

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

## 八、其他 API 的節流需求

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

## 九、變更記錄

| 日期 | 事件 |
|------|------|
| 2026-04-16 | 公車 6→22 城擴充後遭遇 17/22 城 429，建立全域 TDX rate limiter |
| 2026-08-30 | TDX 平台端重置共用金鑰 secret，16 支 collector 連續 15.4 小時 401，新增「六、401 認證失敗排查」 |
