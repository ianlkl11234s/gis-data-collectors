# 2026-08-30：TDX 平台重置共用金鑰 secret，16 支 collector 連續 15.4 小時 401

## 時間軸

- **2026-08-29 ~17:30** — 16 支共用同一組 `TDX_APP_ID`/`TDX_APP_KEY` 的 collector 開始全數失敗
- **~17:30 → 2026-08-30 09:08**（連續 15.4 小時，463 次 × 2 分鐘 interval）— 持續失敗，告警每輪重發，無人判別出根因
- **2026-08-30 09:08** — 換上新 secret 並 redeploy 後，三支受災 collector（`bus_intercity`、`tourist_shuttle`、`tra_train`）首輪全綠，DB 各寫入 1362 / 158 / 153 筆

## 症狀

- `bus_intercity`、`tourist_shuttle` 在 token 端點
  （`https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token`）收到 401
- `tra_train` 收到 429——**這不是第二個獨立問題，是 401 的連鎖症狀**：
  `utils/auth.py` token 失敗時直接 `raise`、不 cache，16 支 collector 每輪（2 分鐘一次）都重打 token 端點，
  形成 retry storm；storm 裡部分請求先被全域 `TDX_RATE_LIMIT` 擋下（token 請求也計入 5 req/sec 配額），
  外顯就是 429，掩蓋了真正在發生的 401

## 根因

TDX 平台端把這把金鑰的 client secret 重置了。時點落在 TDX 官方公告的維護窗
「2026/8/28(五)～8/31(一) 系統維護」內；TDX 在 2026-06 也有過一次「網站與 API 憑證更新作業」維護前例，
屬同類事件的重演。

## 為何拖了 15 小時才判別出來（兩個盲點）

1. **error body 被吞了**：`auth.py` 用 `raise_for_status()`，只留下
   「401 Client Error: Unauthorized for url: …/token」這行，Keycloak 真正回傳的
   `{"error": "unauthorized_client", "error_description": "Invalid client secret"}`
   從未進過 log。光看 log 完全無法分辨是「金鑰被停用」「IP 被封鎖」還是「secret 對不上」。
2. **告警洗版**：463 次失敗、每 2 分鐘一則，通知頻道被同一則錯誤刷了 460+ 則，
   訊噪比低到讓人下意識當成「已知的雜訊」而非「需要立刻排查的新事故」。

## 判別法（一鎚定音）

本地重放 token 請求（讀 `.env`，只印狀態碼與 body，絕不印 secret）：

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

回應是 `unauthorized_client` + `Invalid client secret` → client_id 還存在、只是 secret 對不上，
確定是**平台端重置**，排除兩個容易先聯想到的假設：
- IP 被封鎖 → 那會是 403 或 timeout，不會走到 Keycloak 給出結構化錯誤
- 純限流 → 463 次連敗不可能一次都沒放行過；429 通常會偶爾成功，401 不會

## 修復

1. TDX 會員中心查看金鑰狀態、取得新的 client secret
2. 更新本地 `.env`
3. 更新 Zeabur 主站對應 service 的環境變數
4. redeploy（env 改後必重啟才生效）
5. 驗證：runtime log 首輪成功、DB 有新 row、告警停止

中斷期間（2026-08-29 17:30 ～ 2026-08-30 09:08）的即時資料是**永久缺口**，無法回補。

## 給後人的教訓

1. **401 在 token 端點跟 429 是兩回事，不要混著排查**——429 可能只是 401 觸發 retry storm 後的連鎖症狀，
   看到兩者同時出現時先假設認證層出問題，不要往「調高 rate limit」的方向修
2. **`raise_for_status()` 會吃掉 Keycloak 的 error body**——之後排查認證失敗，第一步永遠是本地重放拿到
   完整 response body，不要只看 log 裡那行被截斷的 exception message
3. **共用一組金鑰的 collector 數量越多，單點故障半徑越大**——16 支同時倒下本身就是強訊號，
   但因為告警洗版反而被當成雜訊，設計告警時要考慮「同一錯誤連續 N 次後升級／降噪但不能消音」
4. TDX 官方維護窗（如「系統維護」公告）期間如果多支 collector 同時出現認證類錯誤，
   優先假設是平台端動作（重置 secret／憑證更新），比照 2026-06 的先例，不要預設是自己端的設定跑掉

## 相關文件

- [docs/TDX_RATE_LIMITING.md](../../docs/TDX_RATE_LIMITING.md) 第六節「401 認證失敗排查」— 完整判別表與 SOP
