# 2026-04-14：CelesTrak 封鎖 Zeabur 出口 IP，衛星 collector 停更 5 天

## 症狀

`collectors/satellite.py` 在 Zeabur 上所有 CelesTrak 群組 query 都 **connect timeout**：

```
[satellite] radar: 拉取失敗 (HTTPSConnectionPool(host='celestrak.org', port=443)
  ... ConnectTimeoutError: Connection to celestrak.org timed out. (connect timeout=60))
```

Supabase `satellite_current / positions / tle` 停在 `2026-04-09 20:22 UTC`，已 5 天沒新資料。

## 診斷結論

**Zeabur 出口 IP 被 CelesTrak 防火牆 drop**（不是 403、不是 RST，是完全不回應 SYN）。

| 測試 | 結果 |
|---|---|
| 本機 `curl celestrak.org` | 0.24s 連上 ✅ |
| Zeabur 容器 `socket.create_connection((celestrak.org, 443))` | **10s timeout** |
| Zeabur DNS 解析 | 正常（`104.168.149.178`） |

TCP SYN 發出但沒收到 SYN-ACK 也沒收到 RST = IP blackhole（典型的 firewall drop）。

CelesTrak 對高頻爬取的 PaaS 共用 IP 段（Zeabur、AWS、GCP 等）有封鎖政策，很可能是累積請求觸發門檻。

## 解法：改用 Space-Track.org

CelesTrak 本身就是從 Space-Track 轉抓的，Space-Track 是官方 18 SDS 來源：
- 需免費註冊帳號
- 有明確 API rate limit（不會靜默封 IP）
- GP class 一次 query 完整 catalog（不用像 CelesTrak 分 29 個群組）
- 附 `DECAY_DATE` / `OBJECT_TYPE` 欄位，可辨識失效衛星 / 火箭體 / 碎片

## 連帶變更

- migration `036_satellite_decay_status.sql`：`satellite_tle` + `satellite_tle_history` 新增 `decay_date`、`is_decayed`、`object_type`
- `satellite_current / positions` 只寫入「活躍 + PAYLOAD（衛星本體）」，保持舊有顯示語意（~17k 顆）
- `satellite_tle` 寫入「完整 catalog」含火箭體/碎片/失效（~67k 筆）
- 加 `current_prune_by: collected_at` 讓 `satellite_current` 每次寫入後 DELETE 過期 row（否則 UPSERT 會累積 stale 資料）

## 給後人的教訓

1. **ConnectTimeoutError 而非 403/404 時，優先懷疑 IP 層封鎖**（特別是 PaaS 共用 IP + 免費資源）
2. **資料源盡量選原始供應者**（Space-Track vs CelesTrak），減少中間商 rate limit 風險
3. **current 表語意要顧**：UPSERT-only 會累積幽靈資料，需要配合 stale row 清理機制
