# immigration_apis_airport on HiCloud VM

> `immigration_apis_airport` collector 不在 Zeabur 跑、而是在中華電信 HiCloud 上一台 VM 跑的特殊版本。
> **完整 pattern 與動機請看 [`docs/EXTERNAL_COLLECTORS.md`](../../docs/EXTERNAL_COLLECTORS.md)。**

## 為什麼

移民署 APIS (`opendata.immigration.gov.tw/APIS/*`) 對國際雲商 IP 封鎖：

| 出口 | 結果 |
|---|---|
| Zeabur (Linode 段) | ❌ TCP connect timeout 30s |
| HiCloud（HiNet AS） | ✅ 預期 HTTP 200 |

驗證日期：2026-06-28（Zeabur 端 6 endpoint 全 connect timeout）

## 目前部署

| 項目 | 值 |
|---|---|
| VM | 共用 HiCloud VM `210.61.15.74`（與 ship_ais / waste_positions 同台） |
| App dir | `/opt/immigration-apis-airport/` |
| Data dir | `/var/lib/immigration-apis-airport/data/` |
| Logs | `/var/log/immigration-apis-airport/` |
| 月費 | 已涵蓋在共用 VM 約 NT$ 480 |

## 架構

```
[每 60 分鐘 :07] cron → immigration_apis_airport_collect.py
                        ├─ GET 6 個 APIS endpoint (TPE1/5/51/52 + RMQ5 + TSA1)
                        ├─ 寫 Supabase live.border_airport_snapshot (INSERT)
                        └─ 寫本地 JSON snapshot
                            /var/lib/immigration-apis-airport/data/immigration_apis_airport/YYYY/MM/DD/iaa_HHMM.json

[每天 03:15] cron → archive_immigration_apis_airport.py
                    ├─ 找 8 天前的 JSON 目錄
                    ├─ 打包 tar.gz
                    ├─ 上傳 s3://migu-gis-data-collector/immigration_apis_airport/archives/YYYY-MM-DD.tar.gz
                    └─ 刪本地該日目錄
```

寫入規格與主 repo `collectors/immigration_apis_airport.py` + `storage/supabase_writer._transform_immigration_apis_airport` + `storage/supabase_tables.TABLE_MAP['immigration_apis_airport']` **完全對齊**（同欄位、同 append-only、同 endpoint 列表）。

## 檔案

```
external/immigration_apis_airport_vm/
├── README.md
├── immigration_apis_airport_collect.py   ← 單檔 collector
├── archive_immigration_apis_airport.py   ← 每日 S3 歸檔
├── setup_vm.sh                           ← VM 一鍵裝環境
├── setup_cron.sh                         ← 安裝 cron
├── test_apis.py                          ← 驗證 VM 能連到 opendata.immigration
└── test_s3.py                            ← 驗證 S3 key 可用
```

## Deploy 流程（新 VM 用 — 10 分鐘）

```bash
# 1. SCP 6 個檔到 /tmp/
scp external/immigration_apis_airport_vm/*.{py,sh} ubuntu@210.61.15.74:/tmp/

# 2. SSH 進去
ssh ubuntu@210.61.15.74

# 3. 跑安裝（root or sudo）
sudo bash /tmp/setup_vm.sh

# 4. 編輯 .env 填入 SUPABASE_DB_URL + S3 key
sudo nano /opt/immigration-apis-airport/.env

# 5. 連線測試
python3 /tmp/test_apis.py   # 應該 HTTP 200
python3 /tmp/test_s3.py     # 應該 PUT/GET/DELETE OK

# 6. 跑一次 collector 驗證
sudo -u root python3 /opt/immigration-apis-airport/immigration_apis_airport_collect.py

# 7. 裝 cron
sudo bash /tmp/setup_cron.sh

# 8. 觀察 log
tail -f /var/log/immigration-apis-airport/collect.log
```

## 主 repo 對應設定

- `data-collectors/config.py` — `IMMIGRATION_APIS_AIRPORT_ENABLED` 預設 false
- Zeabur env — **必須設 `IMMIGRATION_APIS_AIRPORT_ENABLED=false`** 避免雙跑
- `docs/EXTERNAL_COLLECTORS.md` — 已列入清單
- `../gis-platform/docs/data-inventory.md` — 部署位置標 **HiCloud VM**

## 災害復原

照「Deploy 流程」走，10 分鐘可在新 VM 復原。**不需要 git clone、不需要 GitHub 認證**。
