# ship_ais on HiCloud VM

> `ship_ais` collector 不在 Zeabur 跑、而是在中華電信 HiCloud 上一台 VM 跑的特殊版本。
> **完整 pattern 與動機請看 [`docs/EXTERNAL_COLLECTORS.md`](../../docs/EXTERNAL_COLLECTORS.md)。**

## 為什麼

航港局 AIS API (`mpbais.motcmpb.gov.tw`) 對 firewall 白名單只放台灣 ISP 的 AS：

| 出口 | 結果 |
|---|---|
| Zeabur (Linode 段) | ❌ TCP timeout |
| GCP asia-east1 (Google AS) | ❌ TCP timeout |
| AWS / Azure 任一 region | ❌ 預期同樣被擋 |
| HiCloud（HiNet AS） | ✅ HTTP 200 |

驗證日期：2026-06-06

## 目前部署

| 項目 | 值 |
|---|---|
| VM | HiCloud CaaS 第二區（台南），通用型 b.1c1g (1vCPU/1GB/30GB) |
| Public IP | `210.61.15.74` |
| OS | Ubuntu 22.04 LTS |
| App dir | `/opt/ship-ais/` |
| Data dir | `/var/lib/ship-ais/data/` |
| Logs | `/var/log/ship-ais/` |
| 月費 | 約 NT$ 480 |

## 架構

```
[每 10 分鐘] cron → ship_ais_collect.py
                    ├─ GET 航港局 GeoJSON
                    ├─ 寫 Supabase realtime.ship_positions (INSERT)
                    ├─ 寫 Supabase realtime.ship_current  (UPSERT by mmsi)
                    └─ 寫本地 JSON snapshot
                        /var/lib/ship-ais/data/ship_ais/YYYY/MM/DD/ship_ais_HHMM.json

[每天 03:00] cron → archive_ship_ais.py
                    ├─ 找 8 天前的 JSON 目錄
                    ├─ 打包 tar.gz
                    ├─ 上傳 s3://migu-gis-data-collector/ship_ais/archives/YYYY-MM-DD.tar.gz
                    └─ 刪本地該日目錄
```

寫入規格與主 repo `collectors/ship_ais.py` + `storage/supabase_writer._transform_ship_ais` + `storage/supabase_tables.TABLE_MAP['ship_ais']` **完全對齊**（同欄位、同 SRID、同 PK），保證跟 Zeabur 版本可互換。

## 檔案

| 檔案 | 用途 |
|---|---|
| `ship_ais_collect.py` | 主 collector，每 10 分跑一次 |
| `archive_ship_ais.py` | 每日 03:00 歸檔到 S3 |
| `setup_vm.sh` | 一鍵安裝（apt + pip + 目錄 + .env 範本） |
| `setup_cron.sh` | 安裝兩條 cron 規則 |
| `test_ais.py` | 驗證 VM 能連 mpbais.motcmpb.gov.tw |
| `test_s3.py` | 驗證 .env 裡的 key 能存取 S3 |
| `.env.example` | env 範本（VM 上實際路徑：`/opt/ship-ais/.env`） |

## Deploy 流程（新機器 / 災害復原）

從你本機 Mac 操作（VM 上不裝 git，避免 GitHub 認證麻煩）：

```bash
# 1. 開好 HiCloud VM、拿到 IP、能 SSH 進去
ssh root@<NEW_IP>   # 確認 OK 後 exit 回本機

# 2. scp 6 個檔案
cd .../data-collectors/external/ship_ais_vm
scp ship_ais_collect.py archive_ship_ais.py setup_vm.sh setup_cron.sh test_ais.py test_s3.py \
    root@<NEW_IP>:/tmp/

# 3. 在 VM 上跑 setup
ssh root@<NEW_IP>
bash /tmp/setup_vm.sh
install -m 755 /tmp/ship_ais_collect.py /opt/ship-ais/ship_ais_collect.py
install -m 755 /tmp/archive_ship_ais.py /opt/ship-ais/archive_ship_ais.py

# 4. 填 .env
nano /opt/ship-ais/.env
# - SUPABASE_DB_URL
# - S3_ACCESS_KEY / S3_SECRET_KEY
# 其他預填就好

# 5. 驗證
python3 /tmp/test_ais.py            # 應該 HTTP 200 + egress IP 是 HiNet
python3 /tmp/test_s3.py             # 應該列出既有 ship_ais/* 物件 + OK
python3 /opt/ship-ais/ship_ais_collect.py   # 應該寫 6000+ 筆到 Supabase + 寫本地 snapshot

# 6. 裝 cron
bash /tmp/setup_cron.sh
```

## 維運

### 看 log
```bash
tail -f /var/log/ship-ais/collect.log     # 每 10 分一段
tail -f /var/log/ship-ais/archive.log     # 每日 03:00 一段
```

### 暫停（不刪）
```bash
crontab -l | sed '/ship_ais_collect/s/^/#/' | crontab -
```
取消註解就恢復。

### 改 fetch 邏輯
主 repo 的 `collectors/ship_ais.py` 改動後（vessel_type 對照、欄位 mapping），手動同步到這邊的 `ship_ais_collect.py`。歷史顯示這檔案一年改 0–1 次，同步成本極低。

### 改 Supabase schema
若 `realtime.ship_positions` / `ship_current` 的欄位 / PK 改了：
1. 改主 repo 的 `storage/supabase_tables.py` 和 `_transform_ship_ais`
2. 改這邊 `ship_ais_collect.py` 的 `COLUMNS` / `transform` / SQL
3. 一起部署（先改 Supabase migrations → 改主 repo → 改 VM）
