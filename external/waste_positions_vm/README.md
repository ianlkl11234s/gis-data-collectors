# waste_positions on HiCloud VM

> VM 位址與登入帳號不放在公開 repo：文中的 `<VM_HOST>`、`<帳號>` 請見私人筆記。

> 第二個搬到 HiCloud 的 collector，**跟 ship_ais 共用同一台 VM** (`<VM_HOST>`)。
> Pattern 與動機請看 [`docs/EXTERNAL_COLLECTORS.md`](../../docs/EXTERNAL_COLLECTORS.md)。

## 為什麼

垃圾車 GPS 三個來源（高雄/新北/台南）對 Zeabur 出口 IP 偶發 ConnectTimeout（commit `c5a193d` 寫過 retry 緩解但治標）。搬到 HiNet 段 IP 後**根本解決**。

## 目前部署

| 項目 | 值 |
|---|---|
| VM | 共用 ship_ais 那台（HiCloud b.1c1g，第二區域 `<VM_HOST>`） |
| App dir | `/opt/waste-positions/` |
| Data dir | `/var/lib/waste-positions/data/` |
| Logs | `/var/log/waste-positions/` |
| Interval | 2 分鐘 |
| Quiet hours | 01:00–06:00 跳過 |
| 三家來源 | 高雄 `openapi.kcg.gov.tw` / 新北 `data.ntpc.gov.tw` / 台南 `soa.tainan.gov.tw` |

## 架構

```
[每 2 分鐘] cron → waste_positions_collect.py
                    ├─ (01–06 quiet hours skip)
                    ├─ 抓 3 家 (with 5/15s retry, 共 3 次)
                    ├─ 統一 schema → INSERT … ON CONFLICT DO NOTHING spatial.waste_positions_realtime
                    └─ 寫本地 JSON snapshot

[每天 03:05] cron → archive_waste_positions.py
                    └─ 同 ship_ais archiver，路徑換成 waste_positions
```

寫入規格與主 repo `collectors/waste_positions.py` + `_transform_waste_positions` + `TABLE_MAP['waste_positions']` **完全對齊**。
注意：waste_positions 是 **append-only history**（無 current 表、無 UPSERT），前端用 `DISTINCT ON (vehicle_no) ORDER BY observed_at DESC` 取最新。

去重（2026-10-15）：來源每次回傳最近 N 分鐘的 GPS，2 分鐘輪詢會重疊抓到同一點（臺南約 2.5 倍）。INSERT 改成**不指定目標**的 `ON CONFLICT DO NOTHING`，搭配 gis-platform migration 458 的 `(city, vehicle_no, observed_at)` 唯一索引擋掉重複點。**順序：先部署本程式，再套 458**；反過來的話，含重複點的整批 INSERT 會失敗，資料會一直卡在 buffer 重送。

## 檔案

| 檔案 | 用途 |
|---|---|
| `waste_positions_collect.py` | 主 collector，每 2 分跑一次 |
| `archive_waste_positions.py` | 每日 03:05 歸檔到 S3 |
| `setup_waste.sh` | 安裝目錄 + 部署程式 + 建 .env 範本 |
| `setup_waste_cron.sh` | 安裝 cron |
| `.env.example` | env 範本 |

## Deploy 流程（新加裝 / 災害復原）

從本機 Mac：

```bash
# 1. scp 4 個檔案
cd .../data-collectors/external/waste_positions_vm
scp waste_positions_collect.py archive_waste_positions.py setup_waste.sh setup_waste_cron.sh \
    <帳號>@<VM_HOST>:/tmp/

# 2. 在 VM 上裝
ssh <帳號>@<VM_HOST>
bash /tmp/setup_waste.sh

# 3. 複用 ship_ais 的 .env（key 完全一樣，只差 DATA_DIR / QUIET_HOURS）
cp /opt/ship-ais/.env /opt/waste-positions/.env
nano /opt/waste-positions/.env
# 確保以下兩行符合 waste_positions：
#   DATA_DIR=/var/lib/waste-positions/data
#   QUIET_HOURS=01-06

# 4. 驗證
python3 /opt/waste-positions/waste_positions_collect.py

# 5. 裝 cron
bash /tmp/setup_waste_cron.sh
```

## 維運

```bash
tail -f /var/log/waste-positions/collect.log
tail -f /var/log/waste-positions/archive.log
```

暫停：`crontab -l | sed '/waste_positions_collect/s/^/#/' | crontab -`

## 注意

- 因為是 **append-only**，不能像 ship_ais 那樣「中斷重跑沒事」。**Zeabur 必須先 `WASTE_POSITIONS_ENABLED=false`**，否則同一時段會雙 INSERT、前端會看到重複位置點。
