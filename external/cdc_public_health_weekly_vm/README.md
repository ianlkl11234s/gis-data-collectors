# CDC 公衛週報 — External VM collector

> **為什麼走外部**：Zeabur 出口 IP 連 `od.cdc.gov.tw` 連線 timeout（IP 段被擋；2026-06-16 實證）。
> 主 repo `collectors/cdc_public_health_weekly.py` 在 Zeabur 設 `CDC_PUBLIC_HEALTH_WEEKLY_ENABLED=false`，
> 改由本目錄單檔版在 HiCloud VM 跑。

## 寫入

- Supabase `realtime.public_health_weekly`（與主 repo 同表 + 同 schema）
- `UNIQUE(disease_code, iso_year, iso_week, county_code, township_code, age_group, gender, is_imported)` + `ON CONFLICT DO NOTHING`

## 3 個 dataset

| disease_code | filename | dataset_id |
|---|---|---|
| `influenza` | `RODS_Influenza_like_illness.csv` | `rods-influenza` |
| `dengue` | `Weekly_Age_County_Gender_061.csv` | `aagstable-weekly-dengue` |
| `enterovirus` | `RODS_EnteroviralInfection.csv` | `rods-enteroviral-infection` |

## 排程

每週四 11:00（CDC 約 10:00 發布上週資料，補 1 小時 buffer）。

## Deploy 流程（在新 VM）

```bash
# 1. scp 4 個檔到 VM 同一目錄
scp cdc_public_health_weekly_collect.py setup_vm.sh setup_cron.sh .env.example \
    user@hicloud-vm:~/cdc-vm/

# 2. SSH 進 VM
ssh user@hicloud-vm
cd ~/cdc-vm

# 3. 跑 setup
bash setup_vm.sh

# 4. 編輯 .env 填 SUPABASE_DB_URL（範本在 /opt/cdc-public-health/.env）
sudo vim /opt/cdc-public-health/.env

# 5. 測一次
/opt/cdc-public-health/venv/bin/python /opt/cdc-public-health/cdc_public_health_weekly_collect.py

# 6. 裝 cron
bash setup_cron.sh
```

## 驗證

```bash
# 看 cron log
tail -f /var/log/cdc-public-health/collect.log

# 確認 Supabase 有寫入
psql "$SUPABASE_DB_URL" -c "
SELECT disease_code,
       MAX(iso_year*100+iso_week) AS latest_week,
       COUNT(*) AS rows
  FROM realtime.public_health_weekly
 GROUP BY disease_code;
"
```

## 與主 repo 的關係

- **schema / 解析邏輯唯一真相**：`collectors/cdc_public_health_weekly.py`（主 repo）
- 本 VM 版照抄主 repo 的 parser / column mapping
- 若 CDC 改欄名 / 改 dataset_id → **先修主 repo**，再 sync 到本 VM 版

## 已知陷阱

- ⚠️ `od.cdc.gov.tw` 憑證缺 SKI → `verify=False` 必開（同 NHI / mnd.gov.tw）
- ⚠️ 登革熱 CSV 欄名為「發病年份/發病週別/確定病例數/年齡層/是否為境外移入」，跟其他兩 dataset 不同
- ⚠️ 真實檔名要從 CKAN `package_show` API 撈；本檔已固化檔名 mapping
- 💡 全量歷史 ~215k 筆橫跨 2003-2026；過濾 2 年後約 13k 筆
