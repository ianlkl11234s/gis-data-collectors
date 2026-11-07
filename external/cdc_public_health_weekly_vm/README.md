# CDC 公衛週報 — External VM collector

> **為什麼走外部**：Zeabur 出口 IP 連 `od.cdc.gov.tw` 連線 timeout（IP 段被擋；2026-06-16 實證）。
> 主 repo `collectors/cdc_public_health_weekly.py` 在 Zeabur 設 `CDC_PUBLIC_HEALTH_WEEKLY_ENABLED=false`，
> 改由本目錄單檔版在 HiCloud VM 跑。

## 寫入

- Supabase `live.public_health_weekly`（與主 repo 同表 + 同 schema）
- `UNIQUE(disease_code, iso_year, iso_week, county_code, township_code, age_group, gender, is_imported)` + `ON CONFLICT DO NOTHING`

## 3 個 dataset

| disease_code | filename | dataset_id |
|---|---|---|
| `influenza` | `RODS_Influenza_like_illness.csv` | `rods-influenza` |
| `dengue` | `Weekly_Age_County_Gender_061.csv` | `aagstable-weekly-dengue` |
| `enterovirus` | `RODS_EnteroviralInfection.csv` | `rods-enteroviral-infection` |

## 排程

每週四 11:00（CDC 約 10:00 發布上週資料，補 1 小時 buffer）。

## S3 歸檔（2026-11-07 起）

- collector 快照存完整 `data`（以前只存 50 列 sample），路徑 `<DATA_DIR>/cdc_public_health_weekly/YYYY/MM/DD/*.json`
- `archive_cdc_public_health_weekly.py` 每日 03:20 把「昨天以前」的日期目錄打包上
  `s3://migu-gis-data-collector/cdc_public_health_weekly/archives/YYYY-MM-DD.tar.gz`（接續 2026-06-16 停掉的同一前綴），
  上傳後 `head_object` 比對大小一致才刪本地；每次往回掃 30 天補漏
- `.env` 需要 `S3_BUCKET`、`S3_REGION`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`（同其他 VM）
- 主 repo Robot B 仍每日備份 `live.public_health_weekly`；確認 VM 歸檔穩定後，再決定要不要把表移進 `config/backup_manifest.yaml` 的 `archive_py_covered`

### 既有 VM 升級步驟（不重裝）

```bash
# 0. 先 diff VM 現行檔（principles D-04：VM 手動部署，git push 不會生效）
ssh <vm> 'sudo cp /opt/cdc-public-health/cdc_public_health_weekly_collect.py{,.bak_$(date +%Y%m%d)}'
scp cdc_public_health_weekly_collect.py archive_cdc_public_health_weekly.py setup_cron.sh <vm>:~/cdc-vm/
ssh <vm>
diff ~/cdc-vm/cdc_public_health_weekly_collect.py /opt/cdc-public-health/cdc_public_health_weekly_collect.py
cp ~/cdc-vm/cdc_public_health_weekly_collect.py ~/cdc-vm/archive_cdc_public_health_weekly.py /opt/cdc-public-health/
/opt/cdc-public-health/venv/bin/pip install --quiet boto3
# 1. .env 補 S3_BUCKET／S3_REGION／S3_ACCESS_KEY／S3_SECRET_KEY（只填值，不要 cat 出來）
sudo vim /opt/cdc-public-health/.env
# 2. 手動跑一次歸檔（目前本地只有 sample 快照的舊目錄也會上傳；之後的週四才是完整資料）
/opt/cdc-public-health/venv/bin/python /opt/cdc-public-health/archive_cdc_public_health_weekly.py
# 3. 重裝 cron（會加上 03:20 歸檔行）
bash ~/cdc-vm/setup_cron.sh
# 4. 驗證
aws s3 ls s3://migu-gis-data-collector/cdc_public_health_weekly/archives/ | tail -3
```

## Deploy 流程（在新 VM）

```bash
# 1. scp 5 個檔到 VM 同一目錄
scp cdc_public_health_weekly_collect.py archive_cdc_public_health_weekly.py setup_vm.sh setup_cron.sh .env.example \
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
  FROM live.public_health_weekly
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
