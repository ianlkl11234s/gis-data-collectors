# moa_opendata on HiCloud VM（農業部開放資料 7 個 collector）

> VM 位址與登入帳號不放在公開 repo：`<VM_HOST>`、`<帳號>` 見私人筆記。

## 為什麼在 VM

2026-10-21～23 起 `data.moa.gov.tw` 擋 Zeabur 出口（JP IP → TCP ConnectTimeout；`www.moa.gov.tw` 仍可連），
HiCloud VM（台灣 IP）實測 200／2.3s。受影響的 7 個 collector 全部移到這台 VM：

| collector | 主 repo 模組 | interval |
|---|---|---|
| `animal_adoption` | `collectors/animal_adoption.py` | 1440 分（每日） |
| `food_prices` | `collectors/food_prices.py` | 1440 分（每日） |
| `animal_shelter_outcomes`、`animal_shelter_pressure` | `collectors/animal_shelter_outcomes.py` | 43200 分（30 日） |
| `animal_veterinary_clinics` | `collectors/animal_welfare_points.py` | 259200 分（180 日） |
| `animal_licensed_pet_businesses` | 同上 | 525600 分（365 日） |
| `animal_protection_offices` | 同上 | 129600 分（90 日） |

## 和其他 VM app 不同：不重寫，直接跑主 repo 程式

其他 `external/*_vm/` 是單檔重寫版。這裡改成把主 repo 的程式原樣帶上 VM，`moa_opendata_run.py`
import 真正的 collector class 並呼叫 `BaseCollector.run()`，因此 LocalStorage、`SupabaseWriter`、
`metadata.collector_status` 心跳、run ledger、S3 歸檔都與 Zeabur **同一份程式**。改 collector 邏輯只改主 repo，
重新打包部署即可。

- `collectors/__init__.py`、`tasks/__init__.py` 會 import 全部 collector／task（numpy、xarray、yaml…），
  所以 tarball 不帶這兩個檔，runner 也會先把 `collectors`、`tasks` 註冊成空殼 package。
- 額外依賴：**無**。只需要 VM 已有的 `requests`、`psycopg2`、`boto3`（`python-dotenv` 有沒有都可以；cron 先 `. ./.env`）。
- `*_ENABLED` toggle 不影響 runner（只有 Zeabur 的 registry／main.py 看它）。**Zeabur 端 7 個 toggle 要設 false** 防雙跑。

## 排程與到期判斷

| cron（Asia/Taipei） | 動作 |
|---|---|
| `53 0 * * *` | `--all-due`：依序（animal_adoption → food_prices → 其餘）跑到期的 collector |
| `21 3 * * *` | `--archive`：重用 `tasks/archive.py`（前一天以前的日期目錄 → `s3://…/<collector>/archives/YYYY-MM-DD.tar.gz`，驗證 receipt 後刪除超過 `ARCHIVE_RETENTION_DAYS` 的本地目錄） |

- 到期：`/var/lib/moa-opendata/state/last_success.json` 記每個 collector 最後一次成功時間；
  沒有紀錄，或距今 ≥ interval × 0.9 就跑。每日的兩個每天都會跑；30／90／180／365 日的每天檢查一次。
- 成功判定：`run()` 回傳沒有 `error`（含 ledger 記 failed 的 `_collector_error`）。失敗不更新 state → 隔天 00:53 再試；runner exit 1。
- 每個 collector 在獨立子行程跑（每個上限 `MOA_CHILD_TIMEOUT_SEC`，預設 1500s），大型 JSON 用完即釋放；整輪 cron 外層再包 `timeout 3300`。
- 單一 flock（`state/run.lock`）：前一輪還在跑時，新一輪（含 archive）直接 exit 3，不排隊。
- 每輪開頭若 `DATA_DIR/buffer/` 有檔，先跑 `SupabaseWriter.flush_buffer()`（Zeabur 是每 5 分鐘；VM 改每輪一次）。
- 重新部署若清掉 state，第一輪會把 7 個都跑一次（各自是完整快照、各自一筆 run ledger，可接受）。

已知差異：`BaseCollector` 的 collect 連續失敗告警與 `SupabaseWriter` 的「DB 連續失敗 3 次」告警都是 process 內計數，一次性 runner 永遠到不了門檻，**VM 上不會發 Telegram**。collect 例外與 DB 寫入失敗仍會回報 `collector_status` failed（Zeabur 日報看得到）並寫 cron log；DB 失敗的批次進 buffer。ledger 類失敗的監控缺口見 handbook operations.md §3.4。

## 部署

```bash
# 1. 操作者電腦，repo 根目錄（已合併的乾淨 checkout）
bash external/moa_opendata_vm/deploy.sh /tmp          # 印出 tarball 路徑與下列指令
scp /tmp/moa-opendata-<rev>.tar.gz <帳號>@<VM_HOST>:/tmp/moa-opendata.tar.gz

# 2. VM
sudo tar -xzf /tmp/moa-opendata.tar.gz -C /tmp
sudo bash /tmp/moa-opendata/setup_vm.sh                # 建 /opt/moa-opendata、/var/lib/moa-opendata、/var/log/moa-opendata
sudo -e /opt/moa-opendata/.env                         # 首次才需要；之後更新不會覆蓋
```

### .env

key 清單見 `.env.example`。值的來源：`SUPABASE_DB_URL`、`S3_BUCKET`、`S3_REGION`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`
從同台 VM 既有的 `/opt/isohe-port-marine/.env`（或 `/opt/ship-ais/.env`）複製同名 key；
`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID` 從 Zeabur 主服務複製（選填）。
新 key：`SUPABASE_ENABLED=true`（**必填**，漏了只會寫本地）、`SUPABASE_POOL_MIN=1`、`SUPABASE_POOL_MAX=2`、
`DATA_DIR=/var/lib/moa-opendata/data`、`ARCHIVE_RETENTION_DAYS=7`、`INSTANCE_NAME=hicloud-vm`。

### 驗證 → 裝 cron → 關 Zeabur

`.env` 與 `/var/lib/moa-opendata` 都屬 root（cron 也是 root crontab），手動指令一律用同一個 root shell：

```bash
moa() { sudo bash -c 'cd /opt/moa-opendata && set -a && . ./.env && set +a && python3 moa_opendata_run.py "$@"' _ "$@"; }
moa --dry-check                 # import、實例化、印 interval／due；不連 DB、不碰 S3
curl -sS -o /dev/null -w '%{http_code} %{time_total}\n' 'https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=FczRQaLNjcvP&IsTransData=1'
moa food_prices                 # 手動跑一次（會真的寫 DB）；exit 0 才往下
moa animal_adoption
sudo bash /opt/moa-opendata/setup_cron.sh
```

`.env` 是用 `set -a; . ./.env` 載入：值含 `&`、`#`、空白（例如帶 query string 的 `SUPABASE_DB_URL`）要加單引號。

DB 端確認（唯讀）：`metadata.collector_status` 這兩個 collector 的 `last_success_at` 更新、`consecutive_errors = 0`；
`live.animal_adoption_snapshot_runs` 最新一筆 `status` 不是 `failed`；`live.food_price_daily` 有新日期。
隔天 03:21 後看 `archive.log` 與 S3 `<collector>/archives/` 有前一天的 tar.gz。

最後在 Zeabur 設 `FOOD_PRICES_ENABLED`、`ANIMAL_ADOPTION_ENABLED`、`ANIMAL_SHELTER_OUTCOMES_ENABLED`、
`ANIMAL_SHELTER_PRESSURE_ENABLED`、`ANIMAL_VETERINARY_CLINICS_ENABLED`、`ANIMAL_LICENSED_PET_BUSINESSES_ENABLED`、
`ANIMAL_PROTECTION_OFFICES_ENABLED` 為 `false`。

## 回滾

1. VM：`crontab -e` 刪掉 `# ─── moa_opendata cron BEGIN ───` 到 `END` 整段（`/opt/moa-opendata` 可留著）。
2. Zeabur：把上面 7 個 `*_ENABLED` 設回 `true`（僅在 `data.moa.gov.tw` 對 Zeabur 恢復可連時才有意義）。

## Follow-up

- `external/_shared/health_report.py` 的 `DEFAULT_TARGETS` 尚未加入 moa_opendata（log 路徑 `/var/log/moa-opendata/collect.log`）。
- logrotate 未設（每日一行級別，量很小）。
