# external/fire119_vm — 即時 119 案件＋警廣路況（HiCloud VM）

> VM 位址與登入帳號不放在公開 repo：文中的 `<VM_HOST>`、`<帳號>` 見私人筆記。

**一句話**：單一 runner 每分鐘由 cron 叫起，依來源分頻抓 18 個縣市的 119 民眾版案件清單、新北送醫中救護車與警廣即時路況，
落地前先剔除隱私欄位，只把「新案件或有變化」的列寫進 `live.fire119_incidents`／`live.pbs_road_events`（gis-platform migration 555），
解析後的列每日打包上 S3。

**狀態（2026-11-16）**：程式與測試完成，**未部署、migration 555 未套用**。部署與套用都要站主點頭。

- 計畫與決策：taipei-gis-analytics `docs/topic-research/realtime_incidents/`（`PLAN.md` 階段 3、`decision-log.md`、`sources-119.md`、`sources-traffic.md`）
- 表與保留期：gis-platform `migrations/555_fire119_pbs_live_owner.sql`（14 天依 `last_seen_at` 清；站主限定 RPC）
- 為什麼放 VM：警廣、臺南／高雄 DTS 從 pulse-host（境外 IP）連不到；VM 實測 18 個 119 端點與警廣皆 200

## 檔案

| 檔案 | 用途 |
|---|---|
| `fire119_collect.py` | runner（排程、保護、抓取、寫入、健康狀態）；`--dry-run` 不連 DB |
| `fire119_parse.py` | 來源註冊表（網址、頻率、0 筆告警門檻）、隱私剔除、解析、分類、去重 key、TWD97→WGS84 |
| `fire119_store.py` | 組列、逐列驗證（對齊 mig 555 的 CHECK）、本機差異狀態、upsert SQL |
| `match.py` | 路名對照查詢，**原封複製**自 analytics `pipelines/realtime/road_gazetteer/match.py`（檔頭記 commit 與 sha256） |
| `archive_fire119.py` | 每日歸檔（照 `_vm_template/archive_template.py`）＋清掉 3 天前的原始回應 |
| `run_fire119.sh` | cron 包裝：秒偏移、flock、記憶體上限、nice／ionice、timeout |
| `vmstat_monitor.sh` | 第一天監測：每分鐘記 `free -m`、load、上一輪耗時與峰值 RSS |
| `setup_vm.sh`／`setup_cron.sh` | 安裝檔案與套件／安裝、關監測、移除 crontab 區塊 |
| `.env.example` | 環境變數名稱（值不進 repo） |

另需：`external/vm_common/vm_buffer.py`、`external/vm_common/bulk.py`（同目錄部署），以及**不進 git** 的路名對照表
`road_gazetteer_20261116.sqlite`（約 53 MB，目前在 analytics worktree `.worktrees/realtime-incidents/data/processed/road_gazetteer/`，部署時改名 `road_gazetteer.sqlite`）。

## 來源與頻率

正本是 `fire119_parse.SOURCES`。cron 每分鐘一次，runner 內部依 `interval`／`phase` 決定這分鐘抓誰：

| 頻率 | 來源 | 排在哪一分鐘 |
|---|---|---|
| 1 分 | 臺中、新北（送醫圖層）、雲林 | 每分鐘，排最前面 |
| 2 分 | 臺北、高雄、臺南、彰化、基隆 | 奇數分鐘（較重的放這裡，避開垃圾車的偶數分鐘） |
| 2 分 | 新竹市、苗栗、嘉義縣、嘉義市、臺東 | 偶數分鐘（輕的） |
| 5 分 | 屏東、澎湖（分鐘 %5＝1）；南投、宜蘭（=2）；桃園、警廣（=4） | 不用 %5＝0（ship_ais 的 :x0）與 =3（isohe 的 :x8） |

單執行緒、來源間隔 2 秒、每次 timeout 20 秒、一般瀏覽器 UA。一輪超過 45 秒（`SOFT_DEADLINE_SEC`）就把剩下的來源留到下一分鐘。
被延後或在歸檔窗略過的來源，在下一個對到的分鐘補抓（排程以「本輪開始時間」計，不受輪內先後影響）；
超過 2×間隔＋50 秒沒抓才不看分鐘直接抓（保險）。苗栗網址已改 `https`、無 port。

## VM 現有排程佔用表（2026-11-16 唯讀查 crontab）

VM 規格：1 vCPU、記憶體 920 MB（當時 used 392／available 377）、swap 2 GB（已用 57 MB）。

| 時間 | 既有工作 | 長度 |
|---|---|---|
| 每偶數分 :00 秒 | waste_positions（垃圾車，01–06 點 quiet hours 自動跳過） | 數秒～十多秒 |
| :00 :10 … :50 | ship_ais | 數十秒 |
| :08 :18 … :58 | isohe_port_marine | 最長 480 秒（自我中止）＋timeout 540 |
| 每小時 :07 | immigration_apis_airport | 短 |
| 00:53 | moa_opendata `--all-due` | 最長 55 分鐘 |
| 03:00／03:05／03:10／03:15／03:20／03:21 | ship／waste／isohe／immigration／cdc／moa 歸檔 | 數分鐘 |
| 04:30 週一 | pulse-detect | — |
| 07:00 | external-health health_report | 短 |
| 週四 11:00 | cdc_public_health_weekly | 短 |

本支的錯開方式：

- **秒偏移**：`run_fire119.sh` 先 `sleep 25`，避開所有在 :00 秒起跑的收集器；監測腳本 `sleep 50`。
- **分鐘**：較重的 2 分鐘來源放奇數分鐘（垃圾車在偶數分）；5 分鐘來源不落在 :x0、:x3／:x8。
- **歸檔窗**：03:00–03:25 只抓 1 分鐘來源（臺中、新北、雲林），其餘延到 03:25 後補抓（`ARCHIVE_WINDOW`）。
- **本支歸檔**：03:33。

## 保護措施（站主 11-16 要求）

| 措施 | 做法 |
|---|---|
| 不重疊 | `flock -n -E 75 /run/lock/fire119.lock`；被占用就記一行 WARNING 跳過 |
| 時間上限 | `timeout -k 10 90`；runner 自己 45 秒軟期限 |
| 低優先 | `nice -n 10`、`ionice -c3`（idle） |
| 記憶體門檻 | 啟動先讀 `/proc/meminfo`，`MemAvailable < 150 MB`（`MIN_MEM_AVAILABLE_MB`）就跳過本輪並記錄 |
| 單行程上限 | 預設 `systemd-run --scope -p MemoryMax=200M -p MemorySwapMax=0`（cgroup 限實際記憶體、不准用 swap，超過由 OOM 殺掉該輪而不是拖垮整台）。VM 沒有 systemd 或 cgroup v2 時改 `FIRE119_MEM_MODE=ulimit`：`ulimit -v 400 MB`。`ulimit -v` 限的是虛擬位址空間（libssl、libpq 會先映射一大段），所以數字比實際用量大；CI 有一條 Linux 測試證明整條解析在 400 MB 位址空間內跑得完 |
| 自動降頻 | 上一輪沒正常結束（被 timeout 或 OOM 砍掉，runner 會在下一輪看到「開始但沒結束」的標記）或本輪超過 75 秒，各算一次逾時；**連續 3 次**就把所有間隔 ×2（最多 ×4），寫 `/var/lib/fire119/state/ALERT_degraded`，日報紅燈；連續 60 輪正常回升一級 |
| 不常駐 | 每分鐘一個短命行程；路名對照表走 sqlite 索引查詢（cache 2 MB），不載入記憶體 |

本機實測（macOS，19 個來源一次全抓）：一輪約 45 秒（其中 36 秒是來源間隔），峰值 RSS 約 60–70 MB。
正式排程每分鐘約 3–9 個來源，預估 10–25 秒。

## 寫入、隱私與保留

- **只寫變化**：本機狀態檔（`state/incidents_seen.json`、`pbs_seen.json`）記每個 key 的狀態、分隊、內容摘要；
  只有新 key 或有變化才 upsert。`status_history` 在 DB 端 append（`status_raw` 不同才加）、分隊取聯集、`first_seen_at` 不動。
  `last_seen_at` 每 30 分鐘一次批次 UPDATE（只往後推）。狀態檔只在資料「落地」（DB commit 或存進 buffer）後才更新。
- **重放安全**：DB 失敗 → `vm_buffer` 存檔，下輪先補寫；upsert 用 `WHERE EXCLUDED.updated_at > t.updated_at` 擋舊資料，
  同一批同 key 先去重；SQL 固定形狀（`bulk.py`，鐵則 R5），只用交易內 `SET LOCAL statement_timeout`。
- **逐列驗證**（對齊 mig 555）：exact／fuzzy／road 必有點、district／none 必無點；路名對不到降 district（只給鄉鎮代碼）、
  連鄉鎮都沒有降 none；座標不在台灣外框內就移除；`dest_geom` 只給新北；不合的列降級，不讓整批失敗。
- **精度來源**：臺北 `fuzzyAble=N` 的 `csX/csY`＝exact、`fuzzyAble=Y` 的 `fuzzyX/fuzzyY`＝fuzzy（0.0＝缺值）；
  其他來源只靠路名對照表（road＝路段中點，不是事發地）。臺北的點也查對照表拿 `road_key`／`town_code`（前端高亮整條路）。
- **隱私**：原始回應一拿到就剔除臺北 `fuzzyAble≠N` 的 `csX/csY/csPlace`、新北的 `caseList.path`（救護車軌跡）、
  `startPoint`（救護車目前位置）、`startPointKey`、`distance`；新北只留醫院名稱與座標、救護車代號。剔除後才寫本機。
- **清理**：`location_text` 以「測試」開頭的列丟掉；`received_at` 一律 +08:00。
- **去重 key**：新北 `ntpc:<featureId>`、臺南 `tainan:<受理日>-<編號>`（30 輪實測穩定）、其他 `sha1(source|received_at|地點|案類)` 前 16 碼。
- **本機保留**：原始回應（已剔除隱私）gzip 存 `/var/lib/fire119/raw/<來源>/YYYYMMDD/`，3 天後由歸檔腳本刪除、**不上傳**；
  解析後的列存 `/var/lib/fire119/data/{fire119,pbs_road_events}/YYYY/MM/DD/`（119 存每輪全部列、警廣只存有變化的列），留 2 天。
- **S3 正本**：`fire119/archives/YYYY-MM-DD.tar.gz`、`pbs_road_events/archives/YYYY-MM-DD.tar.gz`（永久）。

## 告警

runner 每輪寫 `/var/lib/fire119/state/source_health.json`，`external/_shared/health_report.py` 07:00 讀進 snapshot，日報顯示紅黃燈：

| 燈 | 條件 |
|---|---|
| 🟡 | 某來源 HTTP／解析失敗（目前連續失敗，或 24 小時失敗率 ≥ 10%） |
| 🟡 | 解析成功但連續 0 筆超過該來源門檻（臺中、臺北、高雄 3 小時；臺南、彰化、屏東 6 小時；其他 12 小時；警廣 2 小時）。**宜蘭、澎湖、臺東常態為 0，不告警** |
| 🟡 | 24 小時內有記憶體不足跳過、逾時；路名對照表檔案不存在 |
| 🔴 | 某來源連續失敗 ≥ 6 小時；runner 降頻中；狀態檔超過 15 分鐘沒更新（runner 停了） |

「解析失敗」和「0 筆」分開：每個解析器都先找結構標記（表頭、容器），找不到就算解析失敗，不會變成 0 筆。
collect.log 每輪恰好一行 `本輪結束 …`，health_report 用它算 24 小時輪數與成功率（`db=ok` 或 `db=none` 才算成功）。

## 部署步驟（🔴 需站主點頭；先套 gis-platform migration 555）

```bash
# 0. 本機：準備檔案（gazetteer sqlite 不進 git）
mkdir -p /tmp/fire119 && cp external/fire119_vm/{*.py,*.sh,.env.example} external/vm_common/{vm_buffer.py,bulk.py} /tmp/fire119/
cp ../taipei-gis-analytics/.worktrees/realtime-incidents/data/processed/road_gazetteer/road_gazetteer_20261116.sqlite \
   /tmp/fire119/road_gazetteer.sqlite
scp -r /tmp/fire119 <帳號>@<VM_HOST>:/tmp/
scp external/_shared/health_report.py <帳號>@<VM_HOST>:/tmp/health_report.py

# 1. VM：安裝（只裝檔案與套件，不排程）
ssh <帳號>@<VM_HOST>
bash /tmp/fire119/setup_vm.sh
#    編輯 /opt/fire119/.env：SUPABASE_DB_URL、S3_*（可從 /opt/waste-positions/ 的同名檔複製 key）
# 和 run_fire119.sh 同樣的旗標；MemorySwapMax 需要 cgroup v2，失敗就在 crontab 那行前加 FIRE119_MEM_MODE=ulimit
systemd-run --scope --quiet --collect -p MemoryMax=200M -p MemorySwapMax=0 /bin/true && echo "systemd 模式可用"

# 2. VM：手動跑一輪 dry-run（不寫 DB），看各來源筆數與耗時
cd /opt/fire119 && python3 fire119_collect.py --dry-run --all --deadline 120
#    確認 /var/lib/fire119/state/source_health.json 沒有 alerts，再清掉 dry-run 的狀態檔（避免正式第一輪以為都寫過）：
rm -f /var/lib/fire119/state/*.json && rm -rf /var/lib/fire119/dryrun

# 3. VM：手動正式一輪 → 查 DB 有列
python3 fire119_collect.py --all --deadline 120
#    （唯讀）select geom_precision, count(*) from live.fire119_incidents group by 1;

# 4. VM：排程＋logrotate＋health_report
bash /opt/fire119/setup_cron.sh                         # 收集＋歸檔＋第一天監測
#    /etc/logrotate.d/pulse-collectors 第一段加一行 /var/log/fire119/*.log，再 logrotate -d 驗證
install -m 755 /tmp/health_report.py /opt/external-health/health_report.py
python3 /opt/external-health/health_report.py | tail -20   # 看到 fire119 區塊、alerts 合理

# 5. 隔天：手動跑一次歸檔，確認 S3 有 fire119/archives/ 與 pbs_road_events/archives/
cd /opt/fire119 && python3 archive_fire119.py
```

另外（本機／主 agent）：S3 lifecycle 補 `archive-deep-270d:fire119` 與 `archive-deep-270d:pbs_road_events`
（`put-bucket-lifecycle-configuration` 是整份覆寫，先讀出、append、再寫回），並更新 `docs/AWS_INVENTORY.md`。

## 關閉第一天監測

```bash
tail -n 30 /var/log/fire119/vmstat.log                  # 看 avail、swap_used、elapsed、rss 有沒有異常
bash /opt/fire119/setup_cron.sh --no-monitor            # 重裝 crontab 區塊，只拿掉監測那行
```

`vmstat.log` 留著由 logrotate 處理即可。

## 回滾

```bash
bash /opt/fire119/setup_cron.sh --remove               # 移除整個 crontab 區塊（舊 crontab 備份在 /root/crontab.bak.*）
# 要清掉本機資料：先確認 S3 已有歸檔，再 rm -rf /var/lib/fire119
```

DB 表與 RPC 的回滾在 gis-platform `scripts/rollback_555_fire119_pbs_live_owner.sql`（另需站主拍板）。
暫停單一來源不必回滾：`.env` 設 `FIRE119_DISABLED=<來源名>`，下一分鐘生效。

## 驗收（照 handbook new-collector.md §3，VM 版）

1. 有跑：`tail /var/log/fire119/collect.log` 每分鐘一行 `本輪結束`；`cat /var/lib/fire119/state/last_round.txt`。
2. 解析成功：`source_health.json` 的 `alerts` 為空、各來源 `last_ok_at` 新鮮。
3. 有新資料：`select count(*) from live.fire119_incidents where updated_at > now() - interval '10 minutes'`（有 LIMIT 或有界聚合）。
4. 監控：隔天日報「HiCloud VM 健康」出現 `fire119`。

## 已知限制

- 本支沒有主 repo 對應的 `collectors/` 實作：`cross_layer_map.yaml` 以 `enabled: false` 登記、列在 `sync_cross_layer_map.NON_COLLECTOR_TASKS`；`realtime_tables.yaml` 兩列也是 `enabled: false`，**上線時兩處都改 true** 才會進日報新鮮度檢查（
  依 `last_seen_at`，30 分鐘批次推進）。VM 版不寫 `collector_status`。
- 非救護車型別的新北 feature（實測未出現）語意未確認，當無位置處理、不猜。
- `location_text` 保留來源原文（少數含門牌號），只給站主限定圖層用；要不要去門牌待站主決定。
