# 新增 collector：設計、上線、驗收

一句話：**先答完 12 題設計檢查，再照下面一份步驟清單跨 repo 接線，最後用「有跑、解析成功、有新資料、表級監控」四件事證明它真的在跑**。漏任何一步都會斷鏈，而且多半是默默漏報，不會立刻爆炸。

本檔是「新增 collector 上線步驟」的唯一正本（使用者 2026-10-16 決定）。舊的 `.claude/CLAUDE.md` 12 步、根目錄 README 10 步、taipei-gis-analytics `docs/DATA_LIFECYCLE.md` §5.2 都已改成連到這裡（analytics PR #171）。資料該不該做、走動態／半動態／靜態哪條路，仍以 analytics `docs/DATA_LIFECYCLE.md` 的分流決策為準。

---

## 1. 設計前必答的 12 題

改寫自 gis-wiki `inbox/2026-10-15-gis-data-engineering-blind-spots.md` §3（三輪 Disk IO 優化回頭看整理出的盲點；心智模型全文看那張卡）。每題都對應到一次真的發生過的事。

| # | 問題 | 答不出來時會發生什麼（實例） |
|---|---|---|
| 1 | **寫入模式**：只追加，還是會更新？每輪多少列？多久一輪？ | `*_current` 表每輪全量 UPSERT，表膨脹 33–35 倍（2026-09-04、10-15 bus_current） |
| 2 | **自然鍵**：用什麼判斷「同一筆」？重送或輪詢時間窗重疊時會不會變成重複？ | 垃圾車 GPS 臺南軌跡重複約 2.5 倍（gis-platform mig 458）；唯一約束含恆為 NULL 的欄位，ON CONFLICT 永遠不觸發，公衛週報灌水 13 倍 |
| 3 | **沒變也寫嗎？** 哪些欄位是「內容」、哪些是「心跳」（收到時間）？ | marine 35 小時 1,140 萬次更新、實際新增 4.6 萬；TLE 95% 沒變。反例：`road_events_current.collected_at` 是心跳，不能套「沒變不寫」 |
| 4 | **熱冷分層**：現況、原始、彙總、封存各放哪？各留多久？ | ship_current 79% 的船超過 7 天沒出現（mig 460 熱冷分離） |
| 5 | **過期方式**：能不能用刪整個分區（DROP PARTITION）？還是只能 DELETE？ | DELETE 不會把磁碟還給 OS，只能 VACUUM FULL 縮檔 |
| 6 | **主鍵順序**：跟新資料進來的順序一致嗎？（時間欄位放前面） | iot_wra 主鍵不是時間開頭，寫入散在上千個索引頁，每天讀 5.7 GB（mig 456） |
| 7 | **索引**：每顆索引對應哪一個查詢？有沒有索引到會常變的欄位？ | ship_current 的 geom 索引讓 HOT 更新只剩 3.7%（mig 453 刪除後 97.8%） |
| 8 | **fillfactor／autovacuum**：高頻更新的表有沒有預留空位、調清理門檻？ | mig 451／452 |
| 9 | **衍生資料**：下游彙總的重算成本會不會隨時間變大？能不能增量？晚到資料怎麼處理？ | ship trails 全量 243 秒，改增量後 3 秒（mig 442／443／459） |
| 10 | **排程**：有沒有對齊上游「寫完」的時間？有沒有避開其他重的工作？ | collector 落地分鐘會隨部署漂移（10-15 衛星從 :27 變 :58），固定相位的 cron 會錯過 |
| 11 | **讀取端**：結果每個人看都一樣嗎？能不能做成快照共用（R2／CDN）？ | — |
| 12 | **監控**：有沒有跑、有沒有解析成功、有沒有新資料，三件事分開看並各自告警 | 乾旱燈號 current 表 100 多天沒寫入，心跳仍正常，沒人發現 |

另外三題是本 repo 特有、上線前也要先答：

- **來源擋不擋雲端 IP？** 本機 curl 正常、Zeabur 連線逾時，多半是 IP 被封鎖 → 走 VM 分支（§4）。
- **上游會不會「凍結但仍回 200」？** 要能比對資料內的來源時間，不能只看 HTTP 狀態（見 [principles.md](principles.md)）。
- **缺值怎麼存？** 一律 NULL，不可當 0（休市字串、-99、缺幀都踩過）。

設計原則的完整清單與出處在 [principles.md](principles.md)；事故長文索引在 [pitfalls.md](pitfalls.md)。

---

## 2. 上線步驟（合併版，以程式為準）

順序原則：**上游先動、下游後動**；表與保留期先建好，collector 才開。

### A. gis-platform（建表）

1. **`migrations/NNN_{name}.sql`**：建表＋索引＋RLS＋保留期，同一支 migration 做完。
   - **保留期**：在 `metadata.retention_policies` **INSERT 一列**（gis-platform mig 282 起的唯一正本）。
     - 分區表：`kind='partition'`，由 `cleanup_expired_partitions()` 自動讀註冊表清理。
     - 非分區表：`kind='delete'`＋`time_column`，另排一支 pg_cron 呼叫 `cleanup_rows_batched` procedure（schema 經過 live 遷移，以 gis-platform 現行 migration 為準，例如 mig 313）。
     - 刻意永久保留：明寫 `retention_days = NULL` 並在 note 說明原因。
     - **不要**再 `CREATE OR REPLACE` 清理函式、不要改「硬編碼清單」——那是 mig 282 之前的舊做法，mig 220–222 就是因此漏掉 4 張表、DB 長到 52 GB。
     - 天數怎麼定：至少是前端／RPC 實際回看的窗口再加 margin（例：急診 14 天卡片要留 30 天）。手冊不抄天數，查 `metadata.retention_policies` 或對應 migration。
   - **RLS**：建在 API 暴露 schema（live／reference／spatial…）的表，同一支 migration 就要 `ENABLE ROW LEVEL SECURITY`＋anon／authenticated 的 SELECT policy（範本 gis-platform `migrations/418_funeral_enable_rls.sql`）。漏開會收到 Supabase `rls_disabled_in_public` 告警。
   - **schema**：新即時表建在 `live`，RPC 建在 `public`（ADR-0010）。
   - **自然鍵／唯一索引**：如果要加唯一索引去重，**先部署 collector 的冪等寫入（`ON CONFLICT DO NOTHING`），再建唯一索引**；反過來 collector 會撞唯一鍵整批失敗（Disk IO 第三輪 §6）。
   - migration 取號：當天 `git fetch` 後再定號，避免撞號。
2. **`docs/data-inventory.md`**：資料清冊加一列（含「部署位置」欄：Zeabur／HiCloud VM／Disabled）。
3. **`docs/data-safety-inventory.md`**：加進動態或靜態表清單，標 S3 archive 起始日與健康度。

### B. data-collectors（本 repo）

4. **`collectors/{name}.py`**：繼承 `BaseCollector`；TDX 來源一律用 `utils/tdx_session.py` 的 `TDXSession`（自動經過全域節流），不要自己開 `requests.Session()`。
5. **`collectors/registry.py`**：`COLLECTOR_REGISTRY` 加一筆（`CollectorEntry` 欄位只有 class、display_name、config_prefix、`required_env`、`persistent`）。DB 寫入失敗要讓該輪失敗的 collector，在 collector 類別覆寫 `BaseCollector.require_db_write()` 回傳 `True`（不是 registry 欄位）。`required_env` 要在 registry 和 config 兩邊都宣告，否則會被靜默略過（`.claude/pitfalls/2026-07-26-required-env-silent-skip.md`）。
6. **`config.py`**：`_COLLECTOR_TOGGLES` 加一筆 `(prefix, enabled_default, interval_default)`；**預設 `enabled=False`**。額外變數（城市清單、API key 名稱）另外宣告。
7. **`storage/supabase_tables.py`**：要寫 Supabase 就加 `TABLE_MAP[name]`。
8. **`storage/supabase_writer.py`**：加 `_transform_{name}` 並註冊進 `TRANSFORMERS`。
9. **`config/cross_layer_map.yaml`**：跑 `python3 scripts/sync_cross_layer_map.py` 自動補缺的條目，再**人工改**它填的 `enabled`／`deployment`（腳本只填 repo 預設值，不是正式環境實況）與標 `TODO(sync)` 的欄位。漏寫日報會完全不掃這個 collector。
10. **`config/realtime_tables.yaml`**：每張 history／current／多表都要列，漏寫監控 RPC 撈不到。
11. **`config/backup_manifest.yaml`**：會寫 JSON 進 S3 的 collector，把它寫入的每張 `schema.table`（完整表名，例如 `live.xxx`，不是 collector 名）逐一加進 `archive_py_covered`（避免備份重複 dump）。
12. **`.env.example`**：新環境變數寫上**名稱**與說明（不寫值）。
13. **手冊**：在 `scripts/gen_handbook_catalog.py` 的 `SOURCES_DOC` 登記這個 collector 屬於哪份 `sources/*.md`，跑 `python3 scripts/gen_handbook_catalog.py` 重產 [collectors.md](collectors.md)，並在對應的 `sources/*.md` 加一段。
14. 跑 `python3 -m pytest tests/`：`tests/test_cross_layer_sync.py` 擋 registry／config／cross_layer_map／realtime_tables 之間的 drift；`tests/test_handbook_catalog.py` 擋手冊清單沒重產或沒登記分類。

### C. S3 冷儲存

15. **補一條 S3 lifecycle 規則 `archive-deep-270d:{name}`**（Prefix `{name}/archives/`，270 天轉 DEEP_ARCHIVE），否則歸檔停在 GLACIER_IR。`put-bucket-lifecycle-configuration` 是**整份覆寫**：先讀出現有設定、append 一條、再整份寫回，不可只送新規則。S3 不設 expiration。做完更新 [`docs/AWS_INVENTORY.md`](../AWS_INVENTORY.md)（規則全貌的權威紀錄）。

### D. taipei-gis-analytics

16. **`docs/data-sources.md`** 更新狀態、**`docs/data-registry.yaml`** 加 entry（含 lifecycle／platform_target）。上游 API 的行為與踩雷寫在 analytics `docs/api-platforms/`，不寫在本 repo。

### E. 部署

17. 開 PR → CI 綠 → 合併（Zeabur 自動部署）。
18. 在 Zeabur 設 `{PREFIX}_ENABLED=true`（必要時 `{PREFIX}_INTERVAL`），接著做 §3 驗收。環境變數在**執行中的容器**上改，要 restart 才生效。


---

## 3. 驗收：證明它真的在跑

任何一項沒綠都算未上線。四件事分開看——心跳正常不代表有資料（三大健康盲點見 [operations.md](operations.md)）。

| # | 要證明的事 | 怎麼看 | 常見假象 |
|---|---|---|---|
| 1 | **有跑**：正式環境載入了新程式與新設定 | `zeabur service exec` 進容器讀 `config.{PREFIX}_ENABLED`，並確認 `COLLECTOR_REGISTRY` 裡有這個 prefix（驗證是新 image）；重依賴也在容器內 import 一次 | dashboard 顯示已設定，但容器沒 restart，實際還是 `False`；容器缺套件時 collector 靜默失效 |
| 2 | **解析成功** | collector log 有成功訊息；`metadata.collector_status` 的 `last_success_at` 更新、`last_error` 為空 | `write()` 失敗只回 False，非 `require_db_write` 的 collector 這輪仍算成功；零筆時直接 return、不寫心跳 |
| 3 | **有新資料** | 直接查 DB：`select count(*) from live.{table} where <時間欄> > now() - interval '1 hour'`（一定要有 LIMIT 或有界聚合） | 判斷部署是否生效，要看「只有新版才會產生的內容特徵」，不看 `updated_at` |
| 4 | **表級監控有涵蓋** | 日報的跨層對照列出這個 collector（靠 `cross_layer_map.yaml`）；寫入新鮮度有這張表（靠 `realtime_tables.yaml`）；`select * from metadata.check_retention_coverage()` 沒列出新表 | 共用表時，A 源壞掉會被 B 源的新鮮度蓋過去；`check_retention_coverage()` 本身有盲區（mig 385 擴大後仍有） |

沒事可做的 collector（例如補缺型、pending=0）不會更新心跳，看起來像沒跑。這時可以在容器內直接跑一次 `collect()`（不寫 DB），或刻意製造一個缺口看它會不會自動補回。來源：mini-taiwan-pulse `.claude/memory/PLAYBOOKS.md` PB-33。

本機也可以先跑一輪：`python3 main.py`（啟動時每個啟用的 collector 會先提交一次，不必等第一個 interval；見 `main.py` 的 `run_collectors()`）。

---

## 4. VM 分支：來源只接受台灣 IP

### 何時放 VM

- 症狀：Zeabur 上連續 ConnectTimeout，本機（HiNet）`curl` 正常 → 多半是來源擋國際雲商 IP。先在 Zeabur 容器內測連線確認，不要靠 retry 硬撐。
- 已知案例：航港局 AIS、高雄／新北／台南垃圾車 GPS、疾管署、移民署、ISOHE 商港海氣象。CelesTrak 也封過 Zeabur，但衛星改用 Space-Track 解決，沒有上 VM。
- 判斷流程與完整 SOP 的正本：[`docs/EXTERNAL_COLLECTORS.md`](../EXTERNAL_COLLECTORS.md)。

### 和主流程的差異

| 步驟 | VM collector 要多做／改做的事 |
|---|---|
| 6 `config.py` | `_COLLECTOR_TOGGLES` 預設 `enabled=False`；**Zeabur 上對應的 `*_ENABLED` 一律維持 `false`**，否則雙跑，append-only 表會被重複寫入 |
| 9 `cross_layer_map.yaml` | `deployment: hicloud_vm`，`enabled` 依正式環境實況填 |
| 實作 | 主 repo 的 `collectors/{name}.py` 保留完整實作（schema 正本）；另建 `external/{name}_vm/` 單檔鏡像版，共用 `external/vm_common/`（DB 失敗時的本地 buffer）與 `external/_shared/`（VM health snapshot） |
| 部署 | 手動：從本機 `scp` 檔案到 `<VM_HOST>` → 跑 `setup_vm.sh` → 填 `.env` → 跑 cron 安裝腳本。**`git push` 不會部署到 VM**，改了 VM 版程式要再 scp 一次。各 collector 的步驟在 `external/{name}_vm/README.md` |
| 監控 | VM 每天 07:00 推 health snapshot 到 S3，由日報讀取；VM 本身沒有即時告警 |

VM 位址與登入帳號不放在本 repo（PUBLIC），見私人筆記。

### quiet hours

- VM collector 可用環境變數 `QUIET_HOURS` 讓 collector 在指定時段自己 skip（目前 waste_positions 設 `01-06`，因為垃圾車夜間不跑）。
- 要對 VM collector 寫入的表做維護（刪重複、建唯一索引、VACUUM FULL）時，排在它的 quiet hours 內，並先確認 log 出現 skip（Disk IO 第三輪 mig 458 就是這樣做）。
- Zeabur 端也有類似機制：公車夜間 00:00–04:59 只留 5 都（`docs/BUS_NIGHT_PAUSE.md`）。

---

## 相關

- 為什麼這樣設計：[principles.md](principles.md)、[history.md](history.md)
- 架構與寫入層：[architecture.md](architecture.md)；保留期與冷儲存：[data-lifecycle.md](data-lifecycle.md)
- 各類來源怎麼收：`sources/` 底下各檔
