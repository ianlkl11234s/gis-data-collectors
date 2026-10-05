# Collector health 唯讀調查 — 2026-10-05

## 結論與證據界線
- 調查 collector 基準與 Zeabur RUNNING commit 都是 `2759d83f2357ad7f6a91ca2be59fc25dcb8e019b`；platform 原始碼基準 `94ee3665f0628639896e03ee60b8c9a2402da522`，live function definition 另行查驗。
- 正式 Supabase 僅 SELECT/catalog/有界查詢；未呼叫會發 Telegram 的 `check_collector_freshness()`、refresh、cleanup 或 heartbeat。未 migration、回填、push、merge、部署、設定變更或上傳。
- 時間為 2026-10-05 12:43 起 Asia/Taipei；原始 DB timestamps 為 UTC。台灣白天禁重查詢，因此 14 張大型／無 leading-time index 表未全掃，逐項標 DEFER_HEAVY，不能當成 OK。完整 109 entries 的盤點見 [full-manifest-audit](evidence/full-manifest-audit.md)。
- 71 個 owner 同時刻 join：62 FRESH、5 STALE、4 MISSING。歷史「13 個」不是本次可重現數量；不拿不同時間快照湊數。本次沒有修復心跳，因此 DIO-4 驗收未通過。
- 唯一程式 patch 為明確要求的 TD-1，尚未正式驗證或部署。其他項目只調查／建議。

## DIO-4：心跳與實際寫入脫鉤
同時刻查詢與每 owner 結果：[owner-join.json](evidence/owner-join.json)。原始 status：[collector-status.json](evidence/collector-status.json)。表最新資料：[scan2.json](evidence/scan2.json)。詳細源碼路徑：[code-audit-evidence.md](code-audit-evidence.md)。

|owner|根因分類／實證|建議修法與影響|
|---|---|---|
|ship_ais|6/06 舊 generic 心跳；ship_positions 今天 04:40 UTC 有列；HiCloud 獨立寫入繞過 generic writer|VM 成功寫入後回報同名 heartbeat；不可把 stale status 當來源停擺|
|waste_positions|6/06 舊心跳；spatial.waste_positions_realtime 今天 04:44 UTC；獨立寫入路徑|成功／空資料／失敗各記 run health；資料位置與城市語意不改|
|cdc_public_health_weekly|6/16 舊心跳；資料最新 10/01 03:00 UTC；VM 週期寫入旁路|VM 同步 generic 或明確 external health；影響週報的 false-stale|
|aisstream|無同名 status；專用 ingest_health、vessel_current 今天仍推進|persistent worker 加同名 heartbeat 或 manifest 明示專用 health；不要把 started_at 當每輪成功|
|flight_fr24|owner 名不匹配：實際 status 為 flight_fr24_zone；flight_positions 今天有列且多來源共用|改 owner 或建立可觀察 alias；共用表 max 不能证明 FR24 單一來源正常|
|gfw_vessel_presence|無 status、4 legacy 表皆空；manifest 明示 intentionally disabled/replaced|退役 legacy monitoring、沿用 verified hourly publisher health；不要啟用權利受限 archive|
|air_ticket_radar|無 status，registry/toggle/cross-layer 亦無 owner；fare_offers 停在 9/08|先確認 external owner，再補 run health／retire；不能憑監控名稱宣稱 collector 正常|
|earthquake_shakemap_grid|status 與資料均 8/26；事件去重可能不寫，不能單憑 age 判 dead|獨立 polling receipt 與 event freshness；保留 no-new-event/unknown|
|wra_drought_alert|status/data 同停5/15；零 alerts／hash skip 提前 return，見 DIO-6|polling、parse validity、current replacement 三者分開|

建議把 owner→status/專用 ledger 對應與退役狀態納入反向 contract test。所有 owner 必須能查健康，但 retired/event-idle 不應被捏造為 fresh。歷史13個完整原清單不在交接文件中，無法逐一對照過往13身份；本次已逐一對照全部71 owner。

## DIO-5：不存在的 created_at
根因：`config/realtime_tables.yaml:58` 以 created_at 監控 archive ledger，但 live 表沒有該欄。live 有 uploaded_at、verified_at、period_start/end；最近5列均 verified，最新 uploaded_at/verified_at=10/05 04:28:38 UTC。[archive-rows.json](evidence/archive-rows.json)、[columns.json](evidence/columns.json)及失敗查詢 [scan.json](evidence/scan.json)。
建議把 archival freshness 欄改 uploaded_at；另外檢查 status=verified 與 verified_at，不能只因 upload attempt 新鮮就當耐久保存成功。period_end 表示封存內容時間，不是 DB heartbeat。影響：移除每日無效欄位 error，無需刪表或 schema migration。本任務未改設定。

## DIO-6：乾旱舊燈號與漏警
實證：[drought-rows](evidence/drought-rows.json)仍有新竹／台中兩列、published_date=4/27、fetched_at=5/15；公開頁 HTTP200，純 parser（未載入 config/credential）讀得 published_date=5/26、alerts=[]：[drought-parser](evidence/drought-parser.json)。
根因鏈：`collectors/wra_drought_alert.py:191-203` 零 alerts／unchanged 不產 records，`storage/supabase_writer.py:185-189` 提前成功 return，跳過 `:202` generic heartbeat；沒有刪除已撤下區域的 current snapshot replacement。不能從零 alerts 直接認定全台正常，尚須確認有效空狀態與 selector validity。
3x 規則沒有失效：43200分鐘×3=90天；5/15到本次約142.8天，應為 STALE，尚未達360天 DEAD（`tasks/monitoring.py:184-207`）。Python 日報可建立 STALE anomaly，但 DB 告警 `metadata.check_collector_freshness` 只看所有 enabled collector 的 MAX(last_success_at)，任一 collector 持續寫入便掩蓋 drought。
live definition：[functions.json](evidence/functions.json)。cron最近5次 succeeded：[collector-cron-runs.json](evidence/collector-cron-runs.json)。日報 D1/D3/D7 持續異常節流與前8／6項截斷會影響可見度（`tasks/daily_report.py:875-943`）；未取到完整歷史 Telegram delivery/anomaly-state，因此「從未發過」不能證明。
建議：每輪 parse/run receipt、合法空 snapshot 明示與舊 current 清除策略、每 owner 告警；未驗證空來源一律 unknown。影響：避免舊限水區域永久殘留與 false success；任何正式資料 replacement 需使用者拍板。

## WA-2：A1 跑了，固定上游沒有新事故
上游 HTTP200，1717 records，資料提供日期115年06月22日，事故日期20260101–20260615：[upstream-a1](evidence/upstream-a1.json)。DB collected_at 最新6/27，occurred_at最新6/14 21:41 UTC，即台灣6/15：[a1-latest-event](evidence/a1-latest-event.json)。status 今天10/05 03:27 UTC 成功、run_count282/error_count2。
`collectors/npa_traffic_accident_a1.py:90-153` 正常解析累積年度 feed；`storage/supabase_tables.py:342-355` hash conflict DO NOTHING，所以既有資料不推進 collected_at。根因較符合固定舊資源＋去重，不能解讀「無新事故」為今年沒有新事故。成功心跳也不保證新增列數。
建議維持去重，增加 source提供日期、max事件日、source rows與新增rows的 receipt；以來源停更/coverage警示，不標目前即時A1完整。更換來源前核對官方新資源與schema。影響：沒有資料覆寫風險，需校正產品對來源時效的期待。短期 Zeabur log未涵蓋該12小時collector，不能聲稱取得每次run歷史log。

## TD-1：已做本地 patch，部署未做
原 default1440 relative interval；`main.py` 啟動即跑後 interval schedule，每次restart重置相位。修改 `config.py:291` default240；環境若覆寫 RAIL_TIMETABLE_INTERVAL，patch default 不足以改 runtime。本次禁止 variable 指令，未驗證正式覆寫值。
`storage/supabase_writer.py:885-906` fallback 保留 tra_daily，以 JSONB metadata.degraded/degraded_reason 和 raw_schedules 保存原始資料；`:3377-3398` degraded upsert 不覆蓋既有健康同日row，後續健康結果可修復。未新增 DB 欄位。
風險：degraded 無 converted schedules，delay SQL不crash但可能把列車判為不在班表；Portal需明示 degraded。此 patch 是保存／重試修正，不能保證正常轉換及下游品質。[td1-evidence](td1-evidence.md)有詳細邊界。
live4/02仍有 system=tra、919 trains；6/26查無班表、10/04 tra_daily925 trains；本次中午尚無10/05列：[target-rows](evidence/target-rows.json)。
回填只寫方案：4/02用 orphan raw dated payload轉換，6/26先列查並驗證 dated S3 archive再轉換；核對日期、train count、完整stop contract、source lineage後才upsert tra_daily；驗證後才考慮移除orphan。禁止用Today重建舊日。本次未讀S3 archive、未執行回填。
兩manifest expected_interval仍1440，代表現行日級資料健康；正式若改240 cadence，監控門檻與runtime override需一起拍板，避免本地尚未部署時聲稱正式4小時更新。

## BL-25
- YouBike：public.mv_youbike_h3_dates 最新4/09，但 live.youbike_h3_daily 已到10/05（150 payload）；live refresh cron成功，MV卻沒有refresh job。根因為日期MV與新聚合管線脫鉤，建議恢復受控refresh或改日期RPC直讀day聚合；影響是有資料但日期入口漏日。
- Flight：9/26 summary仍報52,536 records／5,351 flights，同日raw trails=0；9/27–10/05 summary與raw一致。cleanup只刪raw，summary未同步。建議retention同步清summary或RPC過濾實際存在日期，影響是移除虛假可回放日期。
- Waste：10/05 raw trails 新北52、臺南87、高雄105；matched三市皆0。matching worker設定停用且預設只高雄；合法raw不等於map-matched coverage。建議先明示matched不可用，再拍板OSRM容量／城市範圍；不可把舊「臺南沒有資料」當現況，城市filter須用中文。
- H3：live get_h3_demographics_yearly已無20000上限；113年res7=8,084 cells、res8=0。limit缺陷已非現況，但不能宣稱最新年度res8有coverage。
完整recent row、SQL／EXPLAIN／cron與源碼行號見 [transport-evidence.md](transport-evidence.md)；此調查不修RPC、materialization或retention，亦無browser readback。

## BL-7：過去停止已不再成立
`live.reservoir_daily_ops` 最新 observed_at=10/03、collected_at=10/04 15:28 UTC；status同次更新成功，run_count322/error_count0：[reservoir-rows](evidence/reservoir-rows.json)及status。collector仍registry排程，cron.job沒有reservoir ingestion job；它屬Zeabur scheduler，不是pg_cron。
公開WRA endpoint本機probe SSLError：[upstream-reservoir](evidence/upstream-reservoir.json)，不能把本機憑證失敗當Zeabur失敗。短期runtime log沒有該日級run，無法重建4/23歷史停止原因。不得宣稱找到歷史復原日期。
程式仍有非list 2xx→[]被當成功的latent風險（`collectors/water_reservoir_daily_ops.py:67-71`），建議schema/parse fail-closed及source_date receipt；目前無需為本次調查重啟。影響：防止未來 false-success，不代表目前資料停止。

## 全 manifest 額外候選／限制
109 entries 已逐項做 contract/index/cost盤點；94表有原時間欄位實際latest probe、1表欄位錯誤、14表因白天重查詢限制未全掃。AIS ledger另以正確uploaded_at取得新鮮列。
額外候選：air_tickets.fare_offers（9/08，DEAD，owner未知）；earthquake_shakemap_grid（8/26，STALE，事件型未決）。GFW legacy四空表屬明示退役；internet_health_incidents空表是可能合法事件空資料；不列為已停事故。其餘已probe大多通過3x，但MAX新鮮不能證明source quality、某城市/某provider coverage或successful-run。
延後14表詳見 full-manifest-audit.md；包含534MB YouBike、848MB freeway、416MB AIS observations、410MB microsensor等（[remaining-cost](evidence/remaining-cost.json)）。可於非10:00–20:00再做有界source-specific indexed probe／query plan，禁在白天用完整health_snapshot fallback全掃硬湊驗收。

## 測試、原始證據與待拍板
- Focused：隔離環境 python3 -m pytest -q tests/test_supabase_writer.py tests/test_cross_layer_sync.py：35 collected／35 passed，5.23s；[pytest-focused.log](evidence/pytest-focused.log)。
- 初次全套592 collected：553 passed／37 failed／2 skipped，176.03s（缺ijson 10、OpenCC相關25、timing2）；[td1-pytest-full.xml](td1-pytest-full.xml)。
- 補ijson但誤用純Python OpenCC的全套：562 passed／28 failed／2 skipped，188.00s；25個OpenCC incompatibility及3個timing；[pytest-complete.log](evidence/pytest-complete.log)、[td1-pytest-complete.xml](td1-pytest-complete.xml)。
- 隔離 /tmp venv 改依 requirements.txt 的 OpenCC==1.4.2 後，僅重測失敗項：28 passed／12 deselected，16.59s；[pytest-recheck.log](evidence/pytest-recheck.log)。這是失敗項重測通過，不是第三次全套一次全綠。
- 測試runtime Python3.14；conftest credential/dotenv/network/DB guard啟用、沒有live Supabase option。SQL degraded guard只mock驗證，未進行disposable PostgreSQL runtime；正式fallback/downstream仍未部署驗收。
- git diff --check通過。JSON證據保存完整SQL與tool result，所有SQL有LIMIT或有界catalog聚合。公開HTTP只存schema/日期/計數，不存事故個人資料。
- Zeabur metadata：[zeabur-deployments.json](evidence/zeabur-deployments.json)；本次CLI只取最近約100行：[zeabur-runtime.log](evidence/zeabur-runtime.log)。log不覆蓋日級歷史run；沒有據此猜測過去事故原因。
- 待使用者拍板：TD-1上線及runtime interval確認；合法空 drought snapshot／清除旧current與每owner告警設計；A1官方新source接入；external owner心跳與retired entries調整；materialization/retention修復；歷史回填。正式DB變更、restart、Zeabur variable、部署與上傳均未執行。
