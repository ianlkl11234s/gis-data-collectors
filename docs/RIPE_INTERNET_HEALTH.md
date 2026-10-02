# RIPE Atlas + RIS Live internet-health collectors

Status: **Atlas and RIS Live are production enabled at 5-minute cadence and
remain internal-only. Repo defaults remain disabled for both.**

These are evidence collectors.  They do not decide that Taiwan is normal,
degraded, or offline.  RIPE Atlas and RIPE RIS are separate technical signals
but share `independence_group=ripe_ncc`, so they cannot by themselves satisfy a
two-independent-organisation detector rule.

## Production truth (2026-08-31)

- Collector deployment `6a954164...` is `RUNNING`. Migration 383 registry,
  family/FK and public-exclusion gates passed apply, verify, fixture and
  readback. Service health returned HTTP 200 with the main loop and DB green.
- Atlas one-shot run `9d6d032c...` succeeded with 913 received, 56 written and
  0 rejected. Its create-only S3 smoke object read back at 62,888 bytes; raw
  envelope keys `1001`/`2001` and SHA-256 matched. The DB projected 8 fresh
  current rows, while the public RPC returned 0 rows as required.
- Atlas scheduled run `f97ce10e...` then succeeded with 916 received, 56
  written and 0 rejected. `RIPE_ATLAS_INTERNET_HEALTH_ENABLED=true` is the
  production override; Atlas remains internal-only.
- Zeabur Settings showed the RIS service `Replicas` spinbutton at 1; volume
  scaling was disabled and Overview reported 1/1. Production overrides are
  `RIPE_RIS_LIVE_ENABLED=true` and `RIPE_RIS_REPLICA_COUNT=1`. Restart logs at
  22:51:02 confirmed one RIS worker with the reviewed 15 subscriptions.
- The bounded 18-minute RIS smoke produced three 5-minute runs: one expected
  startup-partial run and two succeeded runs. Each succeeded window wrote six
  `unknown` observations; current was 6/6 fresh and public status/timeseries
  returned 0 rows. Two private-S3 gzip objects and their manifests passed full
  GET, SHA-256 and manifest readback, with 2,033 and 112 records respectively.
- By 2026-08-31 15:05 UTC recurring production had written three runs: one
  partial and two succeeded. Both succeeded windows wrote six `unknown`
  observations; current was 6/6 fresh at observed_at 15:05 UTC, and public
  status/timeseries remained 0 rows. Health returned HTTP 200/healthy with the
  main loop and DB green and breaker false. One automatic production archive
  and manifest passed full GET, SHA-256 and manifest readback with 1,292
  records.

## Reviewed roster hard gate

`config/ripe_internet_health.yaml` is versioned and contains no credentials.
Both collectors fail closed unless all of the following are true:

- `schema_version: ripe_internet_health_roster.v1`
- `review_status: approved`
- `internal_only: true`
- Atlas has at least one reviewed ping measurement with explicit probe IDs.
- RIS has a bounded reviewed prefix list (maximum 256) or origin-ASN list
  (maximum 64).  An unfiltered firehose is impossible through this config.

The committed `v2026-08-31.1` roster is approved for internal shadow only. It
contains the official built-in K-root ping measurements 1001 (IPv4) and 2001
(IPv6), plus the 2026-08-31 snapshot of public connected Taiwan probes: 87
IPv4 and 41 IPv6 probe/ASN pairs. No IP, prefix, hostname, description,
contact, or coordinate is retained. RIS is bounded to 15 origin ASNs
represented by at least two reviewed probes; it does not subscribe to a full
firehose. Roster drift is expected and must be reviewed/versioned rather than
silently auto-discovered.

## RIPE Atlas polling

- Collector: `collectors/ripe_atlas_internet_health.py`
- Default cadence: 5 minutes; 30-minute lookback aligned to closed 5-minute
  buckets, deterministic result dedup, and a DB sample-count guard (fewer
  samples never overwrite more).  RIPE Atlas latest results are cached for
  five minutes.  See「資料收集全流程」below (fixed 2026-10-02).
- Only finite IPv4/IPv6 country aggregates are written to
  `live.internet_health_source_runs` and
  `live.internet_health_observations`.
- NULL/timeouts remain missing and are never converted to zero.
- Raw API responses use BaseCollector local storage and the existing private
  daily S3 archive path `ripe_atlas_internet_health/archives/`.
- Public result reads do not require an API key.  `RIPE_ATLAS_API_KEY` is
  optional and must only be used for an explicitly reviewed private
  measurement.
- 2026-08-31 local official-API smoke first proved parser compatibility; the
  production one-shot and subsequent scheduled run are recorded above.

## 資料收集全流程（RIPE Atlas，2026-10-02 修正後）

> 背景：修正前每桶只剩桶尾約 100 秒的探針（IPv4 29–56／87、IPv6 20–29／41），
> 造成 IPv6 每 20 分鐘的鋸齒與不可信的即時值。調查見 mini-taiwan-pulse
> `docs/features/monitor-restyle/internet-health-reading.md` §2。

### 1. 來源

- RIPE NCC 內建 ping 量測 **1001（IPv4）／2001（IPv6）**，目標 K-root
  （`target_group: k_root_server`），量測輪次 **240 秒**（roster
  `interval_seconds: 240`）。公開結果，不需 API key。
- 探針 roster：`config/ripe_internet_health.yaml`，version `2026-08-31.1`，
  IPv4 87 支、IPv6 41 支（臺灣公開探針快照，只存 probe_id／ASN）。roster
  變動要走 review／版本號，不自動探索。
- 每支探針在 240 秒輪次內的相位固定，因此一個 300 秒桶內每支探針出現 1 或 2
  次；完整桶的「回報探針數」約等於當時實際在線的探針數（2026-10 約 IPv4 79–83、
  IPv6 39）。

### 2. 排程與部署

- Collector：`collectors/ripe_atlas_internet_health.py`
  （`RipeAtlasInternetHealthCollector`），由 `CollectorScheduler` 每 5 分鐘觸發
  （`config.py` `RIPE_ATLAS_INTERNET_HEALTH` 預設 5 分）。
- 部署：**Zeabur** 長駐容器（非 HiCloud VM）。repo 預設
  `RIPE_ATLAS_INTERNET_HEALTH_ENABLED=false`，正式環境以 env override 為 `true`。
- **merge 到 `main` 即觸發 Zeabur 自動部署**（`.claude/principles.md`、README
  「Push 到 main 自動部署」），所以 collector 修改一 merge 就上線。
- 原始 API 回應走 BaseCollector 本地＋私有 S3
  `ripe_atlas_internet_health/archives/`（`config/cross_layer_map.yaml`）。

### 3. 抓取窗與分桶規則

1. `started = now()`；`requested_to = floor(started / 300) × 300`
   ——也就是**仍在進行中的那一桶的起點**，該桶整個排除。
2. `requested_from = floor((requested_to − LOOKBACK) / 300) × 300`，
   `RIPE_ATLAS_LOOKBACK_MINUTES` 預設 30 → 每次重抓最近 6 個已結束的桶。
3. 以 `start=requested_from, stop=requested_to, probe_ids=<roster>` 呼叫
   `/measurements/{id}/results/`。
4. 每筆結果依 `timestamp` 落到 300 秒桶（`_bucket_bounds`），以
   (msm, probe, timestamp, af, type) 去重；**只輸出完整落在
   `[requested_from, requested_to)` 的桶**（`_normalize_results` 的
   window 參數；API `stop` 邊界是否包含不影響結果）。
5. 每桶每 AF 產 4 個 signal，`observed_at = window_end = 桶結束`：

| signal | value | sample_count |
|---|---|---|
| `probe_connectivity_ratio_ipv{4,6}` | 回報探針數 ÷ roster 探針數 | 回報探針數 |
| `ping_success_ratio_ipv{4,6}` | Σrcvd ÷ Σsent | 回報探針數 |
| `median_rtt_ms_ipv{4,6}` | 有收到封包探針的 avg 中位數 | RTT 樣本數 |
| `reachable_asn_ratio_ipv{4,6}` | 成功 ASN 數 ÷ roster ASN 數 | 成功 ASN 數 |

`metadata.expected_probe_count`／`expected_asn_count` 是 roster 的分母
（87／41、34／22），不是「實際在線數」。

為什麼不再延遲一個 240 秒輪次：`stale_after_seconds = max(900, 240×3) = 900`，
RPC 以 `source_updated_at + 900s` 判 stale。只排除當前桶時，最新一桶在下一輪寫入
前最老約 10 分鐘；再延 240 秒會逼近 15 分鐘門檻。遲到上傳的結果改由「30 分鐘
回看 × 守門」補齊：每一桶會被後續約 6 輪重抓，樣本只增不減。

### 4. 寫入與守門

- 一個 transaction 寫 `live.internet_health_source_runs`（run ledger，
  `requested_from/to` 為對齊後的值）與 `live.internet_health_observations`
  （`storage/supabase_writer.py` `internet_health_observation_upsert_sql`）。
- 衝突鍵 `(source, entity_type, entity_id, signal, observed_at)`。
  **僅對 `ripe_atlas_internet_health`** 加守門：
  `DO UPDATE ... WHERE COALESCE(EXCLUDED.sample_count,0) >= COALESCE(t.sample_count,0)`
  ——同一桶再寫入時，樣本較少的結果不會蓋掉較多的。Cloudflare／IODA／RIS 的
  `sample_count` 語意不同，維持原本的無條件覆寫。
- `live.internet_health_current` 由 gis-platform migration 379 的
  `AFTER INSERT OR UPDATE` trigger 投影（指向最新 `observed_at`；同 observed_at
  以較新 `collected_at` 為準）。守門擋下的 UPDATE 不觸發投影，current 維持較完整
  的那筆。**本修正不需要 gis-platform migration。**

### 5. 下游

- gis-platform RPC：`public.get_internet_health_status`（現值，migration
  379→383→384；384 開放 RIPE 8 個 Atlas signal 公開輸出）、
  `public.get_internet_health_timeseries`（時序，回傳 `sample_count` 與
  `metadata`）。
- mini-taiwan-pulse 前端：`src/data/internetHealthLoader.ts`（24H 5 分鐘原值、
  7D 30 分鐘、30D 2 小時，比率以 sample_count 加權）與
  `src/components/intel/monitor/TelecomStatusCard.tsx`（monitor-restyle 分支）：
  - 完整桶判定：回報探針數（ping_success／probe_connectivity 的 sample_count）
    ≥ **80% × 預期探針數**；前端預期值寫死 IPv4 79、IPv6 39（實際有回報數，
    不是 roster 的 87／41），見 `ATLAS_EXPECTED_PROBES`／`isCompleteProbeCount`。
  - v2 24H：每整點小時只取該小時內完整桶聚合成一點（`hourlyCompleteSeries`）；
    整小時沒有完整桶畫缺值。
  - 即時值：現值的探針數達門檻才用，否則改用 24H 內最近一個完整桶，都沒有就留空。
  - collector 修正後，新資料每桶應都達門檻；前端這層篩選可保留作防呆。

### 6. 已知限制

- **歷史殘缺資料不回補**（使用者 2026-10-02 決定）。修正部署前的桶仍是殘缺
  值；資料從「部署時間 ＿＿＿＿（UTC，部署後填）往前推 30 分鐘」起的桶才完整，
  即部署後第一輪 run 的 `requested_from` 之後。
- 7D／30D 的 Probe 回報率與可達 ASN 在跨過舊資料的期間仍偏低，直到舊桶滑出視窗
  （7D 約 7 天、30D 約 30 天後）。
- roster 分母是 2026-08-31 快照；探針自然下線會讓比率慢慢下降，不是網路異常。
- `RIPE_ATLAS_OVERLAP_MINUTES` 目前只寫進 run metadata，沒有功能作用；
  `records_written` 記的是送出的 observation 數，被守門擋下的不扣除（會高估）。
- 守門的 SQL 沒有 PG 層單元測試（只測組出的字串），**部署後第一輪 run 才真正驗證
  語法**。

### 7. 部署後驗證

```sql
SET statement_timeout = 15000;
-- (a) 最近 run 成功、窗口已對齊 300 秒
SELECT started_at, status, error_code, requested_from, requested_to,
       extract(epoch FROM requested_to)::int % 300 AS to_mod,
       records_written
FROM live.internet_health_source_runs
WHERE source = 'ripe_atlas'
ORDER BY started_at DESC LIMIT 3;

-- (b) 每桶回報探針數回到預期附近
SELECT to_char(observed_at AT TIME ZONE 'Asia/Taipei', 'MM-DD HH24:MI') AS bucket_end,
       max(sample_count) FILTER (WHERE signal = 'probe_connectivity_ratio_ipv4') AS n4,
       max((metadata->>'expected_probe_count')::int) FILTER (WHERE signal = 'probe_connectivity_ratio_ipv4') AS exp4,
       max(sample_count) FILTER (WHERE signal = 'probe_connectivity_ratio_ipv6') AS n6,
       max((metadata->>'expected_probe_count')::int) FILTER (WHERE signal = 'probe_connectivity_ratio_ipv6') AS exp6,
       round(max(value) FILTER (WHERE signal = 'ping_success_ratio_ipv6')::numeric, 3) AS ps6
FROM live.internet_health_observations
WHERE source = 'ripe_atlas' AND observed_at > now() - interval '2 hours'
GROUP BY observed_at
ORDER BY observed_at DESC
LIMIT 48;
```

驗收：(a) `status = succeeded`、`to_mod = 0`；(b) 部署後的桶 n4 約 79–83、
n6 約 39（皆 ≥ 0.8 × 前端預期 79／39；相對 roster 分母 87／41 約 0.9 以上），
連續桶不再出現 10→17→21→17 的 20 分鐘循環，ps6 穩定在約 0.85–0.90。

## RIPE RIS Live worker

- Worker: `workers/ripe_ris_live.py`; registry shim is persistent and never
  enters the polling scheduler.
- Subscription always sends `type=UPDATE`, `includeRaw=false`, and
  `acknowledge=true`; it waits for every `ris_subscribe_ok` before treating the
  connection as usable.
- Application ping/pong, idle timeout, and full-jitter exponential reconnect
  are enabled.  Reconnect has no replay cursor in the official RIS Live
  protocol.
- Every message is flushed to a local durable NDJSON spool before aggregation.
  Fifteen-minute gzip objects and their manifests are uploaded to private S3;
  HEAD/SHA-256/manifest GET readback must succeed before the local retry copy is
  deleted.
- Any startup interval, reconnect, missing ack, pong timeout, idle timeout, or
  process interruption marks the entire 5-minute window partial.  Its metrics
  are NULL and `reported_status=unknown`.
- `prefix_visibility_ratio_*` remains NULL until a separately validated RIB
  snapshot/reconciliation contract exists.  RIS Live updates alone cannot
  initialize complete route visibility.
- 2026-08-31 local official WebSocket smoke for AS3462 first proved the
  subscription acknowledgement and pong contract. The later bounded production
  smoke crossed startup partial, two complete windows and automatic spool
  rotation; its DB/current/private-S3 evidence is recorded above.

### Single-replica hard gate

Enabling requires the Zeabur service to have exactly one replica and
`RIPE_RIS_REPLICA_COUNT=1`.  A process-local `flock` prevents two worker
threads/processes in one container.  **There is no distributed lease across
replicas.** Do not scale this service above one replica while RIS is enabled.

Zeabur's June 2026 HA feature allows multiple replicas inside one logical
service. CLI service count and RUNNING deployment count therefore do not prove
the runtime replica count. The production gate was satisfied from the Zeabur
Settings `Replicas=1` spinbutton plus Overview 1/1; keep that value at one for
as long as RIS is enabled.

## Production enable checklist

1. Platform source registry/FK/public-exclusion migration is live and verified.
2. Reviewed roster is approved in Git; no secrets are embedded.
3. Atlas exact-runtime fetch/normalize, DB/current and private S3 readback are
   verified; the 5-minute production schedule is enabled.
4. RIS exact-runtime bounded WebSocket receives all subscription acks and pong;
   message rate and spool growth remain within limits. **Verified.**
5. Zeabur replicas=1 and `RIPE_RIS_REPLICA_COUNT=1` are independently verified.
   **Verified from Settings plus Overview.**
6. RIS complete 5-minute DB windows plus gzip/manifest/S3 readback succeed.
   **Verified by the bounded 18-minute production smoke.**
7. Recurring collection remains internal-only. **Enabled; recurring DB/current
   and automatic private-S3 archive evidence are verified.**

Rollback is setting each `*_ENABLED=false` and restarting the service.  The RIS
worker gracefully closes/rotates its spool.  Never delete unverified local raw,
S3 objects, or existing DB evidence as part of rollback.

## Primary documentation

- RIPE Atlas REST API: <https://atlas.ripe.net/docs/apis/rest-api-manual/introduction/>
- Results/latest caching: <https://atlas.ripe.net/docs/apis/rest-api-manual/measurements/results-and-latest/>
- Result format: <https://atlas.ripe.net/docs/apis/measurement-result-format/>
- RIPE Atlas terms v3.5: <https://www.ripe.net/about-us/legal/ripe-atlas-service-terms-and-conditions/>
- RIS Live protocol: <https://ris-live.ripe.net/manual/>
- RIS commercial-use terms: <https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/commercial-use/>
- Zeabur HA replicas (2026-06): <https://zeabur.com/zh-CN/changelogs/high-availability>
