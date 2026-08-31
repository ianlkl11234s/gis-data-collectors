# RIPE Atlas + RIS Live internet-health collectors

Status: **Atlas production enabled at 5-minute cadence; RIS Live remains
internal-only, production disabled, and fail-closed.  Repo defaults remain
disabled for both.**

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
- RIS stays `RIPE_RIS_LIVE_ENABLED=false`. `RIPE_RIS_REPLICA_COUNT` remains
  fail-closed until the actual Zeabur replica count is proven to be exactly 1
  and the bounded 18-minute complete-window/automatic-archive smoke passes.

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
- Default cadence: 5 minutes; 30-minute lookback with deterministic result and
  DB-key dedup.  RIPE Atlas latest results are cached for five minutes.
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
- 2026-08-31 local official WebSocket smoke for AS3462 received the subscription
  acknowledgement and pong with `includeRaw=false` and no `ris_error`. This is
  not the required production complete-window/archive test.

### Single-replica hard gate

Enabling requires the Zeabur service to have exactly one replica and
`RIPE_RIS_REPLICA_COUNT=1`.  A process-local `flock` prevents two worker
threads/processes in one container.  **There is no distributed lease across
replicas.** Do not scale this service above one replica while RIS is enabled.

Zeabur's June 2026 HA feature allows multiple replicas inside one logical
service. CLI service count and RUNNING deployment count therefore do not prove
the runtime replica count. The gate requires the Dashboard/API's actual
`Replicas=1` value; only then run the bounded smoke for at least 18 minutes so
it crosses the startup-partial bucket, one complete 5-minute bucket and one
automatic 15-minute spool rotation.

## Production enable checklist

1. Platform source registry/FK/public-exclusion migration is live and verified.
2. Reviewed roster is approved in Git; no secrets are embedded.
3. Atlas exact-runtime fetch/normalize, DB/current and private S3 readback are
   verified; the 5-minute production schedule is enabled.
4. RIS exact-runtime bounded WebSocket receives all subscription acks and pong;
   message rate and spool growth remain within limits.
5. Zeabur replicas=1 and `RIPE_RIS_REPLICA_COUNT=1` are independently verified.
6. RIS one complete 5-minute DB window plus gzip/manifest/S3 readback succeeds.
7. Only then enable recurring collection; keep both sources internal-only.

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
