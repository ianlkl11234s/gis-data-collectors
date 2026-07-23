# Collector monitoring：綠燈優先（2026-07-22）

## Scope and safety

The monitoring path is **read-only**: it queries Supabase freshness, S3 object metadata, and VM health JSON. It never changes collector data, schedules, deployments, buffers, or credentials. The local notification state contains only sanitized fingerprints and can be reset by deleting `/opt/data/home/.hermes/state/gis-collectors-monitor/incidents.json`.

## Status contract

- **🟢 green** — All verifiable services are healthy. Event-driven, hash-deduplicated, and explicitly disabled reference tables are expected conditions, not errors.
- **🟡 watch** — A transient monitoring/S3 failure, or an archive before its Taipei 04:00 completion deadline. A repeated temporary condition is visible without being declared a collector outage.
- **🔴 critical** — A confirmed freshness/availability incident. A transient Supabase or S3 availability failure escalates only after three consecutive runs.
- **⚪ unknown** — Monitoring cannot establish a service state; this must not be called green.

## Discord delivery

- The 30-minute job is script-only. It prints nothing—and therefore sends nothing—when there is no state transition. It sends once for a new incident, escalation, or recovery.
- The 4-hour job produces a compact green summary when healthy and lists only actionable watch/critical evidence otherwise.
- Neither job is permitted to restart, redeploy, write data, or change collector configuration.

## Health snapshot performance

The snapshot now calls `public.health_snapshot` (gis-platform migration `306_health_snapshot_public.sql`): a two-tier probe (2-day window with partition pruning + full-table fallback) that keeps `count_24h` as a real value while cutting a single call from ~29 min / 12 GB read to seconds. The originally proposed `296_health_snapshot_max_only.sql` (NULL-ing `count_24h`) was superseded — `count_24h > 0` is used as a liveness signal, and the `realtime` schema lost CREATE for `postgres` on 2026-07-23 (42501), so the old function can no longer be replaced in place. See ADR-0009.

To re-verify RPC cost, run three bounded probes:

```bash
uv run --with-requirements requirements.txt python scripts/diagnose_health_snapshot.py --statement-timeout-ms 15000
```

Record only the sanitized JSON timing/error-type output. After migration, the acceptance target is three successful bounded runs; do not deploy/restart collectors as part of this verification.
