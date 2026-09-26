# GFW safe publication repair — 2026-09-18

Base: `3518569`, branch `codex/gfw-safe-publish-20260918`. Paired frontend branch: `codex/gfw-safe-refresh-20260918` in Mini Taiwan Pulse.

## Changes

- Preserve all successful processed/full-fidelity S3 releases. Root advertises current and rollback releases only; this index no longer authorizes archive deletion. Raw provider-response retention is unchanged.
- Immutable uploads use conditional creation, reject same-key different bytes, and verify actual GET bytes and SHA. Root uses ETag conditional replacement and rejects date regression. Conditional rollback never overwrites another publisher's newer root.
- Ambiguous root writes/readback retain spool for reconciliation without falsely marking failed or succeeded. Failed spool is no longer automatically removed. Operators must reconcile before retrying an ambiguous publication; retained diagnostic data needs disk-capacity supervision.
- Toolchain preflight occurs before expensive source acquisition. boto3 minimum is the locally verified 1.43.97, which supports PutObject IfMatch and IfNoneMatch.
- Monitoring uses the existing read-only health RPC, last successful publication and latest complete source date. Recent attempts cannot hide stale publication. Five-day source lag and a 30-hour schedule grace remain explicit. Daily report and snapshot include this separate health state.

## Validation and release boundary

Focused publisher, browser assets, monitoring and daily-report tests cover immutable conflicts, concurrent root writes, rollback/readback failure, unknown cutover spool preservation and freshness thresholds. Final result: 52 passed.

Live read-only inspection found the v3 release still at 2026-08-21 and current S3 lifecycle rules without expiration. Existing release payload is approximately 0.99 GB; each retained revision adds storage. No source fetch, publisher rerun, schedule change, production root mutation or deployment was performed.

Deploy the frontend's manifest-directed safe mirror first, then this collector. Keep the existing 08:30 Asia/Taipei schedule. After normal execution, require succeeded ledger, advancing complete source date, matching S3/HTTP root, seven UTC days, valid bytes/SHA and Range 206, and matching frontend freshness. Repeat acceptance on the following scheduled day and verify the previous S3 release remains. A merged commit or healthy container alone does not prove recovery.

## Follow-up 2026-09-26: spool retention restored

Without the promised disk-capacity supervision, failed spools accumulated to 13 GB on the main-site `/data` volume (10 failed runs of 1–2 GB each, plus orphaned `running` spools left by container restarts). Automatic pruning is restored at task start with a narrower contract: only `failed` spools (by `failed_at`) and orphaned `running` spools (by `started_at`) older than `GFW_HOURLY_FAILED_SPOOL_RETENTION_DAYS` (default 7) are removed, and only when every file matches the known spool layout. `cutover_*` spools awaiting reconciliation are never pruned. The 13.1 GB backlog was removed manually the same day after re-checking every spool status.
