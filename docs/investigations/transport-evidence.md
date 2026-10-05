# BL-25 transport: read-only DB evidence

Date: 2026-10-05 (Asia/Taipei).  Project: `utcmcikhvxnohbxchbrs`.

## Scope and method

- All DB calls were read-only `SELECT` or `EXPLAIN`; no refresh function, DDL,
  migration, deployment, or production write was called.
- The full SQL text and returned rows/plan are preserved in
  [`evidence/transport-db-readonly-20261005.md`](evidence/transport-db-readonly-20261005.md).
- Cost checks: the relevant daily tables have usable date indexes.  The flight
  comparison used the last 10 summary dates and `flight_trails_daily_pkey`;
  the waste comparison used `(day, city)` indexes; H3 uses `(year, resolution)`.

## Findings

### P1 — YouBike public-date materialized view is stale

`public.mv_youbike_h3_dates` ends at `2026-04-09`, while the live pre-aggregate
has rows for `2026-10-05` (150 payloads; latest recorded refresh `04:24Z`).
The active live refresh job 66 succeeded at `04:24Z` and `04:54Z`, but a bounded
`cron.job` search found no current command that references
`mv_youbike_h3_dates`.

The original migration defines a 30-minute MV refresh, so this is a deployed
cron/configuration drift, not evidence that the live aggregate failed.  Restore
and verify the MV refresh job through the normal migration path, then verify
the date RPC readback.  Do not infer a browser result from this DB evidence.

Relevant source: `gis-platform/migrations/021_youbike_h3_rpc.sql:67-89`.

### P1 — flight date summary outlives its raw rows

For the last ten summary dates, 2026-09-27 through 2026-10-05 have matching
summary and raw flight counts.  On 2026-09-26,
`flight_trails_days_summary` still reports `52,536` records and `5,351` flights,
while an indexed count of `flight_trails_daily` returns zero rows/zero flights.

The live cleanup function deletes only `live.flight_trails_daily`; it does not
delete or invalidate `live.flight_trails_days_summary`.  The refresh function
only upserts the requested day.  Make retention semantics consistent: delete or
invalidate corresponding summary dates during cleanup, or make date/summary
readers exclude dates with no retained raw rows.

Relevant source: `gis-platform/migrations/312_move_realtime_to_live.sql:709-723,
4352-4378`.

### P1 — waste map-matched coverage is empty

On 2026-10-05, raw trails are present for exact Chinese city values `新北市` (52),
`臺南市` (87), and `高雄市` (105).  Each has zero matched segments; a direct
latest-row probe of `live.waste_trails_matched_daily` returns no row at all.

The collector implementation exists and is registered, but it is disabled in
both the collector toggle and cross-layer map.  Its default city list is only
`高雄市`, so merely enabling it would still leave `新北市` and `臺南市` outside its
default target.  Enabling/expanding it needs the integration owner's decision
about OSRM capacity and external-service operation; until then the UI must not
represent road-matched coverage as available.

Relevant source: `data-collectors/config.py:328,643-648`,
`data-collectors/config/cross_layer_map.yaml:612-619`, and
`data-collectors/collectors/waste_match.py:1-12,154-183,311-326`.

### Confirmed H3 contract and coverage boundary

The live `public.get_h3_demographics_yearly(integer, integer)` definition has no
`LIMIT 20000`.  It filters `spatial.h3_demographics_yearly` by `year` and
`resolution`.  Most recent rows are year 113/resolution 7: 8,084 cells.
Year 113/resolution 8 returns no grouped row.  Preserve this as no coverage,
not a zero-valued demographic result.

Relevant source: `gis-platform/migrations/020_h3_demographics_yearly.sql:13-31,
40-74`.

## Recent cron observation

The 20 most recent runs across the relevant jobs were `succeeded`.  Recent
examples: job 66 `refresh-youbike-h3` at `04:54Z`, job 64
`refresh-flight-trails` at `04:48Z`, and job 51 `refresh-waste-trails` at
`04:37Z`.  This proves those cron invocations returned successfully, not
frontend/browser freshness or map visibility.

## Unknowns

- No browser/frontend readback was performed.
- No OSRM match attempt/run history was queried beyond the empty matched output;
  the evidence establishes absence of output, not a specific operational cause.
- No refresh or remediation was executed.
