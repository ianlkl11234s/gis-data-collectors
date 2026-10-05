# TD-1 rail timetable evidence

Date: 2026-10-05

## Finding

- `rail_timetable` used a 1,440-minute default despite the TDX `Today` schedule being revised during the day.
- Successful TRA conversion writes `system = 'tra_daily'`, while the conversion fallback wrote `system = 'tra'`. Because `reference.daily_schedules` is unique on `(system, schedule_date)`, this split one service day across two identities.
- `reference.daily_schedules.data` is `JSONB NOT NULL`; the table has no dedicated quality/status column. The fallback can therefore carry an explicit degraded marker inside the JSON document without a schema migration.

## Local change

- Set the default `RAIL_TIMETABLE_INTERVAL` to 240 minutes.
- Keep fallback rows on `system = 'tra_daily'`.
- Store fallback data as `{metadata: {degraded: true, degraded_reason: 'conversion_failed', ...}, raw_schedules: [...]}`. `raw_schedules` is intentionally distinct from converted `schedules`, so downstream analytics do not interpret unconverted TDX records as the mini-taipei schedule contract.
- A degraded upsert may insert a missing day or refresh an already-degraded row, but cannot overwrite a healthy row for the same `(system, schedule_date)`. A later healthy run remains able to replace a degraded row.

## Backfill recommendation (not executed)

- For `2026-04-02`, use the existing orphan `system = 'tra'` raw row as the dated source. Convert that saved payload with the OD progress cache, verify the output date and non-empty converted `schedules`, then write the historical result as `system = 'tra_daily'`. Do not call the TDX `Today` endpoint to reconstruct an old service date.
- For `2026-06-26`, use the dated S3 archive as the source, convert and validate it offline, then write the resulting historical `tra_daily` payload. Do not rerun the current-day collector for this date.
- Only review removal of the `2026-04-02` orphan after the replacement `tra_daily` row is independently verified. No production writes, deletes, or backfill were performed in this investigation.

## Downstream safety review

- `analytics.refresh_tra_delay_daily()` expands `data->'schedules'`. A degraded payload has no `schedules`, so PostgreSQL produces no schedule rows rather than throwing, but observed trains can then be silently classified as absent from the schedule.
- The schedules Portal API returns `data` directly and does not reject `metadata.degraded = true`; external clients must inspect the marker before assuming the converted schedule contract.
- The protected upsert prevents a transient conversion failure from replacing a healthy same-day row. If no healthy row exists yet, the degraded row remains visible for diagnosis and can be repaired by a later healthy run.

## Integration gap

`config/cross_layer_map.yaml` and `config/realtime_tables.yaml` still declare `expected_interval_min: 1440`. They were outside TD-1 file ownership and were not changed here; the integration owner should decide whether health monitoring should follow the new 240-minute collection cadence or intentionally keep a daily freshness threshold.

## Offline tests

Full-suite command (no live Supabase option):

```text
python3 -m pytest -q --junitxml=docs/investigations/td1-pytest-full.xml
```

Raw result: `592 collected; 553 passed, 37 failed, 2 skipped in 176.03s`. The JUnit log is `docs/investigations/td1-pytest-full.xml`. Failures were unrelated to TD-1: 10 missing `ijson`, 25 missing `opencc` or its dependent assertions, one existing rate-limiter timing assertion, and one existing concurrent-writer timing assertion that exceeded its 0.300-second threshold by 0.000152 seconds. The writer timing case passed when rerun alone (`1 passed in 5.55s`). No live database option was enabled and the pytest network/database/credential guards remained active.

Focused TD-1 and isolation command:

```text
python3 -m pytest tests/test_supabase_writer.py::test_rail_timetable_default_interval_is_four_hours tests/test_supabase_writer.py::test_rail_timetable_tra_fallback_keeps_daily_key_and_marks_degraded tests/test_supabase_writer.py::test_rail_timetable_degraded_write_cannot_replace_healthy_row tests/test_supabase_writer.py::test_rail_timetable_healthy_write_can_replace_degraded_row tests/test_test_isolation.py tests/test_tra_timetable_raw_stops.py -q
```

Raw result: `16 collected`, all 16 reached pass status (`100%`, process exit 0).
