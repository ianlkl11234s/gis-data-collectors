# BL-25 transport DB query ledger (read-only)

All statements below were run against `utcmcikhvxnohbxchbrs` on 2026-10-05.
SQL results are recorded as returned.  No statement calls a refresh function.

## Q1 relation inventory

```sql
SELECT n.nspname AS schema, c.relname, c.relkind,
       c.reltuples::bigint AS estimated_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = ANY (ARRAY['mv_youbike_h3_dates','youbike_h3_daily',
  'waste_trails_daily','waste_trails_matched_daily',
  'flight_trails_days_summary','flight_trails_daily','h3_demographics'])
ORDER BY n.nspname, c.relname LIMIT 30;
```

```json
[
 {"schema":"live","relname":"flight_trails_daily","relkind":"r","estimated_rows":47961,"total_size":"31 MB"},
 {"schema":"live","relname":"flight_trails_days_summary","relkind":"r","estimated_rows":185,"total_size":"64 kB"},
 {"schema":"live","relname":"waste_trails_daily","relkind":"r","estimated_rows":5505,"total_size":"14 MB"},
 {"schema":"live","relname":"waste_trails_matched_daily","relkind":"r","estimated_rows":0,"total_size":"9048 kB"},
 {"schema":"live","relname":"youbike_h3_daily","relkind":"r","estimated_rows":2418,"total_size":"136 MB"},
 {"schema":"public","relname":"mv_youbike_h3_dates","relkind":"m","estimated_rows":10,"total_size":"64 kB"},
 {"schema":"spatial","relname":"h3_demographics","relkind":"r","estimated_rows":-1,"total_size":"22 MB"}
]
```

## Q2 relevant indexes

```sql
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes
WHERE tablename = ANY (ARRAY['youbike_h3_daily','waste_trails_daily',
  'waste_trails_matched_daily','flight_trails_daily','h3_demographics'])
ORDER BY tablename, indexname LIMIT 100;
```

Returned indexes include: `flight_trails_daily_day_idx (day)`,
`flight_trails_daily_pkey (day, flight_id)`,
`idx_waste_trails_daily_day_city (day, city)`,
`idx_waste_trails_matched_day_city (day, city)`,
`youbike_h3_daily_day_res_idx (day, resolution)`, and
`idx_h3_version (data_version, resolution)`.

## Q3 H3 live function definition

```sql
SELECT pg_get_functiondef(
  'public.get_h3_demographics_yearly(integer,integer)'::regprocedure
) AS definition LIMIT 1;
```

```sql
CREATE OR REPLACE FUNCTION public.get_h3_demographics_yearly(target_year integer,
 target_resolution integer DEFAULT 7)
RETURNS TABLE(h text, p integer, hh integer, m integer, f integer,
 sr real, dr real, cd real, ed real, ai real)
LANGUAGE sql STABLE SET search_path TO 'public', 'pg_temp'
AS $function$
 SELECT h3_index, population, household_count, male_count, female_count,
        sex_ratio, dependency_ratio, child_dependency, elderly_dependency, aging_index
 FROM spatial.h3_demographics_yearly
 WHERE year = target_year AND resolution = target_resolution;
$function$;
```

No `LIMIT 20000` appears in the live definition.

```sql
SELECT schemaname, tablename, indexname, indexdef
FROM pg_indexes WHERE tablename = 'h3_demographics_yearly'
ORDER BY indexname LIMIT 20;
```

Result includes `idx_h3dy_year_res ON spatial.h3_demographics_yearly (year, resolution)`.

```sql
EXPLAIN (COSTS TRUE, VERBOSE FALSE)
SELECT year, resolution FROM spatial.h3_demographics_yearly
ORDER BY year DESC, resolution LIMIT 20;
```

```text
Limit  (cost=77.78..79.17 rows=20 width=6)
  -> Incremental Sort  (cost=77.78..18837.91 rows=268961 width=6)
       Sort Key: year DESC, resolution
       Presorted Key: year
       -> Index Only Scan Backward using idx_h3dy_year_res
            (cost=0.42..8314.94 rows=268961 width=6)
```

```sql
SELECT year, resolution, count(*) AS cells
FROM spatial.h3_demographics_yearly
WHERE year = 113 AND resolution = 7
GROUP BY year, resolution LIMIT 1;
```

```json
[{"year":113,"resolution":7,"cells":8084}]
```

```sql
SELECT year, resolution, count(*) AS cells
FROM spatial.h3_demographics_yearly
WHERE year = 113 AND resolution = 8
GROUP BY year, resolution LIMIT 1;
```

```json
[]
```

An earlier bounded `WHERE year >= 2023 GROUP BY year,resolution LIMIT 20` also
returned `[]`; year values are ROC years, so this is expected from that predicate.

## Q4 YouBike MV and live daily comparison

```sql
SELECT * FROM public.mv_youbike_h3_dates ORDER BY 1 DESC LIMIT 20;
```

```json
[
 {"date":"2026-04-09","records":312181,"stations":3939},
 {"date":"2026-04-08","records":378144,"stations":3939},
 {"date":"2026-04-07","records":496314,"stations":3939},
 {"date":"2026-04-06","records":374205,"stations":3939},
 {"date":"2026-04-05","records":228462,"stations":3939},
 {"date":"2026-04-04","records":122109,"stations":3939},
 {"date":"2026-04-03","records":362324,"stations":3939},
 {"date":"2026-04-02","records":369901,"stations":3937},
 {"date":"2026-04-01","records":369679,"stations":3935},
 {"date":"2026-03-31","records":251648,"stations":3934}
]
```

```sql
EXPLAIN (COSTS TRUE, VERBOSE FALSE)
SELECT day, count(*) AS payloads, max(refreshed_at) AS refreshed_at
FROM live.youbike_h3_daily GROUP BY day ORDER BY day DESC LIMIT 10;
```

```text
Limit  (cost=77.55..77.57 rows=9 width=20)
  -> Sort (cost=77.55..77.57 rows=9 width=20)
       Sort Key: day DESC
       -> HashAggregate (cost=77.31..77.41 rows=9 width=20)
            Group Key: day
            -> Seq Scan on youbike_h3_daily (cost=0.00..59.18 rows=2418 width=12)
```

```sql
SELECT day, count(*) AS payloads, max(refreshed_at) AS refreshed_at
FROM live.youbike_h3_daily GROUP BY day ORDER BY day DESC LIMIT 10;
```

```json
[
 {"day":"2026-10-05","payloads":150,"refreshed_at":"2026-10-05 04:24:00.051949+00"},
 {"day":"2026-10-04","payloads":288,"refreshed_at":"2026-10-04 17:28:00.053265+00"},
 {"day":"2026-10-03","payloads":285,"refreshed_at":"2026-10-03 17:28:00.03152+00"},
 {"day":"2026-10-02","payloads":258,"refreshed_at":"2026-10-02 17:28:00.05022+00"},
 {"day":"2026-10-01","payloads":288,"refreshed_at":"2026-10-01 17:28:00.121345+00"},
 {"day":"2026-09-30","payloads":288,"refreshed_at":"2026-09-30 17:28:00.029839+00"},
 {"day":"2026-09-29","payloads":285,"refreshed_at":"2026-09-29 17:28:00.172057+00"},
 {"day":"2026-09-28","payloads":288,"refreshed_at":"2026-09-28 17:28:00.063547+00"},
 {"day":"2026-09-27","payloads":288,"refreshed_at":"2026-09-27 17:28:00.100418+00"}
]
```

```sql
SELECT pg_get_viewdef('public.mv_youbike_h3_dates'::regclass, true) AS definition LIMIT 1;
```

Returned definition groups `live.youbike_snapshots` by Taipei-local `collected_at` date.

```sql
SELECT jobid, jobname, schedule, command, active
FROM cron.job WHERE command ILIKE '%mv_youbike_h3_dates%'
ORDER BY jobid LIMIT 10;
```

```json
[]
```

## Q5 waste latest raw/matched comparison

```sql
EXPLAIN (COSTS TRUE, VERBOSE FALSE)
WITH latest AS (SELECT max(day) AS day FROM live.waste_trails_daily),
base AS (
 SELECT w.day, w.city, count(*) AS raw_trails
 FROM live.waste_trails_daily w JOIN latest l ON l.day = w.day
 GROUP BY w.day, w.city
)
SELECT b.day, b.city, b.raw_trails, COALESCE(m.matched_segments, 0) AS matched_segments
FROM base b
LEFT JOIN LATERAL (
 SELECT count(*) AS matched_segments FROM live.waste_trails_matched_daily mt
 WHERE mt.day = b.day AND mt.city = b.city
) m ON true ORDER BY b.city LIMIT 30;
```

```text
Nested Loop uses Index Only Scan Backward idx_waste_trails_daily_day_city to find
the latest date, then Index Only Scan on the same `(day, city)` index.  The
matched relation has an estimated 0 rows and its probe is cost 0.00..0.01.
```

```sql
-- Same CTE/query as above, without EXPLAIN.
```

```json
[
 {"day":"2026-10-05","city":"新北市","raw_trails":52,"matched_segments":0},
 {"day":"2026-10-05","city":"臺南市","raw_trails":87,"matched_segments":0},
 {"day":"2026-10-05","city":"高雄市","raw_trails":105,"matched_segments":0}
]
```

```sql
SELECT day, city, vehicle_no, trip_id, segment_seq
FROM live.waste_trails_matched_daily
ORDER BY day DESC, city LIMIT 1;
```

```json
[]
```

## Q6 flight last-ten summary/raw comparison

```sql
EXPLAIN (COSTS TRUE, VERBOSE FALSE)
WITH recent_days AS (
 SELECT day, records, flights, refreshed_at FROM live.flight_trails_days_summary
 ORDER BY day DESC LIMIT 10
)
SELECT s.day, s.records AS summary_records, s.flights AS summary_flights,
 r.raw_records, r.raw_flights, s.refreshed_at
FROM recent_days s
LEFT JOIN LATERAL (
 SELECT count(*) AS raw_records, count(DISTINCT flight_id) AS raw_flights
 FROM live.flight_trails_daily f WHERE f.day = s.day
) r ON true ORDER BY s.day DESC LIMIT 10;
```

```text
Nested Loop Left Join (cost=646.08..6460.07 rows=10)
  -> Index Scan Backward using flight_trails_days_summary_pkey (last 10 dates)
  -> Aggregate
       -> Index Only Scan using flight_trails_daily_pkey
            Index Cond: (day = flight_trails_days_summary.day)
```

```sql
-- Same CTE/query as above, without EXPLAIN.
```

```json
[
 {"day":"2026-10-05","summary_records":23639,"summary_flights":2748,"raw_records":2748,"raw_flights":2748,"refreshed_at":"2026-10-05 04:48:00.055147+00"},
 {"day":"2026-10-04","summary_records":53884,"summary_flights":5630,"raw_records":5630,"raw_flights":5630,"refreshed_at":"2026-10-04 17:20:00.046358+00"},
 {"day":"2026-10-03","summary_records":52351,"summary_flights":5469,"raw_records":5469,"raw_flights":5469,"refreshed_at":"2026-10-03 17:20:00.057483+00"},
 {"day":"2026-10-02","summary_records":54914,"summary_flights":5726,"raw_records":5726,"raw_flights":5726,"refreshed_at":"2026-10-02 17:20:00.078156+00"},
 {"day":"2026-10-01","summary_records":56221,"summary_flights":5899,"raw_records":5899,"raw_flights":5899,"refreshed_at":"2026-10-01 17:20:00.210342+00"},
 {"day":"2026-09-30","summary_records":55703,"summary_flights":5885,"raw_records":5885,"raw_flights":5885,"refreshed_at":"2026-09-30 17:20:00.096242+00"},
 {"day":"2026-09-29","summary_records":52313,"summary_flights":5590,"raw_records":5590,"raw_flights":5590,"refreshed_at":"2026-09-29 17:20:00.081882+00"},
 {"day":"2026-09-28","summary_records":53868,"summary_flights":5467,"raw_records":5467,"raw_flights":5467,"refreshed_at":"2026-09-28 17:20:00.08114+00"},
 {"day":"2026-09-27","summary_records":54962,"summary_flights":5685,"raw_records":5685,"raw_flights":5685,"refreshed_at":"2026-09-27 17:20:00.083111+00"},
 {"day":"2026-09-26","summary_records":52536,"summary_flights":5351,"raw_records":0,"raw_flights":0,"refreshed_at":"2026-09-26 17:20:00.068058+00"}
]
```

## Q7 relevant cron definitions and latest raw run output

```sql
SELECT jobid, jobname, schedule, command, active
FROM cron.job
WHERE jobname ILIKE ANY (ARRAY['%youbike%', '%flight%trail%', '%waste%trail%', '%h3%demo%'])
ORDER BY jobid LIMIT 50;
```

```text
18 cleanup-flight-trails active: SELECT public.cleanup_flight_trails_daily(7)
23 cleanup-youbike-h3 active: SELECT public.cleanup_youbike_h3_daily(7)
51 refresh-waste-trails active: current Taipei day, minutes 7,37
52 cleanup-waste-trails active: SELECT public.cleanup_waste_trails_daily(7)
53 cleanup-waste-matched-trails active: SELECT public.cleanup_waste_trails_matched_daily(7)
64 refresh-flight-trails active: current Taipei day, minutes 18,48
66 refresh-youbike-h3 active: current Taipei day, minutes 24,54
86 refresh-waste-trails-yesterday active: daily 17:08 Taipei
89 refresh-flight-trails-yesterday active: daily 17:20 Taipei
91 refresh-youbike-h3-yesterday active: daily 17:28 Taipei
```

```sql
SELECT r.jobid, j.jobname, r.status, r.start_time, r.end_time, r.return_message
FROM cron.job_run_details r JOIN cron.job j ON j.jobid = r.jobid
WHERE r.jobid = ANY (ARRAY[18,23,51,52,53,64,66,86,89,91])
ORDER BY r.start_time DESC LIMIT 20;
```

```text
66 refresh-youbike-h3 succeeded 2026-10-05 04:54:00Z..04:54:12Z 1 row
64 refresh-flight-trails succeeded 2026-10-05 04:48:00Z..04:48:00Z 1 row
51 refresh-waste-trails succeeded 2026-10-05 04:37:00Z..04:37:01Z 1 row
66 refresh-youbike-h3 succeeded 2026-10-05 04:24:00Z..04:24:12Z 1 row
53 cleanup-waste-matched-trails succeeded 2026-10-05 04:18:00Z..04:18:00Z 1 row
64 refresh-flight-trails succeeded 2026-10-05 04:18:00Z..04:18:00Z 1 row
52 cleanup-waste-trails succeeded 2026-10-05 04:12:00Z..04:12:00Z 1 row
51 refresh-waste-trails succeeded 2026-10-05 04:07:00Z..04:07:02Z 1 row
66 refresh-youbike-h3 succeeded 2026-10-05 03:54:00Z..03:54:11Z 1 row
64 refresh-flight-trails succeeded 2026-10-05 03:48:00Z..03:48:00Z 1 row
51 refresh-waste-trails succeeded 2026-10-05 03:37:00Z..03:37:01Z 1 row
66 refresh-youbike-h3 succeeded 2026-10-05 03:24:00Z..03:24:11Z 1 row
64 refresh-flight-trails succeeded 2026-10-05 03:18:00Z..03:18:00Z 1 row
51 refresh-waste-trails succeeded 2026-10-05 03:07:00Z..03:07:02Z 1 row
66 refresh-youbike-h3 succeeded 2026-10-05 02:54:00Z..02:54:10Z 1 row
64 refresh-flight-trails succeeded 2026-10-05 02:48:00Z..02:48:00Z 1 row
51 refresh-waste-trails succeeded 2026-10-05 02:37:00Z..02:37:01Z 1 row
66 refresh-youbike-h3 succeeded 2026-10-05 02:24:00Z..02:24:10Z 1 row
64 refresh-flight-trails succeeded 2026-10-05 02:18:00Z..02:18:00Z 1 row
51 refresh-waste-trails succeeded 2026-10-05 02:07:00Z..02:07:01Z 1 row
```

## Q8 live function retention/refresh definitions

`pg_get_functiondef` confirmed these relevant live bodies:

```sql
cleanup_flight_trails_daily(keep_days):
  DELETE FROM live.flight_trails_daily WHERE day < (current_date - keep_days);

refresh_flight_trails_daily(target_day):
  DELETE FROM live.flight_trails_daily WHERE day = target_day;
  INSERT INTO live.flight_trails_daily ...;
  INSERT INTO live.flight_trails_days_summary(day, records, flights, refreshed_at)
  SELECT target_day, COALESCE(sum(point_count),0), COUNT(*), now()
  FROM live.flight_trails_daily WHERE day = target_day
  ON CONFLICT (day) DO UPDATE ...;

cleanup_youbike_h3_daily(keep_days):
  DELETE FROM live.youbike_h3_daily WHERE day < (current_date - keep_days);

refresh_youbike_h3_daily(target_day):
  DELETE FROM live.youbike_h3_daily WHERE day = target_day;
  INSERT INTO live.youbike_h3_daily ...;
```

The exact live function output was obtained with a bounded four-row
`pg_proc` subquery and `jsonb_object_agg`; it contained no secret values.
