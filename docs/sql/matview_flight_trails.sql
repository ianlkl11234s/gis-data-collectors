-- ============================================================
-- realtime.flight_trails_daily — 預聚合每日航班軌跡
--
-- 同 matview_ship_trails.sql 的架構（普通 table + per-day refresh function）
-- 套用原 RPC 過濾：altitude IS NOT NULL AND altitude >= 100
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f matview_flight_trails.sql
--   SELECT public.refresh_flight_trails_daily(d::date)
--     FROM generate_series(current_date - 6, current_date, '1 day') AS d;
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Table
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime.flight_trails_daily;
DROP MATERIALIZED VIEW IF EXISTS realtime.flight_trails_daily;

CREATE TABLE realtime.flight_trails_daily (
    day            date NOT NULL,
    flight_id      text NOT NULL,
    callsign       text NOT NULL DEFAULT '',
    aircraft_type  text NOT NULL DEFAULT '',
    origin         text NOT NULL DEFAULT '',
    destination    text NOT NULL DEFAULT '',
    trail          text NOT NULL,
    point_count    integer NOT NULL,
    refreshed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, flight_id)
);

CREATE INDEX flight_trails_daily_day_idx
    ON realtime.flight_trails_daily (day);

COMMENT ON TABLE realtime.flight_trails_daily IS
    '每日航班軌跡預聚合（最近 7 天，含 ±1h overlap）。由 refresh_flight_trails_daily(date) 維護。';

-- ------------------------------------------------------------
-- 2) Refresh function
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_flight_trails_daily(target_day date)
RETURNS integer
LANGUAGE plpgsql
SET statement_timeout TO '0'
AS $function$
DECLARE
    inserted_count integer;
BEGIN
    -- advisory xact lock 防止並發撞 unique constraint
    PERFORM pg_advisory_xact_lock(hashtext('refresh_flight_trails_daily:' || target_day::text));
    DELETE FROM realtime.flight_trails_daily WHERE day = target_day;

    INSERT INTO realtime.flight_trails_daily
        (day, flight_id, callsign, aircraft_type, origin, destination, trail, point_count)
    SELECT
        target_day AS day,
        flight_id,
        COALESCE(MAX(callsign), '')      AS callsign,
        COALESCE(MAX(aircraft_type), '') AS aircraft_type,
        COALESCE(MAX(origin), '')        AS origin,
        COALESCE(MAX(destination), '')   AS destination,
        string_agg(
            lat::text || ',' || lng::text || ',' || COALESCE(altitude, 0)::text || ',' || EXTRACT(EPOCH FROM collected_at)::bigint::text,
            ';' ORDER BY collected_at
        ) AS trail,
        COUNT(*)::int AS point_count
    FROM realtime.flight_positions
    WHERE collected_at >= (target_day::text || ' 00:00:00+08')::timestamptz - INTERVAL '1 hour'
      AND collected_at <  ((target_day + 1)::text || ' 00:00:00+08')::timestamptz + INTERVAL '1 hour'
      AND altitude IS NOT NULL
      AND altitude >= 100
    GROUP BY flight_id
    HAVING COUNT(*) >= 2;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$function$;

COMMENT ON FUNCTION public.refresh_flight_trails_daily(date) IS
    '重算 flight_trails_daily 中指定一天的資料。回傳寫入的 row 數。';

-- ------------------------------------------------------------
-- 3) 清理函式
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.cleanup_flight_trails_daily(keep_days integer DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    DELETE FROM realtime.flight_trails_daily
    WHERE day < (current_date - keep_days);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 4) RPC 改寫
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_flight_trails(target_date date)
RETURNS TABLE(flight_id text, callsign text, aircraft_type text, origin text, destination text, trail text)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $function$
    SELECT flight_id, callsign, aircraft_type, origin, destination, trail
    FROM realtime.flight_trails_daily
    WHERE day = target_date
$function$;

GRANT EXECUTE ON FUNCTION public.get_flight_trails(date) TO anon, authenticated;
