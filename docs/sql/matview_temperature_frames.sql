-- ============================================================
-- realtime.temperature_frames_daily — 預聚合每日溫度 frames
--
-- 動機：原 get_temperature_frames 對 realtime.temperature_grids
-- 做 GROUP BY observed_at + string_agg(... ORDER BY grid_lat, grid_lng)。
-- 現況 ~550ms（尚 OK），但 per-frame sort 依賴 planner 選對 plan，
-- 一旦 stats stale 或 partition 量增加，可能惡化。
-- 套 pre-aggregate pattern 一勞永逸。
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f matview_temperature_frames.sql
--   SELECT public.refresh_temperature_frames_daily(d::date)
--     FROM generate_series(current_date - 6, current_date, '1 day') AS d;
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Table
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime.temperature_frames_daily;

CREATE TABLE realtime.temperature_frames_daily (
    day          date NOT NULL,
    observed_at  timestamptz NOT NULL,
    cell_count   integer NOT NULL,
    temps        text NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, observed_at)
);

CREATE INDEX temperature_frames_daily_day_idx
    ON realtime.temperature_frames_daily (day);

COMMENT ON TABLE realtime.temperature_frames_daily IS
    '每日 CWA 溫度網格 frames 預聚合（string_agg 已完成）。pg_cron 每 30 分鐘 refresh today+yesterday（CWA 每小時更新一次）。';

-- ------------------------------------------------------------
-- 2) Refresh function
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_temperature_frames_daily(target_day date)
RETURNS integer
LANGUAGE plpgsql
SET statement_timeout TO '0'
AS $function$
DECLARE
    inserted_count integer;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('refresh_temperature_frames_daily:' || target_day::text));

    DELETE FROM realtime.temperature_frames_daily WHERE day = target_day;

    INSERT INTO realtime.temperature_frames_daily (day, observed_at, cell_count, temps)
    SELECT
        target_day,
        observed_at,
        COUNT(*)::int,
        string_agg(
            round(temperature::numeric, 1)::text,
            ',' ORDER BY grid_lat, grid_lng
        )
    FROM realtime.temperature_grids
    WHERE observed_at >= (target_day::text || ' 00:00:00+08')::timestamptz
      AND observed_at <  ((target_day + 1)::text || ' 00:00:00+08')::timestamptz
    GROUP BY observed_at;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 3) Cleanup
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.cleanup_temperature_frames_daily(keep_days integer DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    DELETE FROM realtime.temperature_frames_daily WHERE day < (current_date - keep_days);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 4) RPC 改寫
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_temperature_frames(target_date date DEFAULT CURRENT_DATE)
RETURNS TABLE(observed_at timestamptz, cell_count integer, temps text)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $function$
    SELECT observed_at, cell_count, temps
    FROM realtime.temperature_frames_daily
    WHERE day = target_date
    ORDER BY observed_at
$function$;

GRANT EXECUTE ON FUNCTION public.get_temperature_frames(date) TO anon, authenticated;
