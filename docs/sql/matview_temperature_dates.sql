-- ============================================================
-- realtime.temperature_dates_cache — 預聚合 temperature_grids 的日期清單
--
-- 動機：原 get_temperature_dates 直接對 realtime.temperature_grids（~61 萬/月）
-- 做 GROUP BY to_char + COUNT(DISTINCT)，掃全部 partition + 跨 partition Sort。
-- 現況 ~1.9s（anon role 3s timeout 的 2/3），partition 累積後會越撐越慢。
--
-- 改成：小 cache table + pg_cron 每 15 分鐘 refresh（跑在 DB worker，不受 pooler 2min 限制）
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f matview_temperature_dates.sql
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Cache table
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime.temperature_dates_cache;

CREATE TABLE realtime.temperature_dates_cache (
    date         text PRIMARY KEY,
    frames       bigint NOT NULL,
    cells        bigint NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now()
);

COMMENT ON TABLE realtime.temperature_dates_cache IS
    'temperature_grids 的日期清單快取。由 refresh_temperature_dates() 維護，pg_cron 每 15 分鐘刷新。';

-- ------------------------------------------------------------
-- 2) Refresh function
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_temperature_dates()
RETURNS integer
LANGUAGE plpgsql
SET statement_timeout TO '0'
AS $function$
DECLARE
    inserted_count integer;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('refresh_temperature_dates'));

    DELETE FROM realtime.temperature_dates_cache;

    INSERT INTO realtime.temperature_dates_cache (date, frames, cells)
    SELECT
        to_char(observed_at AT TIME ZONE 'Asia/Taipei', 'YYYY-MM-DD') AS date,
        COUNT(DISTINCT observed_at) AS frames,
        COUNT(*) AS cells
    FROM realtime.temperature_grids
    GROUP BY 1;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$function$;

COMMENT ON FUNCTION public.refresh_temperature_dates() IS
    '重算 temperature_dates_cache。每 15 分鐘由 pg_cron 觸發。';

-- ------------------------------------------------------------
-- 3) RPC 改讀 cache table
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_temperature_dates()
RETURNS TABLE(date text, frames bigint, cells bigint)
LANGUAGE sql
STABLE
SET statement_timeout TO '30s'
AS $function$
    SELECT date, frames, cells
    FROM realtime.temperature_dates_cache
    ORDER BY date
$function$;

GRANT EXECUTE ON FUNCTION public.get_temperature_dates() TO anon, authenticated;
