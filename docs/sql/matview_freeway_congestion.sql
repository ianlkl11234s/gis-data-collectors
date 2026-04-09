-- ============================================================
-- realtime.freeway_congestion_daily — 預聚合每日國道路段壅塞 timeline
--
-- 動機：原 get_freeway_congestion_day 每次被呼叫就對 realtime.freeway_sections
-- 當日全範圍掃描 + per-section string_agg(... ORDER BY collected_at) + JOIN
-- freeway_sections_current，已經被逼到 60s statement_timeout。
--
-- 照 ship/flight trails 的 pattern（參見 matview_ship_trails.sql）：
--   - 普通 table + per-day refresh function + advisory xact lock 防並發
--   - pg_cron 每 10 分鐘 refresh today + yesterday
--   - RPC 改讀 table，保留 60s timeout 做 payload 傳輸護欄
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f matview_freeway_congestion.sql
--   -- 第一次 backfill 7 天：
--   SELECT public.refresh_freeway_congestion_daily(d::date)
--     FROM generate_series(current_date - 6, current_date, '1 day') AS d;
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Table
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime.freeway_congestion_daily;

CREATE TABLE realtime.freeway_congestion_daily (
    day              date NOT NULL,
    section_id       text NOT NULL,
    section_name     text,
    road_name        text,
    direction_label  text,
    geom             text,            -- ST_AsGeoJSON 結果
    timeline         text NOT NULL,   -- "epoch,level,speed;..."
    point_count      integer NOT NULL,
    refreshed_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, section_id)
);

CREATE INDEX freeway_congestion_daily_day_idx
    ON realtime.freeway_congestion_daily (day);

COMMENT ON TABLE realtime.freeway_congestion_daily IS
    '每日國道路段壅塞 timeline 預聚合（7 天）。由 refresh_freeway_congestion_daily(date) 維護，pg_cron 每 10 分鐘 refresh today+yesterday。';

-- ------------------------------------------------------------
-- 2) Refresh function
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_freeway_congestion_daily(target_day date)
RETURNS integer
LANGUAGE plpgsql
SET statement_timeout TO '0'
AS $function$
DECLARE
    inserted_count integer;
BEGIN
    -- advisory xact lock 防止並發（cron + 手動同時 call）
    PERFORM pg_advisory_xact_lock(hashtext('refresh_freeway_congestion_daily:' || target_day::text));

    DELETE FROM realtime.freeway_congestion_daily WHERE day = target_day;

    WITH agg AS (
        SELECT
            s.section_id,
            string_agg(
                EXTRACT(EPOCH FROM s.collected_at)::bigint::text
                || ',' || COALESCE(s.congestion_level::text, '0')
                || ',' || COALESCE(round(s.travel_speed)::text, ''),
                ';' ORDER BY s.collected_at
            ) AS timeline,
            COUNT(*)::int AS point_count
        FROM realtime.freeway_sections s
        WHERE s.collected_at >= (target_day::text || ' 00:00:00+08')::timestamptz
          AND s.collected_at <  ((target_day + 1)::text || ' 00:00:00+08')::timestamptz
        GROUP BY s.section_id
    )
    INSERT INTO realtime.freeway_congestion_daily
        (day, section_id, section_name, road_name, direction_label, geom, timeline, point_count)
    SELECT
        target_day,
        a.section_id,
        c.section_name,
        c.road_name,
        c.direction_label,
        ST_AsGeoJSON(c.geom),
        a.timeline,
        a.point_count
    FROM agg a
    JOIN realtime.freeway_sections_current c USING (section_id)
    WHERE c.geom IS NOT NULL;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$function$;

COMMENT ON FUNCTION public.refresh_freeway_congestion_daily(date) IS
    '重算 freeway_congestion_daily 中指定一天的資料。回傳寫入的 row 數。';

-- ------------------------------------------------------------
-- 3) 清理函式：刪除超過 N 天的舊資料
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.cleanup_freeway_congestion_daily(keep_days integer DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    DELETE FROM realtime.freeway_congestion_daily
    WHERE day < (current_date - keep_days);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 4) RPC 改寫：簽名 / 回傳完全不變
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_freeway_congestion_day(target_date date)
RETURNS TABLE(section_id text, section_name text, road_name text, direction_label text, geom text, timeline text)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $function$
    SELECT section_id, section_name, road_name, direction_label, geom, timeline
    FROM realtime.freeway_congestion_daily
    WHERE day = target_date
$function$;

GRANT EXECUTE ON FUNCTION public.get_freeway_congestion_day(date) TO anon, authenticated;

-- 注意：get_freeway_dates 不動（mv_freeway_dates 680 sections × 144/day ≈ 10 萬筆，
-- REFRESH 仍在 timeout 內正常運作，跟 mv_ship_dates 80 萬不同）
