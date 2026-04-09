-- ============================================================
-- realtime.youbike_h3_daily — 預聚合每日 YouBike H3 快照
--
-- 動機：原 get_youbike_h3_snapshots 每次 call 都對 realtime.youbike_snapshots
-- (~36 萬筆/日) JOIN station_h3_mapping + 雙層 GROUP BY + jsonb_agg，
-- 實測 6.4 秒（anon role 3s timeout 必爆，且吃 30s function timeout 的 1/5）。
--
-- 改成 per-day table + refresh function（同 ship/freeway pattern），
-- 但多一個 resolution 維度：一次 refresh 同時跑 res=7 和 res=8。
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f matview_youbike_h3.sql
--   -- backfill 7 天
--   SELECT public.refresh_youbike_h3_daily(d::date)
--     FROM generate_series(current_date - 6, current_date, '1 day') AS d;
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Table
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime.youbike_h3_daily;

CREATE TABLE realtime.youbike_h3_daily (
    day          date NOT NULL,
    resolution   smallint NOT NULL,
    time_key     text NOT NULL,
    cells        jsonb NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, resolution, time_key)
);

CREATE INDEX youbike_h3_daily_day_res_idx
    ON realtime.youbike_h3_daily (day, resolution);

COMMENT ON TABLE realtime.youbike_h3_daily IS
    '每日 YouBike H3 聚合快照（7 天，含 res 7/8）。refresh_youbike_h3_daily(date) 同時跑兩個 resolution。pg_cron 每 10 分鐘 refresh today+yesterday。';

-- ------------------------------------------------------------
-- 2) Refresh function（一次跑一天的 res=7 和 res=8）
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_youbike_h3_daily(target_day date)
RETURNS integer
LANGUAGE plpgsql
SET statement_timeout TO '0'
AS $function$
DECLARE
    inserted_count integer;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('refresh_youbike_h3_daily:' || target_day::text));

    DELETE FROM realtime.youbike_h3_daily WHERE day = target_day;

    -- 一次跑 res=7 和 res=8（JOIN 會自動處理，因為 station_h3_mapping 有兩個 resolution）
    WITH quarter_agg AS (
        SELECT
            m.resolution,
            m.h3_index,
            to_char(
                date_trunc('hour', y.collected_at AT TIME ZONE 'Asia/Taipei')
                + INTERVAL '15 min' * FLOOR(EXTRACT(MINUTE FROM y.collected_at AT TIME ZONE 'Asia/Taipei') / 15),
                'YYYY-MM-DD"T"HH24:MI'
            ) AS tkey,
            SUM(y.available_rent)::float / NULLIF(SUM(y.total), 0) AS fullness,
            AVG(y.total) AS avg_total
        FROM realtime.youbike_snapshots y
        JOIN reference.station_h3_mapping m
            ON m.station_uid = y.station_uid
        WHERE y.collected_at >= (target_day::text || ' 00:00:00+08')::timestamptz
          AND y.collected_at <  ((target_day + 1)::text || ' 00:00:00+08')::timestamptz
        GROUP BY m.resolution, m.h3_index, tkey
    )
    INSERT INTO realtime.youbike_h3_daily (day, resolution, time_key, cells)
    SELECT
        target_day,
        resolution,
        tkey,
        jsonb_agg(
            jsonb_build_object(
                'h', h3_index,
                'fr', ROUND(fullness::numeric, 4),
                'sc', ROUND(avg_total::numeric, 1)
            )
        )
    FROM quarter_agg
    WHERE fullness IS NOT NULL
    GROUP BY resolution, tkey;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$function$;

COMMENT ON FUNCTION public.refresh_youbike_h3_daily(date) IS
    '重算 youbike_h3_daily 中指定一天（同時 res=7 和 res=8）。回傳寫入的 row 數。';

-- ------------------------------------------------------------
-- 3) 清理
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.cleanup_youbike_h3_daily(keep_days integer DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    DELETE FROM realtime.youbike_h3_daily WHERE day < (current_date - keep_days);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 4) RPC 改寫：簽名 / 回傳完全不變
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_youbike_h3_snapshots(target_date date, h3_resolution smallint DEFAULT 7)
RETURNS TABLE(time_key text, cells jsonb)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $function$
    SELECT time_key, cells
    FROM realtime.youbike_h3_daily
    WHERE day = target_date AND resolution = h3_resolution
    ORDER BY time_key
$function$;

GRANT EXECUTE ON FUNCTION public.get_youbike_h3_snapshots(date, smallint) TO anon, authenticated;
