-- ============================================================
-- realtime.ship_trails_daily — 預聚合每日船舶軌跡
--
-- 架構：普通 table + per-day refresh function（不是 matview）
--
-- 動機：
--   原 get_ship_trails RPC 直接掃 ship_positions 80 萬筆 + string_agg ORDER BY，
--   會跑超過 60s timeout。
--   一開始嘗試過 MATERIALIZED VIEW 一次 build 7 天（560 萬筆 sort），
--   實測 36 分鐘還沒完，cron 15 分鐘 refresh 完全不可能。
--
--   改成普通 table + per-day refresh function：
--     - 每次只重算 1 天（80 萬筆，sort 量縮 7 倍）
--     - cron 每 15 分鐘只 refresh today + yesterday 兩天
--     - 第一次 backfill 手動 call 7 次 refresh
--
-- 行為與舊 RPC 100% 一致（保留 ±1h overlap）：
--   舊 RPC 撈 target_date 00:00 +08 ± 1h 的點，讓 timeline 跨日銜接不會斷掉。
--   refresh function 沿用同樣 WHERE clause，所以每天的 trail 跟舊 RPC 完全相同。
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f matview_ship_trails.sql
--   -- 第一次 backfill：
--   SELECT public.refresh_ship_trails_daily(d::date)
--     FROM generate_series(current_date - 6, current_date, '1 day') AS d;
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Table（取代舊的 matview，如果存在則先 DROP）
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime.ship_trails_daily;
DROP MATERIALIZED VIEW IF EXISTS realtime.ship_trails_daily;

CREATE TABLE realtime.ship_trails_daily (
    day          date NOT NULL,
    mmsi         text NOT NULL,
    ship_type    text,
    trail        text NOT NULL,
    point_count  integer NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, mmsi)
);

CREATE INDEX ship_trails_daily_day_idx
    ON realtime.ship_trails_daily (day);

COMMENT ON TABLE realtime.ship_trails_daily IS
    '每日船舶軌跡預聚合（最近 7 天，含 ±1h overlap）。由 refresh_ship_trails_daily(date) 維護，pg_cron 每 15 分鐘 refresh today + yesterday。';

-- ------------------------------------------------------------
-- 2) Refresh function（per-day upsert）
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_ship_trails_daily(target_day date)
RETURNS integer
LANGUAGE plpgsql
SET statement_timeout TO '0'
AS $function$
DECLARE
    inserted_count integer;
BEGIN
    -- advisory xact lock 防止並發撞 unique constraint（cron + 手動同時 call）
    PERFORM pg_advisory_xact_lock(hashtext('refresh_ship_trails_daily:' || target_day::text));
    DELETE FROM realtime.ship_trails_daily WHERE day = target_day;

    INSERT INTO realtime.ship_trails_daily (day, mmsi, ship_type, trail, point_count)
    SELECT
        target_day AS day,
        mmsi,
        MAX(ship_type) AS ship_type,
        string_agg(
            lat::text || ',' || lng::text || ',' || EXTRACT(EPOCH FROM collected_at)::bigint::text,
            ';' ORDER BY collected_at
        ) AS trail,
        COUNT(*)::int AS point_count
    FROM realtime.ship_positions
    WHERE collected_at >= (target_day::text || ' 00:00:00+08')::timestamptz - INTERVAL '1 hour'
      AND collected_at <  ((target_day + 1)::text || ' 00:00:00+08')::timestamptz + INTERVAL '1 hour'
    GROUP BY mmsi
    HAVING COUNT(*) >= 2;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;

    -- 順手更新小 summary table，讓 get_ship_dates 毫秒級
    INSERT INTO realtime.ship_trails_days_summary (day, records, ships, refreshed_at)
    SELECT target_day, COALESCE(sum(point_count), 0), COUNT(*), now()
    FROM realtime.ship_trails_daily
    WHERE day = target_day
    ON CONFLICT (day) DO UPDATE
        SET records = EXCLUDED.records,
            ships   = EXCLUDED.ships,
            refreshed_at = now();

    RETURN inserted_count;
END;
$function$;

COMMENT ON FUNCTION public.refresh_ship_trails_daily(date) IS
    '重算 ship_trails_daily 中指定一天的資料（先 DELETE 再 INSERT）。回傳寫入的 row 數。';

-- ------------------------------------------------------------
-- 3) 清理函式：刪除超過 N 天的舊資料
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.cleanup_ship_trails_daily(keep_days integer DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    DELETE FROM realtime.ship_trails_daily
    WHERE day < (current_date - keep_days);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 4) RPC 改寫：簽名 / 回傳完全不變
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_ship_trails(target_date date)
RETURNS TABLE(mmsi text, ship_type text, trail text)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $function$
    SELECT mmsi, ship_type, trail
    FROM realtime.ship_trails_daily
    WHERE day = target_date
$function$;

GRANT EXECUTE ON FUNCTION public.get_ship_trails(date) TO anon, authenticated;

-- ------------------------------------------------------------
-- 5) get_ship_dates 讀 summary table
--
-- 原本從 mv_ship_dates 讀，但 mv_ship_dates 定義是全掃 ship_positions
-- GROUP BY date，REFRESH 會撞 2 分鐘 pooler timeout → cron 持續失敗
-- → 前端看到的最新 date 永遠停在最後一次成功 refresh 的時間點。
--
-- 不能直接 GROUP BY ship_trails_daily（trail 欄位 KB 級，125k rows × 1KB = 125MB IO → 4 秒）
-- 改讀 ship_trails_days_summary，refresh function 順手 upsert 維護。
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_ship_dates()
RETURNS TABLE(date text, records bigint, ships bigint)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $function$
    SELECT day::text, records, ships
    FROM realtime.ship_trails_days_summary
    ORDER BY day
$function$;

GRANT EXECUTE ON FUNCTION public.get_ship_dates() TO anon, authenticated;
