-- ============================================================
-- pg_cron 降載 + 錯開排程
--
-- 背景：Supabase Micro instance (87 Mbps IO baseline / 2 vCPU / 1GB)
-- 套完 pre-aggregate pattern 後，8 個 refresh cron 全部 */10 同一秒觸發，
-- 瞬間 IO + CPU 爆表，Supavisor pool 耗盡，前端 RPC timeout。
--
-- 策略：
--   1. 降頻：ship/flight 10→15min，其他 10→20min，temperature_frames 30→60min
--   2. 錯開：每個 job 選不同分鐘數，避免同一秒同時啟動
--   3. 保留 refresh today + yesterday 的行為（跨日延遲資料仍會被吸收）
--
-- 使用：
--   psql "$SUPABASE_DB_URL" -f cron_throttle.sql
--
-- 事後驗證：
--   SELECT jobname, schedule FROM cron.job ORDER BY jobname;
--   SELECT jobname, status, start_time, end_time - start_time AS duration
--   FROM cron.job_run_details ORDER BY start_time DESC LIMIT 20;
-- ============================================================

-- 先停掉舊排程（名稱要對上原本 schedule 時用的 jobname）
SELECT cron.unschedule('refresh-ship-trails')       WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh-ship-trails');
SELECT cron.unschedule('refresh-flight-trails')     WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh-flight-trails');
SELECT cron.unschedule('refresh-freeway-congestion')WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh-freeway-congestion');
SELECT cron.unschedule('refresh-youbike-h3')        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh-youbike-h3');
SELECT cron.unschedule('refresh-disaster-alerts')   WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh-disaster-alerts');
SELECT cron.unschedule('refresh-temperature-dates') WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh-temperature-dates');
SELECT cron.unschedule('refresh-temperature-frames')WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh-temperature-frames');

-- ⚠️ 移除 5 個廢棄的 MV refresh cron（命名用底線，容易漏看）
-- 這些是早期用 MATERIALIZED VIEW 時的遺留，已被 *_days_summary 表取代
-- */30 REFRESH MATERIALIZED VIEW CONCURRENTLY 會全掃大表，是 IO 爆表的主因之一
SELECT cron.unschedule('refresh_mv_ship_dates')           WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh_mv_ship_dates');
SELECT cron.unschedule('refresh_mv_flight_dates')         WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh_mv_flight_dates');
SELECT cron.unschedule('refresh_mv_youbike_h3_dates')     WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh_mv_youbike_h3_dates');
SELECT cron.unschedule('refresh_mv_freeway_dates')        WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh_mv_freeway_dates');
SELECT cron.unschedule('refresh_mv_disaster_alert_dates') WHERE EXISTS (SELECT 1 FROM cron.job WHERE jobname='refresh_mv_disaster_alert_dates');

-- ------------------------------------------------------------
-- 新排程：錯開分鐘，降低頻率
-- ------------------------------------------------------------

-- Ship trails：15 分鐘一次，分 00/15/30/45
SELECT cron.schedule('refresh-ship-trails', '0,15,30,45 * * * *', $$
    SELECT public.refresh_ship_trails_daily(current_date);
    SELECT public.refresh_ship_trails_daily(current_date - 1);
$$);

-- Flight trails：15 分鐘一次，錯開到 03/18/33/48
SELECT cron.schedule('refresh-flight-trails', '3,18,33,48 * * * *', $$
    SELECT public.refresh_flight_trails_daily(current_date);
    SELECT public.refresh_flight_trails_daily(current_date - 1);
$$);

-- Freeway congestion：20 分鐘一次，06/26/46
SELECT cron.schedule('refresh-freeway-congestion', '6,26,46 * * * *', $$
    SELECT public.refresh_freeway_congestion_daily(current_date);
    SELECT public.refresh_freeway_congestion_daily(current_date - 1);
$$);

-- YouBike H3：20 分鐘一次，09/29/49
SELECT cron.schedule('refresh-youbike-h3', '9,29,49 * * * *', $$
    SELECT public.refresh_youbike_h3_daily(current_date);
    SELECT public.refresh_youbike_h3_daily(current_date - 1);
$$);

-- Disaster alerts：20 分鐘一次，12/32/52
SELECT cron.schedule('refresh-disaster-alerts', '12,32,52 * * * *', $$
    SELECT public.refresh_disaster_alerts_daily(current_date);
    SELECT public.refresh_disaster_alerts_daily(current_date - 1);
$$);

-- Temperature dates cache：20 分鐘一次（CWA 本來 1hr 更新，不急）
-- 注意：函式名是 refresh_temperature_dates()，不是 _cache 後綴
SELECT cron.schedule('refresh-temperature-dates', '*/20 * * * *', $$
    SELECT public.refresh_temperature_dates();
$$);

-- Temperature frames：60 分鐘一次（CWA 每小時才更新一次，不需要每 30 分）
-- 排在每小時的 15 分，避開其他 job 的密集時段
SELECT cron.schedule('refresh-temperature-frames', '15 * * * *', $$
    SELECT public.refresh_temperature_frames_daily(current_date);
    SELECT public.refresh_temperature_frames_daily(current_date - 1);
$$);

-- ------------------------------------------------------------
-- 驗證
-- ------------------------------------------------------------
SELECT jobname, schedule, active FROM cron.job
WHERE jobname LIKE 'refresh-%' OR jobname LIKE 'cleanup-%'
ORDER BY jobname;
