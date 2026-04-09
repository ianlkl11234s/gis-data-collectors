-- ============================================================
-- Supabase 資源診斷查詢集
--
-- 使用時機：
--   - Project 顯示 unhealthy / IO budget 爆表
--   - 前端 RPC timeout / pool 耗盡
--   - cron refresh 堆疊懷疑
--
-- 使用：
--   psql "$SUPABASE_DB_URL" -f diagnose_resource.sql
--   或 Dashboard SQL Editor 分段跑
-- ============================================================

\echo '========== 1) 連線現況（by application） =========='
SELECT
    COALESCE(application_name, '(unknown)') AS app,
    state,
    count(*) AS n
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY app, state
ORDER BY n DESC
LIMIT 30;

\echo '========== 2) 最久未結束 query（前 20） =========='
SELECT
    pid,
    now() - query_start AS duration,
    state,
    usename,
    application_name,
    LEFT(query, 150) AS query_preview
FROM pg_stat_activity
WHERE datname = current_database()
  AND state != 'idle'
  AND query_start IS NOT NULL
ORDER BY duration DESC NULLS LAST
LIMIT 20;

\echo '========== 3) 卡住的 refresh cron 呼叫 =========='
SELECT
    pid,
    now() - query_start AS duration,
    state,
    LEFT(query, 200) AS query_preview
FROM pg_stat_activity
WHERE datname = current_database()
  AND (query ILIKE '%refresh_%_daily%' OR query ILIKE '%refresh_%_cache%')
  AND state != 'idle'
ORDER BY duration DESC;

\echo '========== 4) 最近 30 筆 cron 執行紀錄 =========='
SELECT
    j.jobname,
    d.status,
    d.start_time AT TIME ZONE 'Asia/Taipei' AS start_taipei,
    COALESCE(d.end_time - d.start_time, now() - d.start_time) AS duration,
    LEFT(d.return_message, 80) AS msg
FROM cron.job_run_details d
JOIN cron.job j USING (jobid)
ORDER BY d.start_time DESC
LIMIT 30;

\echo '========== 5) 跑最久的 cron job（最近 24h） =========='
SELECT
    j.jobname,
    count(*) AS runs,
    avg(d.end_time - d.start_time) AS avg_duration,
    max(d.end_time - d.start_time) AS max_duration,
    sum(CASE WHEN d.status = 'failed' THEN 1 ELSE 0 END) AS failed
FROM cron.job_run_details d
JOIN cron.job j USING (jobid)
WHERE d.start_time > now() - interval '24 hours'
  AND d.end_time IS NOT NULL
GROUP BY j.jobname
ORDER BY avg_duration DESC NULLS LAST;

\echo '========== 6) pre-aggregate tables 行數與最新 refresh 時間 =========='
SELECT
    schemaname || '.' || relname AS table_name,
    n_live_tup AS rows,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS size
FROM pg_stat_user_tables
WHERE relname LIKE '%_daily' OR relname LIKE '%_days_summary' OR relname LIKE '%_cache'
ORDER BY n_live_tup DESC;

\echo '========== 7) 原始分區表大小（可能的 IO bottleneck） =========='
SELECT
    schemaname || '.' || relname AS table_name,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || relname)) AS size,
    n_live_tup AS rows
FROM pg_stat_user_tables
WHERE schemaname = 'realtime'
  AND (relname LIKE 'ship_positions%' OR relname LIKE 'flight_positions%' OR relname LIKE 'temperature_grids%')
ORDER BY pg_total_relation_size(schemaname || '.' || relname) DESC
LIMIT 20;

\echo '========== 8) 目前 cron 排程清單 =========='
SELECT jobname, schedule, active
FROM cron.job
ORDER BY jobname;

\echo '========== 9) long-running / hung autovacuum? =========='
SELECT pid, now() - xact_start AS xact_age, state, LEFT(query, 120)
FROM pg_stat_activity
WHERE query ILIKE '%autovacuum%'
  AND state != 'idle'
ORDER BY xact_age DESC;

\echo ''
\echo '診斷完成。若要 kill 卡住的 refresh：'
\echo '    SELECT pg_cancel_backend(<pid>);     -- 溫和取消'
\echo '    SELECT pg_terminate_backend(<pid>);  -- 強制殺'
