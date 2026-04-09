-- ============================================================
-- reference.temperature_grid_cells — CWA 溫度靜態格點
--
-- 動機：原 get_temperature_grid_info 對 realtime.temperature_grids
-- 做 WHERE observed_at 範圍 + DISTINCT grid_lat, grid_lng + ORDER BY，
-- 實測 ~1s（anon role 3s timeout 的 1/3），隨 partition 累積會越慢。
--
-- CWA 0.03° 格點幾乎是靜態的（除非 CWA 換網格規格），不需要每次從大表掃。
-- 搬到 reference schema 靜態表，第一次 snapshot 就固定。
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f reference_temperature_grid.sql
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Static reference table
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS reference;

DROP TABLE IF EXISTS reference.temperature_grid_cells;
CREATE TABLE reference.temperature_grid_cells (
    grid_lat real NOT NULL,
    grid_lng real NOT NULL,
    PRIMARY KEY (grid_lat, grid_lng)
);

-- 從最新的 observed_at snapshot 一份（避免掃多個 partition）
INSERT INTO reference.temperature_grid_cells (grid_lat, grid_lng)
SELECT grid_lat, grid_lng
FROM realtime.temperature_grids
WHERE observed_at = (
    SELECT max(observed_at)
    FROM realtime.temperature_grids
    WHERE observed_at >= now() - interval '6 hours'
);

COMMENT ON TABLE reference.temperature_grid_cells IS
    'CWA 0.03° 溫度網格靜態格點清單。get_temperature_grid_info 直接讀這張避免掃大表。';

-- ------------------------------------------------------------
-- 2) RPC 改讀 reference 表
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_temperature_grid_info(target_date date DEFAULT CURRENT_DATE)
RETURNS TABLE(grid_lat real, grid_lng real)
LANGUAGE sql
STABLE
SET statement_timeout TO '10s'
AS $function$
    SELECT grid_lat, grid_lng
    FROM reference.temperature_grid_cells
    ORDER BY grid_lat, grid_lng
$function$;

GRANT EXECUTE ON FUNCTION public.get_temperature_grid_info(date) TO anon, authenticated;
