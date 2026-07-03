-- ============================================================
-- CWA Imagery 上 R2 CDN（AR-11 read-path-cdn）
--
-- ⚠️ 已於 2026-07-03 直接 apply 至 gis-platform DB。
-- 本檔為「留檔」，待之後同步回 gis-platform repo 補上正式 migration 編號。
--
-- 內容：
--   1) realtime.cwa_imagery_frames 加 image_key 欄（R2 object key，nullable，
--      新寫入必填、歷史列由 scripts/backfill_imagery_r2.py 回填）
--   2) 新 RPC public.get_cwa_imagery_manifest — 語意同 get_cwa_imagery_frames_batch
--      （含 p_step_minutes 抽稀），差別在只回 key、不回 bytes，且 image_key IS NOT NULL
--   3) GRANT 給 anon / authenticated / service_role
--
-- 契約 SSOT：taipei-gis-analytics/docs/handoff/read-path-cdn-imagery.md
-- 既有 base64 RPC（get_cwa_imagery_frames_batch）保留不動，穩定一週後走 AR-11e 下架。
-- ============================================================

ALTER TABLE realtime.cwa_imagery_frames ADD COLUMN IF NOT EXISTS image_key text;

CREATE OR REPLACE FUNCTION public.get_cwa_imagery_manifest(
    p_dataset_ids text[],
    p_since timestamptz DEFAULT (now() - interval '48 hours'),
    p_until timestamptz DEFAULT now(),
    p_step_minutes integer DEFAULT NULL)
RETURNS TABLE(dataset_id text, observed_at timestamptz, mime_type text,
              lon_min double precision, lon_max double precision,
              lat_min double precision, lat_max double precision,
              image_key text, image_size integer)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path TO 'realtime', 'public'
SET statement_timeout TO '30s'
AS $fn$
    SELECT dataset_id, observed_at, mime_type,
           lon_min, lon_max, lat_min, lat_max,
           image_key, image_size
    FROM realtime.cwa_imagery_frames
    WHERE dataset_id = ANY(p_dataset_ids)
      AND observed_at >= p_since
      AND observed_at < p_until
      AND image_key IS NOT NULL
      AND (p_step_minutes IS NULL
           OR (extract(epoch FROM observed_at)::bigint / 60) % p_step_minutes = 0)
    ORDER BY dataset_id, observed_at;
$fn$;

GRANT EXECUTE ON FUNCTION public.get_cwa_imagery_manifest TO anon, authenticated, service_role;
