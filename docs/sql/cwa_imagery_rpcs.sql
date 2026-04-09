-- ============================================================
-- CWA Imagery 前端存取 RPC
-- 目的：讓前端 supabase-js（anon role）可以讀 realtime.cwa_imagery_frames
-- realtime schema 不公開給 PostgREST，所以透過 public schema 的 RPC 包裝
-- ============================================================

-- 1) 列 frame metadata（不含 bytes，快速）
CREATE OR REPLACE FUNCTION public.get_cwa_imagery_list(
    p_dataset_ids text[],
    p_since timestamptz DEFAULT (now() - interval '24 hours')
)
RETURNS TABLE (
    dataset_id text,
    observed_at timestamptz,
    mime_type text,
    lon_min double precision,
    lon_max double precision,
    lat_min double precision,
    lat_max double precision,
    image_size integer
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = realtime, public
AS $$
    SELECT dataset_id, observed_at, mime_type,
           lon_min, lon_max, lat_min, lat_max, image_size
    FROM realtime.cwa_imagery_frames
    WHERE dataset_id = ANY(p_dataset_ids)
      AND observed_at >= p_since
    ORDER BY dataset_id, observed_at;
$$;

-- 2) 讀單張 frame bytes（base64）
CREATE OR REPLACE FUNCTION public.get_cwa_imagery_frame(
    p_dataset_id text,
    p_observed_at timestamptz
)
RETURNS text
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = realtime, public
AS $$
    SELECT encode(image_bytes, 'base64')
    FROM realtime.cwa_imagery_frames
    WHERE dataset_id = p_dataset_id
      AND observed_at = p_observed_at;
$$;

-- 3) 批次讀 metadata + bytes（取代 list + N 次 fetch_frame）
-- 原本前端要對每張 frame 各呼叫一次 get_cwa_imagery_frame，想擴大時間窗時
-- 幾百個並發 HTTP fetch 會撐爆瀏覽器網路層（TypeError: Failed to fetch）。
-- 改成一次 RPC 回傳全部 base64，payload 雖大（~57MB/48h）但只有 1 個 HTTP。
CREATE OR REPLACE FUNCTION public.get_cwa_imagery_frames_batch(
    p_dataset_ids text[],
    p_since timestamptz DEFAULT (now() - interval '48 hours')
)
RETURNS TABLE (
    dataset_id text,
    observed_at timestamptz,
    mime_type text,
    lon_min double precision,
    lon_max double precision,
    lat_min double precision,
    lat_max double precision,
    image_b64 text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = realtime, public
SET statement_timeout TO '60s'
AS $$
    SELECT dataset_id, observed_at, mime_type,
           lon_min, lon_max, lat_min, lat_max,
           encode(image_bytes, 'base64')
    FROM realtime.cwa_imagery_frames
    WHERE dataset_id = ANY(p_dataset_ids)
      AND observed_at >= p_since
    ORDER BY dataset_id, observed_at;
$$;

-- 4) 授權 anon role 呼叫
GRANT EXECUTE ON FUNCTION public.get_cwa_imagery_list(text[], timestamptz) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_cwa_imagery_frame(text, timestamptz) TO anon, authenticated;
GRANT EXECUTE ON FUNCTION public.get_cwa_imagery_frames_batch(text[], timestamptz) TO anon, authenticated;
