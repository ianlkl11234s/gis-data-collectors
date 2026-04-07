-- ============================================================
-- CWA 衛星雲圖 / 雷達回波 PNG 影像幀
-- collector: cwa_satellite (10 分鐘一次)
-- ------------------------------------------------------------
-- 每筆 row = 一張 PNG（bytea），對應一個資料集 + 觀測時間
-- 預期容量：2 datasets × 6 frames/hr × 24h × ~150KB ≈ 42 MB / 天
-- 視需求設定保留期（建議用 partition by RANGE(observed_at) 或 cron 清理）
--
-- 使用方式：
--   psql $SUPABASE_DB_URL -f cwa_satellite_imagery.sql
-- ============================================================

CREATE SCHEMA IF NOT EXISTS realtime;

CREATE TABLE IF NOT EXISTS realtime.cwa_imagery_frames (
    dataset_id     text        NOT NULL,
    observed_at    timestamptz NOT NULL,
    image_bytes    bytea       NOT NULL,
    mime_type      text        NOT NULL DEFAULT 'image/png',
    -- bounding box (EPSG:4326，等距圓柱投影直接 image overlay)
    lon_min        double precision,
    lon_max        double precision,
    lat_min        double precision,
    lat_max        double precision,
    -- 像素尺寸
    width          integer,
    height         integer,
    image_size     integer,
    product_url    text,
    resource_desc  text,
    collected_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, observed_at)
);

-- 查詢索引：依 dataset 取最近 N 張
CREATE INDEX IF NOT EXISTS idx_cwa_imagery_dataset_observed
    ON realtime.cwa_imagery_frames (dataset_id, observed_at DESC);

COMMENT ON TABLE realtime.cwa_imagery_frames IS
    'CWA Open Data 衛星雲圖 / 雷達回波 PNG 影像幀。bytea 直存，前端轉 base64 → blob → texture。';

-- ============================================================
-- 可選：保留 N 天的清理函式（cron 或 pg_cron 呼叫）
-- ============================================================
-- DELETE FROM realtime.cwa_imagery_frames
-- WHERE observed_at < now() - interval '7 days';
