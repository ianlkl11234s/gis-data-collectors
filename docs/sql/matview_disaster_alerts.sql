-- ============================================================
-- realtime.disaster_alerts_daily — 預聚合每日災害示警（含幾何解析結果）
--
-- 動機：原 get_disaster_alerts_day 每次 call 都要：
--   1. scan disaster_alerts (cross-day 過濾)
--   2. tokenize area_desc 切 township 名稱
--   3. JOIN spatial.township_boundaries / boundaries 做 ST_Union
--   4. ST_SimplifyPreserveTopology 壓 geom
-- 實測 13.2s（anon role 3s timeout 必爆，且吃掉 30s function timeout 的 44%）。
-- 颱風季 alerts 暴增會更慢。
--
-- 改成：per-day table 預先解析完幾何，RPC 薄 SELECT。
-- 一個 alert 跨多天時會在多天 row 各存一份（space trade-off for speed）。
--
-- 使用方式：
--   psql "$SUPABASE_DB_URL" -f matview_disaster_alerts.sql
-- ============================================================

SET statement_timeout = 0;

-- ------------------------------------------------------------
-- 1) Table
-- ------------------------------------------------------------
DROP TABLE IF EXISTS realtime.disaster_alerts_daily;

CREATE TABLE realtime.disaster_alerts_daily (
    day            date NOT NULL,
    identifier     text NOT NULL,
    event          text,
    event_term     text,
    category       text,
    severity       text,
    urgency        text,
    certainty      text,
    msg_type       text,
    headline       text,
    description    text,
    instruction    text,
    area_desc      text,
    sender_name    text,
    author         text,
    sent_ts        bigint,
    effective_ts   bigint,
    onset_ts       bigint,
    expires_ts     bigint,
    geom           text,
    refreshed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (day, identifier)
);

CREATE INDEX disaster_alerts_daily_day_idx
    ON realtime.disaster_alerts_daily (day);

COMMENT ON TABLE realtime.disaster_alerts_daily IS
    '每日災害示警預聚合（含已解析 ST_Union + ST_SimplifyPreserveTopology 幾何）。pg_cron 每 10 分鐘 refresh today+yesterday。';

-- ------------------------------------------------------------
-- 2) Refresh function
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.refresh_disaster_alerts_daily(target_day date)
RETURNS integer
LANGUAGE plpgsql
SET statement_timeout TO '0'
AS $function$
DECLARE
    inserted_count integer;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('refresh_disaster_alerts_daily:' || target_day::text));

    DELETE FROM realtime.disaster_alerts_daily WHERE day = target_day;

    WITH bounds AS (
        SELECT
            (target_day::text || ' 00:00:00+08')::timestamptz       AS day_start,
            ((target_day + 1)::text || ' 00:00:00+08')::timestamptz AS day_end
    ),
    base AS (
        SELECT a.*,
               translate(COALESCE(a.area_desc, ''), '台', '臺') AS area_desc_norm
        FROM realtime.disaster_alerts a, bounds b
        WHERE COALESCE(a.effective, a.sent, a.onset) < b.day_end
          AND COALESCE(a.expires, a.effective, a.sent) >= b.day_start
          AND a.msg_type IS DISTINCT FROM 'Cancel'
    ),
    tokens AS (
        SELECT
            base.identifier,
            btrim(translate(tok, '台', '臺')) AS tok
        FROM base,
             LATERAL regexp_split_to_table(base.area_desc_norm, E'\\s*[/／、,，;；\n]\\s*') AS tok
        WHERE btrim(tok) <> ''
    ),
    township_geoms AS (
        SELECT t.identifier, ST_Union(b.geom) AS geom
        FROM tokens t
        JOIN spatial.township_boundaries b ON b.name = t.tok
        GROUP BY t.identifier
    ),
    county_geoms AS (
        SELECT t.identifier, ST_Union(b.geom) AS geom
        FROM tokens t
        JOIN spatial.boundaries b
          ON b.level = 'county' AND b.name = t.tok
        GROUP BY t.identifier
    ),
    resolved AS (
        SELECT
            base.identifier,
            COALESCE(base.geom, tg.geom, cg.geom) AS final_geom
        FROM base
        LEFT JOIN township_geoms tg USING (identifier)
        LEFT JOIN county_geoms   cg USING (identifier)
    )
    INSERT INTO realtime.disaster_alerts_daily (
        day, identifier, event, event_term, category, severity, urgency, certainty,
        msg_type, headline, description, instruction, area_desc, sender_name, author,
        sent_ts, effective_ts, onset_ts, expires_ts, geom
    )
    SELECT
        target_day,
        b.identifier, b.event, b.event_term, b.category, b.severity, b.urgency, b.certainty,
        b.msg_type, b.headline, b.description, b.instruction, b.area_desc, b.sender_name, b.author,
        EXTRACT(EPOCH FROM b.sent)::bigint,
        EXTRACT(EPOCH FROM b.effective)::bigint,
        EXTRACT(EPOCH FROM b.onset)::bigint,
        EXTRACT(EPOCH FROM b.expires)::bigint,
        ST_AsGeoJSON(ST_SimplifyPreserveTopology(r.final_geom, 0.001))
    FROM base b
    LEFT JOIN resolved r USING (identifier);

    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 3) Cleanup
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.cleanup_disaster_alerts_daily(keep_days integer DEFAULT 7)
RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    deleted_count integer;
BEGIN
    DELETE FROM realtime.disaster_alerts_daily WHERE day < (current_date - keep_days);
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$function$;

-- ------------------------------------------------------------
-- 4) RPC 改寫：簽名 / 回傳完全不變
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.get_disaster_alerts_day(target_date date)
RETURNS TABLE(
    identifier text, event text, event_term text, category text, severity text,
    urgency text, certainty text, msg_type text, headline text, description text,
    instruction text, area_desc text, sender_name text, author text,
    sent_ts bigint, effective_ts bigint, onset_ts bigint, expires_ts bigint, geom text
)
LANGUAGE sql
STABLE
SET statement_timeout TO '60s'
AS $function$
    SELECT
        identifier, event, event_term, category, severity, urgency, certainty,
        msg_type, headline, description, instruction, area_desc, sender_name, author,
        sent_ts, effective_ts, onset_ts, expires_ts, geom
    FROM realtime.disaster_alerts_daily
    WHERE day = target_date
    ORDER BY COALESCE(effective_ts, sent_ts) ASC
$function$;

GRANT EXECUTE ON FUNCTION public.get_disaster_alerts_day(date) TO anon, authenticated;
