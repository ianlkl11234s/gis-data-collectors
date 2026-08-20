#!/usr/bin/env python3
"""
load_maritime_zones.py — 灌入海域界線幾何到 spatial.maritime_zones（VW-9 P1 資料層）

搭配 gis-platform/migrations/353_maritime_zones.sql。設計說明見
mini-taiwan-pulse/docs/proposal/vessel-zone-watch.md §2/§3/§4.1。

只灌三類線 —— baseline / territorial_sea_12nm / contiguous_zone_24nm（共 12 features）。
basepoint（26 個點）不灌：那是顯示用，前端 PMTiles 已有，且它不是面無法產生 area_geom。

來源 GeoJSON 四個 region 命名在不同 layer 間不完全一致（例如 baseline 用「臺灣」、
其他層用「臺灣本島」；territorial_sea_12nm 用「中沙群島(黃岩島)」、其他層用「黃岩島」），
本檔用 REGION_ALIASES 正規化成 4 個 canonical 中文 region 名，再用 ZONE_KEY_MAP
組出穩定英文 zone_key（寫死對照表，不用中文或雜湊）。

area_geom 生成規則（在 SQL 端用 PostGIS 算，不在 Python 端用 shapely 算）：
  - LineString 且閉合 → ST_MakePolygon → ST_Multi
  - MultiLineString → 每一段各自 ST_MakePolygon 再 ST_Collect 成 MultiPolygon
    （釣魚台 baseline 106 段、12nm 2 段皆屬此類）
  ⚠️ 若任何一段不閉合，ST_MakePolygon 會直接報錯（PostGIS 硬性要求首尾點座標相同）——
     腳本不會自作主張補點封閉，錯誤會讓整個 upsert 失敗並印出是哪個 zone_key。
     載入前已用獨立腳本驗證來源 GeoJSON 12 個目標 features 的每一段都精確閉合
     （首尾座標逐位元相同，非僅距離趨近 0）。

Idempotent：ON CONFLICT (zone_key) DO UPDATE，可重跑。

用法：
    python3 scripts/load_maritime_zones.py                  # 灌入（讀預設路徑 GeoJSON）
    python3 scripts/load_maritime_zones.py --dry-run         # 只解析不寫 DB
    python3 scripts/load_maritime_zones.py --geojson PATH    # 指定其他來源檔

需環境變數：SUPABASE_DB_URL（呼叫時注入，不寫死在檔案裡）
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2

DEFAULT_GEOJSON = (
    "/Users/migu/Desktop/資料庫/gen_ai_try/ichef_工作用/GIS/taipei-gis-analytics/"
    "data/processed/environment/maritime_boundary/maritime_boundaries.geojson"
)

TARGET_LAYERS = {"baseline", "territorial_sea_12nm", "contiguous_zone_24nm"}

SOURCE_NOTE_TMPL = (
    "內政部「中華民國第一批領海基線、領海及鄰接區外界線（98年修正）」－{layer_zh}；"
    "授權 OGDL-Taiwan-1.0；上游 pipeline: "
    "taipei-gis-analytics/pipelines/environment/maritime_boundary/01_process_maritime_boundary.py"
)

# 來源 GeoJSON 的 region 命名在不同 layer 間不一致，正規化成 4 個 canonical 中文名
REGION_ALIASES = {
    "臺灣": "臺灣本島",
    "臺灣本島": "臺灣本島",
    "東沙群島": "東沙群島",
    "釣魚台列嶼": "釣魚台列嶼",
    "黃岩島": "黃岩島",
    "中沙群島(黃岩島)": "黃岩島",
}

# canonical region 中文名 → 英文 slug（寫死，不用中文或雜湊當 zone_key）
REGION_SLUG = {
    "臺灣本島": "twmain",
    "東沙群島": "dongsha",
    "釣魚台列嶼": "diaoyutai",
    "黃岩島": "huangyan",
}

# layer → zone_key 用的 slug
LAYER_SLUG = {
    "baseline": "baseline",
    "territorial_sea_12nm": "12nm",
    "contiguous_zone_24nm": "24nm",
}

UPSERT_SQL = """
INSERT INTO spatial.maritime_zones (zone_key, layer, region, line_geom, area_geom, source_note)
VALUES (
  %(zone_key)s,
  %(layer)s,
  %(region)s,
  ST_SetSRID(ST_GeomFromText(%(wkt)s), 4326),
  ST_Multi(
    CASE
      WHEN GeometryType(ST_SetSRID(ST_GeomFromText(%(wkt)s), 4326)) = 'LINESTRING'
        THEN ST_MakePolygon(ST_SetSRID(ST_GeomFromText(%(wkt)s), 4326))
      ELSE (
        SELECT ST_Collect(ST_MakePolygon(d.geom))
        FROM ST_Dump(ST_SetSRID(ST_GeomFromText(%(wkt)s), 4326)) AS d
      )
    END
  ),
  %(source_note)s
)
ON CONFLICT (zone_key) DO UPDATE SET
  layer       = EXCLUDED.layer,
  region      = EXCLUDED.region,
  line_geom   = EXCLUDED.line_geom,
  area_geom   = EXCLUDED.area_geom,
  source_note = EXCLUDED.source_note
"""


def load_features(geojson_path: Path) -> list[dict]:
    """讀來源 GeoJSON，篩出三類目標線，回傳待灌列（含 WKT）。"""
    with geojson_path.open(encoding="utf-8") as f:
        data = json.load(f)

    # 延遲 import：只有真的要組 WKT 時才需要 shapely
    from shapely.geometry import shape as shp_from_gj

    rows = []
    for feat in data["features"]:
        props = feat["properties"]
        layer = props.get("layer")
        if layer not in TARGET_LAYERS:
            continue

        raw_region = props.get("region")
        region = REGION_ALIASES.get(raw_region)
        if region is None:
            raise ValueError(
                f"未知 region 值 '{raw_region}'（layer={layer}），"
                f"REGION_ALIASES 對照表需要補這一項才能繼續，不可猜測跳過。"
            )

        region_slug = REGION_SLUG[region]
        layer_slug = LAYER_SLUG[layer]
        zone_key = f"{region_slug}_{layer_slug}"

        geom = shp_from_gj(feat["geometry"])
        wkt = geom.wkt

        rows.append({
            "zone_key": zone_key,
            "layer": layer,
            "region": region,
            "wkt": wkt,
            "source_note": SOURCE_NOTE_TMPL.format(layer_zh=props.get("layer_zh") or layer),
            "_geom_type": geom.geom_type,
            "_num_parts": len(geom.geoms) if geom.geom_type == "MultiLineString" else 1,
        })
    return rows


def upsert(conn, rows: list[dict]) -> dict:
    stats = {"upserted": 0}
    with conn.cursor() as cur:
        for r in rows:
            params = {k: v for k, v in r.items() if not k.startswith("_")}
            try:
                cur.execute(UPSERT_SQL, params)
            except Exception:
                conn.rollback()
                print(f"[load_maritime_zones] FAILED on zone_key={r['zone_key']} "
                      f"(layer={r['layer']}, region={r['region']}) — 未 commit，見上方錯誤訊息",
                      file=sys.stderr)
                raise
            stats["upserted"] += 1
    conn.commit()
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--geojson", default=DEFAULT_GEOJSON, help="來源 GeoJSON 路徑")
    ap.add_argument("--dry-run", action="store_true", help="只解析不寫 DB")
    args = ap.parse_args()

    geojson_path = Path(args.geojson)
    if not geojson_path.exists():
        print(f"[load_maritime_zones] 找不到來源檔：{geojson_path}", file=sys.stderr)
        return 1

    rows = load_features(geojson_path)
    print(f"[load_maritime_zones] 解析出 {len(rows)} 筆待灌（預期 12：3 layer × 4 region）")
    for r in rows:
        print(f"  {r['zone_key']:<20} layer={r['layer']:<22} region={r['region']:<8} "
              f"geom_type={r['_geom_type']:<16} parts={r['_num_parts']}")

    if len(rows) != 12:
        print(f"[load_maritime_zones] ⚠️ 預期 12 筆，實際 {len(rows)} 筆 —— "
              f"來源 GeoJSON 可能有變動，請確認後再繼續", file=sys.stderr)

    if args.dry_run:
        print("[load_maritime_zones] --dry-run：跳過 DB 寫入")
        return 0

    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        print("[load_maritime_zones] 需要環境變數 SUPABASE_DB_URL", file=sys.stderr)
        return 1

    conn = psycopg2.connect(db_url)
    try:
        stats = upsert(conn, rows)
        print(f"[load_maritime_zones] upserted={stats['upserted']}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
