#!/usr/bin/env python3
"""
seed_reservoir_watershed.py — 水庫集水區 polygon seed（水利署 data.gov.tw 129474）

下載 https://gic.wra.gov.tw/gis/gic/API/Google/DownLoad.aspx?fname=RESERVOIR&filetype=SHP
→ 解壓 → TWD97 TM2 轉 WGS84 → upsert reference.reservoir_watershed

用法：
    python3 scripts/seed_reservoir_watershed.py                # 下載最新版 + upsert
    python3 scripts/seed_reservoir_watershed.py --local PATH   # 用本地既有 shp（除錯用）
    python3 scripts/seed_reservoir_watershed.py --dry          # 只解析不寫 DB

依賴：requests, pyshp, pyproj, shapely, psycopg2
更新頻率：不定期（2020-08 首發、2023-05 最新版），建議每季重跑一次

2026-04-21 初版 — 驗證 80/80 polygon 有 primary_name match，79/80 對應水庫點
"""
from __future__ import annotations

import argparse
import io
import os
import re
import sys
import zipfile
from datetime import date
from pathlib import Path

import requests
import psycopg2
import shapefile  # pyshp
from pyproj import Transformer
from shapely.geometry import shape as shp_from_gj, Polygon, MultiPolygon
from shapely.ops import transform as shp_transform

SHP_URL = (
    "https://gic.wra.gov.tw/gis/gic/API/Google/DownLoad.aspx"
    "?fname=RESERVOIR&filetype=SHP"
)
PUBLISH_DATE = date(2023, 5, 5)

# TWD97 TM2 (EPSG:3826) → WGS84 (EPSG:4326)
_tr = Transformer.from_crs("EPSG:3826", "EPSG:4326", always_xy=True)


def _to_wgs(x: float, y: float, _z: float | None = None) -> tuple[float, float]:
    lng, lat = _tr.transform(x, y)
    return (lng, lat)


def normalize_name(s: str) -> str:
    """去「(XX堰)」後綴 + 去「水庫/堰/壩」結尾，用於 join reservoir_geometry."""
    s = re.sub(r"\([^)]*\)", "", s).strip()
    s = re.sub(r"(水庫|堰|壩)$", "", s).strip()
    return s


def download_shp(dest_dir: Path) -> Path:
    """下載並解壓 SHP，回傳 .shp 路徑。"""
    dest_dir.mkdir(parents=True, exist_ok=True)
    print(f"[seed] downloading {SHP_URL}")
    resp = requests.get(SHP_URL, timeout=60)
    resp.raise_for_status()
    print(f"[seed] got {len(resp.content):,} bytes")
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(dest_dir)
    shp_files = list(dest_dir.glob("*.shp"))
    if not shp_files:
        raise RuntimeError("No .shp found in zip")
    return shp_files[0]


def parse_polygons(shp_path: Path) -> list[dict]:
    """讀 SHP 並轉 WGS84，回傳 records 清單。"""
    sf = shapefile.Reader(str(shp_path), encoding="utf-8", encodingErrors="replace")
    out: list[dict] = []
    for rec, shp in zip(sf.records(), sf.shapes()):
        d = rec.as_dict()
        full_name = (d.get("Name") or "").strip()
        if not full_name:
            continue
        geom_tm2 = shp_from_gj(shp.__geo_interface__)
        geom_wgs = shp_transform(_to_wgs, geom_tm2)
        # 統一為 MultiPolygon（簡化下游處理）
        if isinstance(geom_wgs, Polygon):
            geom_wgs = MultiPolygon([geom_wgs])
        out.append({
            "primary_name": normalize_name(full_name),
            "full_name":    full_name,
            "zone":         (d.get("ZONE") or "").strip() or None,
            "class_1":      (d.get("CLASS_1") or "").strip() or None,
            "state":        (d.get("STATE") or "").strip() or None,
            "unit":         (d.get("UNIT") or "").strip() or None,
            "type":         (d.get("TYPE") or "").strip() or None,
            "area_m2":      float(d.get("area") or 0) or None,
            "note":         (d.get("NOTE") or "").strip() or None,
            "wkt":          geom_wgs.wkt,
        })
    return out


def upsert(conn, rows: list[dict]) -> dict:
    """upsert 進 reference.reservoir_watershed，同時用 compare_id 回填。"""
    stats = {"inserted": 0, "updated": 0, "compare_id_matched": 0}
    sql = """
        INSERT INTO reference.reservoir_watershed
            (primary_name, full_name, compare_id, zone, class_1, state, unit, type,
             area_m2, note, publish_date, geom)
        VALUES
            (%(primary_name)s, %(full_name)s, %(compare_id)s, %(zone)s, %(class_1)s,
             %(state)s, %(unit)s, %(type)s, %(area_m2)s, %(note)s, %(publish_date)s,
             ST_Multi(ST_SetSRID(ST_GeomFromText(%(wkt)s), 4326)))
        ON CONFLICT (full_name) DO UPDATE SET
            primary_name = EXCLUDED.primary_name,
            compare_id   = EXCLUDED.compare_id,
            zone         = EXCLUDED.zone,
            class_1      = EXCLUDED.class_1,
            state        = EXCLUDED.state,
            unit         = EXCLUDED.unit,
            type         = EXCLUDED.type,
            area_m2      = EXCLUDED.area_m2,
            note         = EXCLUDED.note,
            geom         = EXCLUDED.geom,
            updated_at   = now()
        RETURNING (xmax = 0) AS inserted
    """

    # 先建立 primary_name → compare_id 的對照（從既有 reference.reservoir_geometry）
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT ON (normalized) normalized, compare_id
            FROM (
                SELECT compare_id,
                       regexp_replace(
                         regexp_replace(res_name, '\\([^)]*\\)', '', 'g'),
                         '(水庫|堰|壩)$', ''
                       ) AS normalized
                FROM reference.reservoir_geometry
                WHERE compare_id > 0
            ) sub
            WHERE normalized <> ''
        """)
        name_to_cid = dict(cur.fetchall())

    for r in rows:
        r["compare_id"] = name_to_cid.get(r["primary_name"])
        if r["compare_id"]:
            stats["compare_id_matched"] += 1
        r["publish_date"] = PUBLISH_DATE
        with conn.cursor() as cur:
            cur.execute(sql, r)
            inserted = cur.fetchone()[0]
            if inserted:
                stats["inserted"] += 1
            else:
                stats["updated"] += 1
    conn.commit()
    return stats


def load_env_db_url() -> str:
    """從 data-collectors/.env 讀 SUPABASE_DB_URL。"""
    # __file__ = scripts/seed_reservoir_watershed.py
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if not env_path.exists():
        raise RuntimeError(f".env not found at {env_path}")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("SUPABASE_DB_URL="):
            return line.split("=", 1)[1]
    raise RuntimeError("SUPABASE_DB_URL not set in .env")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--local", help="用本地 .shp 路徑（跳過下載）")
    ap.add_argument("--dry", action="store_true", help="只解析不寫 DB")
    args = ap.parse_args()

    if args.local:
        shp_path = Path(args.local)
    else:
        import tempfile
        tmpdir = Path(tempfile.mkdtemp(prefix="reservoir_watershed_"))
        shp_path = download_shp(tmpdir)

    rows = parse_polygons(shp_path)
    print(f"[seed] parsed {len(rows)} polygons")

    if args.dry:
        for r in rows[:3]:
            print(f"  sample: {r['full_name']:<24} zone={r['zone']} "
                  f"area={r['area_m2']:.0f}m² wkt_len={len(r['wkt'])}")
        print("[seed] --dry: skip DB write")
        return 0

    db_url = os.environ.get("SUPABASE_DB_URL") or load_env_db_url()
    conn = psycopg2.connect(db_url)
    try:
        stats = upsert(conn, rows)
        print(f"[seed] inserted={stats['inserted']} updated={stats['updated']} "
              f"compare_id_matched={stats['compare_id_matched']}/{len(rows)}")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
