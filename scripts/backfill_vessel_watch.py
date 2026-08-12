#!/usr/bin/env python3
"""
從 S3 raw 回補特殊船舶（海警／海巡／科研船／軍艦…）到 live.vessel_watch_*。

搭配 gis-platform/migrations/339_vessel_watch.sql。設計說明見
mini-taiwan-pulse/docs/proposal/vessel-watch-layer.md。

為什麼讀 S3 raw 而不是 DB 母表：
  S3 原始 JSON 有 imo / call_sign / length / width / draught / nav_status /
  destination，這些**當初沒進 DB**（supabase_writer._transform_ship_ais 只寫 9 欄），
  而它們正是船隻名冊最需要的身分證據。

S3 涵蓋（2026-08-12 查證）：
  ship_ais/2026/<MM>/<DD>/*.json   2026-02-03 ~ 02-28（逐檔）
  ship_ais/archives/<date>.tar.gz  2026-02-28 ~ 迄今（每日打包，延遲約 6 天）
  → 最近 6 天不在 S3，改用 SELECT live.sweep_vessel_watch('21 days') 從母表撈。

兩個關鍵處理：
  1. 時鐘對齊：DB 的 collected_at = collector 抓取時間，所以這裡取 _fetch_time／
     fetch_time（台灣時間無時區後綴，補 +08:00），**不是** record_time（AIS 訊息時間）。
     沿用 backfill_ship_flight.py 既有慣例。
  2. 降採樣：S3 raw 是每 2 分鐘一個快照，而 go-forward sweep 實測是每船 15 分鐘一筆。
     照原解析度回補 190 天會灌進 400 萬筆以上，且大多是錨泊船連日重複座標。
     這裡降採樣成每船每 15 分鐘一筆，跟現行密度一致。

⚠️ 收錄條件的唯一真相是 SQL 的 live.is_watch_candidate()。
   本檔的 _COARSE_PATTERN 只是效能用的粗篩（刻意比 SQL 更寬），
   最終仍由 INSERT ... WHERE live.is_watch_candidate(...) 把關。
   **不要在這裡收緊規則** —— 改規則請改 migration 裡的 SQL 函數。

用法：
    # 全量回補（S3 有多少補多少）
    python3 scripts/backfill_vessel_watch.py --since 2026-02-03 --until 2026-08-05

    # 單日
    python3 scripts/backfill_vessel_watch.py --date 2026-07-15

    # 試跑（不寫 DB，只印統計）
    python3 scripts/backfill_vessel_watch.py --date 2026-07-15 --dry-run

    # 只更新名冊不寫軌跡（每週掃描用，另見 scan_vessel_registry.py）
    python3 scripts/backfill_vessel_watch.py --date 2026-08-05 --registry-only

需環境變數：SUPABASE_DB_URL、S3_BUCKET / S3_ACCESS_KEY / S3_SECRET_KEY / S3_REGION
"""
import sys
import os
import re
import json
import tarfile
import logging
from io import BytesIO
from datetime import datetime, date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from storage.s3 import S3Storage

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_URL = (
    os.getenv('SUPABASE_DB_URL')
    or os.getenv('DATABASE_POOL_URL')
    or os.getenv('DATABASE_URL')
)

# S3 版面
ARCHIVE_PREFIX = 'ship_ais/archives'
DAILY_PREFIX = 'ship_ais/2026'
ARCHIVE_START = date(2026, 2, 28)   # 此日起改用 tar.gz 打包

# 降採樣：每船每 N 秒最多留一筆（對齊 go-forward sweep 的實測密度）
DOWNSAMPLE_SEC = 15 * 60

# 粗篩 pattern —— 效能用，刻意比 SQL 的 is_watch_candidate 更寬。
# 真正的把關在 DB 端，這裡只是避免把每天 400 萬筆全部送進 DB。
_COARSE_PATTERN = re.compile(
    r'COASTGUARD|HAIJING|HAIXUN|YUZHENG|HAIJIAN|XIANGYANGHONG|HAIYANG|TANSUO'
    r'|SHIYAN|OCEANRESEARCH|FISHERYRESEARCH|RESEARCHVESSEL|SHENHAIYOU'
    r'|WARSHIP|NAVY|^CG\d|^PP\d|^CL\d'
)
_COARSE_TYPES = {'執法船', '軍艦'}


def _norm(name: str) -> str:
    """對齊 SQL 的 live.normalize_vessel_name()：去非英數 + 大寫。"""
    return re.sub(r'[^A-Za-z0-9]', '', name or '').upper()


def is_coarse_candidate(rec: dict) -> bool:
    """粗篩：寧可誤收（後面 SQL 會再過），不可漏收。"""
    if rec.get('vessel_type_name') in _COARSE_TYPES:
        return True
    return bool(_COARSE_PATTERN.search(_norm(rec.get('ship_name'))))


def _fetch_time_of(data: dict, member_name: str, date_str: str) -> str:
    """取抓取時間並補台灣時區（沿用 backfill_ship_flight.py 的慣例）。"""
    ft = data.get('fetch_time') or data.get('_fetch_time')
    if not ft:
        # 檔名形如 ship_ais_2307.json → 23:07
        t = member_name.replace('ship_ais_', '').replace('.json', '')
        ft = f"{date_str}T{t[:2]}:{t[2:4]}:00+08:00" if len(t) >= 4 else f"{date_str}T00:00:00+08:00"
    elif '+' not in ft and 'Z' not in ft:
        ft = ft + '+08:00'
    return ft


def _iter_archive(s3: S3Storage, day: date, ds: str, quiet: bool = False):
    """讀 archives/<date>.tar.gz。"""
    key = f'{ARCHIVE_PREFIX}/{ds}.tar.gz'
    try:
        blob = s3.get_file(key)
    except Exception as e:
        if not quiet:
            logger.warning(f'  {ds}: 讀不到 {key}（{type(e).__name__}）—— 可能尚未打包（archive 延遲約 6 天）')
        return
    with tarfile.open(fileobj=BytesIO(blob), mode='r:gz') as tar:
        for m in sorted(tar.getmembers(), key=lambda x: x.name):
            if not m.name.endswith('.json'):
                continue
            f = tar.extractfile(m)
            if not f:
                continue
            try:
                data = json.loads(f.read())
            except json.JSONDecodeError:
                logger.warning(f'  跳過無法解析: {m.name}')
                continue
            yield _fetch_time_of(data, m.name, ds), data.get('data', [])


def _iter_daily(s3: S3Storage, day: date, ds: str):
    """讀 ship_ais/2026/<MM>/<DD>/*.json 逐檔版面。"""
    prefix = f'{DAILY_PREFIX}/{day:%m}/{day:%d}/'
    # ⚠️ S3Storage.list_files() 回傳的是 dict（key/size/modified），不是字串
    for f in sorted(s3.list_files(prefix, max_keys=2000), key=lambda x: x['key']):
        key = f['key']
        if not key.endswith('.json'):
            continue
        try:
            data = s3.get_json(key)
        except Exception as e:
            logger.warning(f'  跳過 {key}（{type(e).__name__}）')
            continue
        yield _fetch_time_of(data, key.split('/')[-1], ds), data.get('data', [])


def _iter_snapshots(s3: S3Storage, day: date):
    """
    yield (fetch_time_iso, [records]) —— 自動處理 tar.gz 與逐檔兩種版面。

    ⚠️ 2026-02-28 是換版面的當天，兩邊都只有部分資料
    （該日 tar.gz 僅 2.0MB，其餘日子約 30MB）→ 兩個來源都讀。
    重複讀無害：positions 以 (mmsi, 時間桶) 去重、registry 以 set 合併。
    """
    ds = day.isoformat()
    if day > ARCHIVE_START:
        yield from _iter_archive(s3, day, ds)
    elif day == ARCHIVE_START:
        yield from _iter_archive(s3, day, ds, quiet=True)
        yield from _iter_daily(s3, day, ds)
    else:
        yield from _iter_daily(s3, day, ds)


def collect_day(s3: S3Storage, day: date):
    """
    掃一天的 S3 → (positions rows, registry dict)。
    positions 已降採樣；registry 為 mmsi → 該日彙整的身分證據。
    """
    positions = {}   # (mmsi, bucket) → row tuple
    registry = {}    # mmsi → dict

    for ft_iso, records in _iter_snapshots(s3, day):
        try:
            ts = datetime.fromisoformat(ft_iso)
        except ValueError:
            continue
        bucket = int(ts.timestamp()) // DOWNSAMPLE_SEC

        for r in records:
            if not is_coarse_candidate(r):
                continue
            mmsi = str(r.get('mmsi') or '').strip()
            lat, lng = r.get('lat'), r.get('lon')
            if not mmsi:
                continue

            name = (r.get('ship_name') or '').strip()
            stype = r.get('vessel_type_name') or ''

            if lat and lng:
                positions[(mmsi, bucket)] = (
                    mmsi, ft_iso, name, stype, lat, lng,
                    r.get('sog'), r.get('heading'),
                    r.get('nav_status_name'), (r.get('destination') or '').strip() or None,
                    's3_backfill', f'SRID=4326;POINT({lng} {lat})',
                )

            e = registry.setdefault(mmsi, {
                'imo': None, 'call_sign': None, 'names': set(),
                'length': None, 'width': None, 'draught': None,
                'first': ft_iso, 'last': ft_iso, 'ship_type': stype,
            })
            if name:
                e['names'].add(name)
            for src, dst in (('imo', 'imo'), ('call_sign', 'call_sign'),
                             ('length', 'length'), ('width', 'width'), ('draught', 'draught')):
                v = r.get(src)
                if v not in (None, '', 0) and e[dst] in (None, ''):
                    e[dst] = v
            if ft_iso < e['first']:
                e['first'] = ft_iso
            if ft_iso > e['last']:
                e['last'] = ft_iso
            if stype:
                e['ship_type'] = stype

    return list(positions.values()), registry


# ── DB 寫入 ──────────────────────────────────────────────────
# 收錄條件由 DB 端 live.is_watch_candidate() 最終把關（本檔的粗篩只是效能優化）。
POSITIONS_SQL = """
INSERT INTO live.vessel_watch_positions
    (mmsi, collected_at, ship_name, ship_type, lat, lng, speed, heading,
     nav_status, destination, source, geom)
SELECT t.mmsi, t.collected_at::timestamptz, t.ship_name, t.ship_type,
       t.lat::double precision, t.lng::double precision,
       t.speed::real, t.heading::real, t.nav_status, t.destination, t.source,
       t.geom::geometry
FROM (VALUES %s) AS t(mmsi, collected_at, ship_name, ship_type, lat, lng,
                      speed, heading, nav_status, destination, source, geom)
WHERE live.is_watch_candidate(t.mmsi, t.ship_name, t.ship_type)
ON CONFLICT (mmsi, collected_at) DO NOTHING
"""

# ⚠️ 人工欄位（confirmed_class / note / is_excluded / confirmed_at）不在 UPDATE 清單裡。
#    這是整個「持續更新清單」需求的地基：掃描永不覆寫人工校正。
REGISTRY_SQL = """
INSERT INTO live.vessel_watch_registry
    (mmsi, imo, call_sign, names_seen, length_m, width_m, draught_m,
     rule_class, rule_flag, matched_by, first_seen, last_seen, last_scan_at)
SELECT t.mmsi,
       t.imo, t.call_sign, t.names_seen,
       t.length_m::numeric, t.width_m::numeric, t.draught_m::numeric,
       c.vessel_class, c.flag, c.matched_by,
       t.first_seen::timestamptz, t.last_seen::timestamptz, now()
FROM (VALUES %s) AS t(mmsi, imo, call_sign, names_seen, length_m, width_m, draught_m,
                      primary_name, ship_type, first_seen, last_seen)
CROSS JOIN LATERAL live.classify_vessel(t.mmsi, t.primary_name, t.ship_type) AS c
WHERE live.is_watch_candidate(t.mmsi, t.primary_name, t.ship_type)
ON CONFLICT (mmsi) DO UPDATE SET
    imo        = COALESCE(live.vessel_watch_registry.imo, EXCLUDED.imo),
    call_sign  = COALESCE(live.vessel_watch_registry.call_sign, EXCLUDED.call_sign),
    length_m   = COALESCE(live.vessel_watch_registry.length_m, EXCLUDED.length_m),
    width_m    = COALESCE(live.vessel_watch_registry.width_m, EXCLUDED.width_m),
    draught_m  = COALESCE(live.vessel_watch_registry.draught_m, EXCLUDED.draught_m),
    -- ⚠️ COALESCE 不可省：array_agg 對空集合回傳 NULL 而非空陣列，
    --    船名一直是空字串的船（實測有）在第二次 upsert 就會撞 NOT NULL。
    names_seen = COALESCE((SELECT array_agg(DISTINCT x)
                           FROM unnest(live.vessel_watch_registry.names_seen || EXCLUDED.names_seen) x
                           WHERE x IS NOT NULL AND x <> ''), '{}'),
    rule_class = EXCLUDED.rule_class,
    rule_flag  = EXCLUDED.rule_flag,
    matched_by = EXCLUDED.matched_by,
    first_seen = LEAST(COALESCE(live.vessel_watch_registry.first_seen, EXCLUDED.first_seen),
                       EXCLUDED.first_seen),
    last_seen  = GREATEST(COALESCE(live.vessel_watch_registry.last_seen, EXCLUDED.last_seen),
                          EXCLUDED.last_seen),
    last_scan_at = now()
"""


def write_day(conn, positions, registry, registry_only=False, dry_run=False):
    if dry_run:
        return len(positions), len(registry)

    reg_rows = [
        (m, e['imo'], e['call_sign'], sorted(e['names']),
         e['length'], e['width'], e['draught'],
         (sorted(e['names'])[0] if e['names'] else ''), e['ship_type'],
         e['first'], e['last'])
        for m, e in registry.items()
    ]

    with conn.cursor() as cur:
        if reg_rows:
            execute_values(cur, REGISTRY_SQL, reg_rows, page_size=500)
        if positions and not registry_only:
            for i in range(0, len(positions), 2000):
                execute_values(cur, POSITIONS_SQL, positions[i:i + 2000], page_size=2000)
    conn.commit()
    return len(positions), len(reg_rows)


def daterange(a: date, b: date):
    for i in range((b - a).days + 1):
        yield a + timedelta(days=i)


def main():
    import argparse
    p = argparse.ArgumentParser(description='從 S3 raw 回補特殊船舶到 live.vessel_watch_*')
    p.add_argument('--date', help='單日 YYYY-MM-DD')
    p.add_argument('--since', help='起日 YYYY-MM-DD')
    p.add_argument('--until', help='迄日 YYYY-MM-DD')
    p.add_argument('--registry-only', action='store_true', help='只更新名冊，不寫軌跡')
    p.add_argument('--dry-run', action='store_true', help='不寫 DB，只印統計')
    args = p.parse_args()

    if args.date:
        days = [date.fromisoformat(args.date)]
    elif args.since:
        until = date.fromisoformat(args.until) if args.until else date.today() - timedelta(days=1)
        days = list(daterange(date.fromisoformat(args.since), until))
    else:
        p.error('需要 --date 或 --since')

    if not DB_URL and not args.dry_run:
        logger.error('缺 SUPABASE_DB_URL')
        return 1

    s3 = S3Storage()
    conn = None if args.dry_run else psycopg2.connect(DB_URL)
    tot_pos = tot_reg = 0
    seen_mmsi = set()

    try:
        for d in days:
            positions, registry = collect_day(s3, d)
            if not positions and not registry:
                logger.info(f'{d}: 無資料')
                continue
            np_, nr = write_day(conn, positions, registry, args.registry_only, args.dry_run)
            tot_pos += np_
            tot_reg += nr
            seen_mmsi |= set(registry)
            logger.info(f'{d}: 軌跡 {np_:6d} 筆（降採樣後）／船 {nr:4d} 艘')
    finally:
        if conn:
            conn.close()

    mode = '［試跑，未寫入］' if args.dry_run else ''
    logger.info(f'完成{mode}：{len(days)} 天，軌跡 {tot_pos} 筆，不重複船 {len(seen_mmsi)} 艘')
    if args.dry_run:
        logger.info('註：試跑數字是「粗篩」結果，實際寫入會再經 DB 的 is_watch_candidate() 過濾')
    return 0


if __name__ == '__main__':
    sys.exit(main())
