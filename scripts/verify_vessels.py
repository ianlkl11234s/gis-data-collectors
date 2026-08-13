#!/usr/bin/env python3
"""
特殊船舶「網路查證」流程的兩端：匯出待查清單 / 回寫查證結果。

搭配 gis-platform migration 343（`live.vessel_watch_unverified` view + 查證欄位）。

## 為什麼需要這個流程

2026-08-13 品質稽核：registry 686 艘裡 **154 艘（22%）只憑船方自報的 ship_type
判定**，沒有第二個證據。抽查三艘就有兩艘錯：
  JIAN TZAY FA NO.6 (416002652) 判「軍艦」→ 實為台灣漁船
  CHINACOASTGUARD14534 (546000000) 判「軍艦」→ 該 MMSI 查無此船
  ASTERIX (316030879) 判「軍艦」→ 正確（皇家加拿大海軍補給艦）

用戶表示人工審也看不出來 → 改用公開船舶資料庫（MarineTraffic / VesselFinder /
MyShipTracking，皆以 MMSI 為 key）查證。

## 三步流程

    # 1. 匯出待查清單（預設先出最危險的軍艦類）
    python3 scripts/verify_vessels.py export --limit 30 -o /tmp/todo.json

    # 2. 交給 Claude 逐艘 WebSearch，產出 verdicts JSON（格式見下）
    #    ⚠️ 這步是 LLM 在做判斷，故格式強制要求 evidence_url

    # 3. 回寫（沒有 evidence_url 的一律拒收）
    python3 scripts/verify_vessels.py apply /tmp/verdicts.json

## verdicts JSON 格式

    [
      {
        "mmsi": "416002652",
        "verified_class": null,            // 判定分類；不屬於任何特殊船種就填 null
        "verified_by": "web_search",       // web_search | not_found | inconclusive
        "evidence_url": "https://www.vesselfinder.com/vessels/details/416002652",
        "evidence_note": "VesselFinder 載明 Fishing vessel，台灣籍",
        "is_excluded": true                // 選填：確認是誤收的民船就標 true
      }
    ]

## ⚠️ 防幻覺的機械護欄

`verified_by = "web_search"` 時**必須**有 `evidence_url`，否則該筆直接拒收並計入
rejected。這是整個流程唯一擋得住 LLM 亂編的地方——不要為了「跑得順」把它拿掉。
查不到就老實填 `not_found`（那本身是有用的負面證據，通常代表假 MMSI）。

## 與人工確認的關係

`confirmed_class`（人工）優先於 `verified_class`（網路查證）優先於 `rule_class`（規則）。
本腳本**只寫 verified_* 欄位**，永遠不碰 confirmed_class——人工說了算。
"""
import sys
import os
import json
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

DB_URL = (
    os.getenv('SUPABASE_DB_URL')
    or os.getenv('DATABASE_POOL_URL')
    or os.getenv('DATABASE_URL')
)

VALID_BY = {'web_search', 'not_found', 'inconclusive'}

EXPORT_SQL = """
SELECT mmsi, imo, call_sign, names_seen, rule_class, matched_by,
       length_m, reason, name_looks_corrupt,
       to_char(last_seen AT TIME ZONE 'Asia/Taipei', 'YYYY-MM-DD') AS last_seen_date
FROM live.vessel_watch_unverified
{where}
LIMIT %s
"""

APPLY_SQL = """
UPDATE live.vessel_watch_registry r
   SET verified_class = v.verified_class,
       verified_by    = v.verified_by,
       evidence_url   = v.evidence_url,
       evidence_note  = v.evidence_note,
       is_excluded    = COALESCE(v.is_excluded, r.is_excluded),
       verified_at    = now()
  FROM (VALUES %s) AS v(mmsi, verified_class, verified_by, evidence_url, evidence_note, is_excluded)
 WHERE r.mmsi = v.mmsi
   AND r.confirmed_class IS NULL      -- 人工確認過的一律不動
"""


def cmd_export(args):
    where = ""
    params = []
    if args.rule_class:
        where = "WHERE rule_class = %s"
        params.append(args.rule_class)
    elif args.only_corrupt:
        where = "WHERE name_looks_corrupt"

    conn = psycopg2.connect(DB_URL)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(EXPORT_SQL.format(where=where), params + [args.limit])
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    for r in rows:
        if r.get('last_seen_date') is None:
            r['last_seen_date'] = ''
        r['names_seen'] = list(r.get('names_seen') or [])
        if r.get('length_m') is not None:
            r['length_m'] = float(r['length_m'])

    out = args.out or '/tmp/vessel_verify_todo.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f'匯出 {len(rows)} 艘 → {out}')
    if rows:
        from collections import Counter
        c = Counter(r['rule_class'] or '（無）' for r in rows)
        for k, v in c.most_common():
            print(f'  {k}: {v}')


def cmd_apply(args):
    with open(args.verdicts, encoding='utf-8') as f:
        verdicts = json.load(f)

    rows, rejected, stripped = [], [], []
    for v in verdicts:
        mmsi = str(v.get('mmsi') or '').strip()
        by = (v.get('verified_by') or '').strip()
        url = (v.get('evidence_url') or '').strip() or None
        cls = (v.get('verified_class') or None)

        if not mmsi or by not in VALID_BY:
            rejected.append((mmsi, f'verified_by 不合法: {by!r}'))
            continue
        # ⚠️ 唯一擋得住 LLM 幻覺的護欄：宣稱查到就必須附來源
        if by == 'web_search' and not url:
            rejected.append((mmsi, 'verified_by=web_search 但缺 evidence_url'))
            continue
        # ⚠️ inconclusive／not_found 不得挾帶分類 —— 實測 haiku 會在 inconclusive
        #    時填「從 MMSI 國碼推論」的 class（note 自己都寫「無法查到具體船舶」）。
        #    那是推論不是查證。migration 344 的 effective_class 本來就不採用它，
        #    但仍要在寫入前清掉，否則 verified_class 欄位的語意會被沒有依據的值污染。
        if by != 'web_search' and cls:
            stripped.append((mmsi, cls))
            cls = None

        rows.append((
            mmsi,
            cls,
            by,
            url,
            (v.get('evidence_note') or None),
            v.get('is_excluded'),
        ))

    if stripped:
        print(f'⚠️ 清掉 {len(stripped)} 筆「非 web_search 卻挾帶分類」（推論非查證）：')
        for m, c in stripped[:6]:
            print(f'   {m}: {c} → 清空')

    if rejected:
        print(f'⚠️ 拒收 {len(rejected)} 筆：')
        for m, why in rejected[:10]:
            print(f'   {m}: {why}')

    if not rows:
        print('沒有可寫入的判決')
        return

    if args.dry_run:
        print(f'［試跑］可寫入 {len(rows)} 筆')
        return

    conn = psycopg2.connect(DB_URL)
    with conn.cursor() as cur:
        execute_values(cur, APPLY_SQL, rows, page_size=200)
        n = cur.rowcount
    conn.commit()
    conn.close()
    print(f'✅ 寫入 {n} 筆（拒收 {len(rejected)}）')


def cmd_status(args):
    conn = psycopg2.connect(DB_URL)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COALESCE(verified_by,'（未查證）'), count(*)
            FROM live.vessel_watch_registry WHERE NOT is_excluded
            GROUP BY 1 ORDER BY 2 DESC
        """)
        print('查證進度：')
        for by, n in cur.fetchall():
            print(f'  {by:14s} {n:4d}')
        cur.execute("SELECT count(*) FROM live.vessel_watch_unverified")
        print(f'\n待查剩餘：{cur.fetchone()[0]}')
    conn.close()


def main():
    p = argparse.ArgumentParser(description='特殊船舶網路查證：匯出待查 / 回寫判決')
    sub = p.add_subparsers(dest='cmd', required=True)

    e = sub.add_parser('export', help='匯出待查清單 JSON')
    e.add_argument('--limit', type=int, default=30)
    e.add_argument('--rule-class', help='只出某一類（例：軍艦）')
    e.add_argument('--only-corrupt', action='store_true', help='只出船名疑似損壞的')
    e.add_argument('-o', '--out')
    e.set_defaults(func=cmd_export)

    a = sub.add_parser('apply', help='回寫查證結果')
    a.add_argument('verdicts', help='verdicts JSON 路徑')
    a.add_argument('--dry-run', action='store_true')
    a.set_defaults(func=cmd_apply)

    s = sub.add_parser('status', help='查證進度')
    s.set_defaults(func=cmd_status)

    args = p.parse_args()
    if not DB_URL:
        print('缺 SUPABASE_DB_URL', file=sys.stderr)
        return 1
    args.func(args)
    return 0


if __name__ == '__main__':
    sys.exit(main())
