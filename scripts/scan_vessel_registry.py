#!/usr/bin/env python3
"""
每週掃描：更新特殊船舶名冊 live.vessel_watch_registry，並印出「待你人工確認」的船。

搭配 gis-platform/migrations/339_vessel_watch.sql 與 backfill_vessel_watch.py。
這支是**手動執行**的（用戶 2026-08-12：「我可以每週都去掃一次」），不進排程。

它做四件事：
  1. 掃最近 N 天的 S3 raw → upsert registry 的**規則欄位**
     （imo / call_sign / 船名 / 尺寸 / rule_class / first_seen / last_seen）
  2. 補掃 DB 母表最近幾天（S3 archive 延遲約 6 天，這段只有 DB 有）
  3. 印出待審清單：規則認不出的、以及本週新出現的船
  4. 跑 VZ-8「壞 MMSI 守門規則」，印出疑似假 MMSI 清單（見下方 VZ-8 段落）

⚠️ 它**永遠不會**覆寫 confirmed_class / note / is_excluded —— 你標過的分類不會被洗掉。
   （這個保證來自 backfill_vessel_watch.REGISTRY_SQL 的 ON CONFLICT DO UPDATE 欄位清單。）

── VZ-8：壞 MMSI 守門規則（2026-08-20）──────────────────────────
背景：用「相異船名 >3」這條規則抓假 MMSI 時，兩次差點誤殺真船——
  994161168 有 82 個相異船名，看起來是典型假碼，實際上是**台灣籍自主無人載具
  MATANGI AUTONOMOUS**，那 82 個「船名」全是 `CT4-2073-XXXX%` 格式的**電量／
  狀態百分比回報**；412819678（中國漁政真船）3 個「船名」是 `YU ZHENG81967`
  加電量%後綴。兩艘都不是假 MMSI 在跳船名，是同一艘船的狀態回報格式長得像
  很多不同船名。413555220 是中國海監「海監 66」真船，99.97% 定位用同一船名，
  那幾個「相異船名」只是亂碼雜訊，被 90% 占比門檻自然擋掉。

三條規則命中任一即標記「待審」（VZ8_HIT）——**規則只找、不判**，一律留給人工：
  A. 船名分散：相異船名（正規化後）>3 且最大單一船名占比 <90%
     正規化只去尾端「分隔符+1~4位數字+%」（電量/狀態格式），刻意不去所有尾端
     數字——見 normalize_ship_name() docstring 為什麼要窄。
  B. 格式違規：非 9 位數字，或首碼不在 ITU-R M.585 合法 MID 首碼 2-7 範圍
     （對齊 gis-platform migrations/341_vessel_watch_mmsi_guard.sql 的
     classify_vessel() 判準）。
  C. 物理矛盾：相鄰定位點（時間差 ≤1 小時）隱含速度 >40 節，累積 ≥10 次
     （真船最多 1~2 次 GPS 噪點；假碼實測 13~488 次。⚠️ 這個門檻是觀察到的
     自然斷點，未做統計檢定，不是嚴謹的統計顯著性判斷）。

⚠️ 99 開頭（AtoN／助航設施格式，如 994161168）**不套用上述任何一條規則**——
   它們本來就會用非船名格式廣播，格式規則對它們是範疇錯置。獨立列在
   「99 開頭」報告區塊，不混進假碼待審清單。

⚠️ VZ-8 只印報告，**不寫 DB**（本檔連線用唯讀 transaction 即可）。目前 registry
   沒有 needs_review 欄位；要不要新增是 gis-platform 那邊的決定，不在本檔範圍。

用法：
    python3 scripts/scan_vessel_registry.py                 # 掃最近 14 天（涵蓋 archive 延遲）
    python3 scripts/scan_vessel_registry.py --days 30
    python3 scripts/scan_vessel_registry.py --report-only   # 不掃，只印目前待審清單

審完怎麼標（psql）：
    -- 確認是中國海警
    UPDATE live.vessel_watch_registry
       SET confirmed_class = '中國海警', confirmed_at = now()
     WHERE mmsi = '413xxxxxx';

    -- 確認是誤收的民船（留列不刪，否則下週掃描又會提示一次）
    UPDATE live.vessel_watch_registry
       SET is_excluded = true, note = '台灣民間拖船', confirmed_at = now()
     WHERE mmsi = '416xxxxxx';
"""
import sys
import os
import re
import logging
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2

from storage.s3 import S3Storage
from backfill_vessel_watch import DB_URL, collect_day, write_day

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# S3 archive 延遲約 6 天，預設掃 14 天確保無縫
DEFAULT_DAYS = 14

# 母表補掃：S3 還沒打包的那幾天，改用 DB 端 sweep（同一份 SQL 收錄條件）
SWEEP_SQL = "SELECT live.sweep_vessel_watch(%s::interval)"

# 母表 → registry 的補掃。DB 沒有 imo/call_sign/尺寸，只能補身分與時序，
# 但至少讓最近幾天新出現的船不會漏進名冊。
REGISTRY_FROM_DB_SQL = """
WITH latest AS (
    SELECT DISTINCT ON (mmsi) mmsi, ship_name, ship_type, collected_at
    FROM live.ship_positions
    WHERE collected_at >= now() - %s::interval
      AND mmsi IS NOT NULL
      AND live.is_watch_candidate(mmsi, ship_name, ship_type)
    ORDER BY mmsi, collected_at DESC
),
span AS (
    SELECT mmsi, min(collected_at) f, max(collected_at) l
    FROM live.ship_positions
    WHERE collected_at >= now() - %s::interval
      AND mmsi IN (SELECT mmsi FROM latest)
    GROUP BY mmsi
)
INSERT INTO live.vessel_watch_registry
    (mmsi, names_seen, rule_class, rule_flag, matched_by, first_seen, last_seen, last_scan_at)
SELECT l.mmsi,
       CASE WHEN coalesce(l.ship_name,'') = '' THEN '{}'::text[] ELSE ARRAY[l.ship_name] END,
       c.vessel_class, c.flag, c.matched_by, s.f, s.l, now()
FROM latest l
JOIN span s USING (mmsi)
CROSS JOIN LATERAL live.classify_vessel(l.mmsi, l.ship_name, l.ship_type) AS c
ON CONFLICT (mmsi) DO UPDATE SET
    -- ⚠️ COALESCE 不可省，理由同 backfill_vessel_watch.REGISTRY_SQL
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

SUMMARY_SQL = """
SELECT COALESCE(effective_class, '（規則認不出）') AS cls,
       count(*) AS n,
       count(*) FILTER (WHERE confirmed_class IS NOT NULL) AS confirmed
FROM live.vessel_watch_registry
WHERE NOT is_excluded
GROUP BY 1 ORDER BY 2 DESC
"""

PENDING_SQL = """
SELECT mmsi,
       COALESCE(imo, '-')                          AS imo,
       COALESCE(call_sign, '-')                    AS call_sign,
       COALESCE(array_to_string(names_seen, ' / '), '-') AS names,
       COALESCE(rule_class, '—')                   AS rule_class,
       COALESCE(matched_by, '-')                   AS matched_by,
       to_char(last_seen AT TIME ZONE 'Asia/Taipei', 'MM-DD HH24:MI') AS last_seen
FROM live.vessel_watch_registry
WHERE confirmed_class IS NULL
  AND NOT is_excluded
  AND (rule_class IS NULL OR last_scan_at >= now() - %s::interval)
ORDER BY (rule_class IS NULL) DESC, last_seen DESC NULLS LAST
LIMIT 200
"""


# ── VZ-8：壞 MMSI 守門規則（實作；理由見檔頭 docstring）─────────

# 規則 B 的格式門檻：對齊 gis-platform migrations/341_vessel_watch_mmsi_guard.sql
# 裡 classify_vessel() 用的同一套 ITU-R M.585 判準（9 位數、首碼 2-7）。
VALID_MMSI_RE = re.compile(r'^[2-7][0-9]{8}$')

# 99 開頭＝AtoN／助航設施格式，範疇不同，一律排除在三條規則之外。
ATON_MMSI_RE = re.compile(r'^99')

# 規則 A 的船名尾碼：只認「分隔符 + 1~4 位數字 + %」，不認裸尾端數字。
_NAME_SUFFIX_RE = re.compile(r'[\s\-_]*\d{1,4}\s*%$')

# 規則 C 的門檻。實測：真船最多 1~2 次 GPS 噪點，假碼 13~488 次——
# 40 節 / 10 次是觀察到的自然斷點，⚠️ 未做統計檢定，是經驗值不是顯著性判斷。
SPEED_THRESHOLD_KN = 40
SPEED_MIN_COUNT = 10


def normalize_ship_name(name):
    """
    正規化船名以便計算「相異船名數」，只去掉尾端「分隔符+1~4位數字+%」這種
    電量／狀態回報格式，**不**去所有尾端數字。

    案例（VZ-8 踩坑，2026-08-20 查證）：
      994161168 MATANGI AUTONOMOUS（台灣籍自主無人載具）用 82 個「船名」
      廣播狀態百分比：CT4-2073-1801% / CT4-2073-1817% / CT4-2073-1818% / …
      正規化後全部收斂成 'CT4-2073'，相異數從 82 降到個位數（剩下的是
      幾筆亂碼雜訊，各自只出現一次，不影響 >3 判斷）。
      412819678（中國漁政真船）"YU ZHENG81967-89%" / "-90%" / "-99%"
      同理收斂成 'YU ZHENG81967'（3 個本來就不會被 >3 門檻命中，但正規化
      後更乾淨）。

    為什麼刻意窄（只認「數字+%」，不認裸尾端數字）：
      412000000 這類已確認假 MMSI 的典型手法是直接換好幾個完全不相關的
      船名（CHINACOASTGUARD18602 / MIN DONG YU 63360 / YUAN HAI 088…），
      這些名字本來就該被算成「相異」。如果正規化把所有尾端數字都吃掉，
      會連 SHUNDA168 這類正常船名型號尾碼也一起吃掉，稀釋掉真正的訊號、
      弱化規則對假 MMSI 的偵測力。只認「數字+%」這個電量/狀態回報獨有的
      格式，才能精準只解決 MATANGI／YU ZHENG 這種案例，不傷及其他判斷。
    """
    n = (name or '').strip().upper()
    if not n:
        return ''
    return _NAME_SUFFIX_RE.sub('', n).strip() or n


# 規則 A 的資料來源：逐筆船名分布（registry.names_seen 只有去重集合，
# 沒有出現次數，算不出「最大單一船名占比」）。
NAME_COUNTS_SQL = """
SELECT mmsi, ship_name, count(*)
FROM live.vessel_watch_positions
WHERE mmsi !~ '^99' AND ship_name IS NOT NULL AND ship_name <> ''
GROUP BY mmsi, ship_name
"""

# 規則 C 的資料來源：同船相鄰兩筆定位點的隱含速度（大圓距離 / 時間差）。
SPEED_JUMP_SQL = """
WITH ordered AS (
    SELECT mmsi, collected_at, lat, lng,
           lag(collected_at) OVER w AS prev_t,
           lag(lat) OVER w AS prev_lat,
           lag(lng) OVER w AS prev_lng
    FROM live.vessel_watch_positions
    WHERE mmsi !~ '^99' AND lat IS NOT NULL AND lng IS NOT NULL
    WINDOW w AS (PARTITION BY mmsi ORDER BY collected_at)
),
diffs AS (
    SELECT mmsi,
           ST_DistanceSphere(ST_MakePoint(lng, lat), ST_MakePoint(prev_lng, prev_lat)) AS dist_m,
           EXTRACT(EPOCH FROM (collected_at - prev_t)) / 3600.0 AS dt_h
    FROM ordered
    WHERE prev_t IS NOT NULL
),
speeds AS (
    -- 時間差 ≤1 小時才算——超過 1 小時的間隔本來就可能包含正常移動的長距離
    SELECT mmsi, dist_m / 1852.0 / dt_h AS speed_kn
    FROM diffs
    WHERE dt_h > 0 AND dt_h <= 1.0
)
SELECT mmsi, count(*) AS n_fast, max(speed_kn) AS max_kn
FROM speeds
WHERE speed_kn > %s
GROUP BY mmsi
HAVING count(*) >= %s
"""

# 規則 B／整體掃描範圍：registry ∪ positions 的 mmsi（排除 99 開頭）。
# 用 UNION 而非只用 positions，是因為週掃只更新 registry（registry_only=True），
# 有些 mmsi 可能還沒被 hourly sweep／backfill 寫進 positions，但格式違規
# 不需要位置資料就能判——只用 positions 會漏掉這些。
ALL_MMSI_SQL = """
SELECT mmsi FROM live.vessel_watch_registry WHERE mmsi !~ '^99'
UNION
SELECT DISTINCT mmsi FROM live.vessel_watch_positions WHERE mmsi !~ '^99'
"""

# 99 開頭清單（獨立列出，見檔頭「不套用上述任何一條規則」）。
ATON_INFO_SQL = """
SELECT r.mmsi, r.names_seen, r.is_excluded, r.confirmed_class, r.note,
       (SELECT count(*) FROM live.vessel_watch_positions p WHERE p.mmsi = r.mmsi) AS n_pos
FROM live.vessel_watch_registry r
WHERE r.mmsi ~ '^99'
ORDER BY r.mmsi
"""

# 待審清單用：命中規則的 mmsi 目前的人工審查狀態（判斷是「新圈出」還是
# 「已經審過、這次只是複驗」）。與檔頭 PENDING_SQL 用同一套「未審」定義
# （confirmed_class IS NULL AND NOT is_excluded），全檔保持一致。
REVIEW_STATUS_SQL = """
SELECT mmsi, is_excluded, confirmed_class,
       COALESCE(array_to_string(names_seen, ' / '), '-')
FROM live.vessel_watch_registry
WHERE mmsi = ANY(%s)
"""


def rule_a_name_diversity(conn):
    """規則 A：船名分散。回傳 {mmsi: {distinct, max_name, max_share, total}}。"""
    with conn.cursor() as cur:
        cur.execute(NAME_COUNTS_SQL)
        rows = cur.fetchall()

    per_mmsi = {}
    for mmsi, name, n in rows:
        norm = normalize_ship_name(name)
        d = per_mmsi.setdefault(mmsi, {})
        d[norm] = d.get(norm, 0) + n

    hits = {}
    for mmsi, counts in per_mmsi.items():
        total = sum(counts.values())
        distinct = len(counts)
        if distinct <= 3 or total == 0:
            continue
        max_name, max_n = max(counts.items(), key=lambda kv: kv[1])
        share = max_n / total
        if share < 0.90:
            hits[mmsi] = {'distinct': distinct, 'max_name': max_name,
                          'max_share': share, 'total': total}
    return hits


def rule_b_format_violation(mmsi_list):
    """規則 B：格式違規。呼叫端須先濾掉 99 開頭（見 ALL_MMSI_SQL 註解）。"""
    return {m for m in mmsi_list if not VALID_MMSI_RE.match(m)}


def rule_c_physical_contradiction(conn):
    """規則 C：物理矛盾。回傳 {mmsi: {n_fast, max_kn}}。"""
    with conn.cursor() as cur:
        cur.execute(SPEED_JUMP_SQL, (SPEED_THRESHOLD_KN, SPEED_MIN_COUNT))
        rows = cur.fetchall()
    return {mmsi: {'n_fast': n, 'max_kn': round(float(mx), 1)} for mmsi, n, mx in rows}


def scan_vz8(conn):
    """
    跑 VZ-8 三條規則，回傳 (hits, aton_rows)：
      hits: {mmsi: {'rules': ['A','C',...], 'evidence': '……'}} —— 命中任一即入列
      aton_rows: 99 開頭清單（ATON_INFO_SQL 的原始 rows），不套用三條規則
    純唯讀查詢，不寫 DB。
    """
    with conn.cursor() as cur:
        cur.execute(ALL_MMSI_SQL)
        all_mmsi = [r[0] for r in cur.fetchall()]

    a_hits = rule_a_name_diversity(conn)
    b_hits = rule_b_format_violation(all_mmsi)
    c_hits = rule_c_physical_contradiction(conn)

    hits = {}
    for mmsi in set(a_hits) | b_hits | set(c_hits):
        rules, evidence = [], []
        if mmsi in a_hits:
            e = a_hits[mmsi]
            rules.append('A')
            evidence.append(f"船名分散：正規化後 {e['distinct']} 個相異，"
                             f"最大占比 {e['max_share'] * 100:.0f}%")
        if mmsi in b_hits:
            rules.append('B')
            evidence.append('格式違規：非 9 位數或首碼不在 2-7')
        if mmsi in c_hits:
            e = c_hits[mmsi]
            rules.append('C')
            evidence.append(f"物理矛盾：{e['n_fast']} 次時速>{SPEED_THRESHOLD_KN}節"
                             f"（最高 {e['max_kn']} 節）")
        hits[mmsi] = {'rules': rules, 'evidence': '；'.join(evidence)}

    with conn.cursor() as cur:
        cur.execute(ATON_INFO_SQL)
        aton_rows = cur.fetchall()

    return hits, aton_rows


def print_table(rows, headers):
    if not rows:
        print('  （無）')
        return
    cols = list(zip(*([headers] + [[str(c) for c in r] for r in rows])))
    w = [min(max(len(x) for x in col), 46) for col in cols]
    line = '  ' + '  '.join(h[:w[i]].ljust(w[i]) for i, h in enumerate(headers))
    print(line)
    print('  ' + '  '.join('-' * x for x in w))
    for r in rows:
        print('  ' + '  '.join(str(c)[:w[i]].ljust(w[i]) for i, c in enumerate(r)))


def main():
    import argparse
    p = argparse.ArgumentParser(description='每週掃描更新特殊船舶名冊')
    p.add_argument('--days', type=int, default=DEFAULT_DAYS, help=f'回看天數（預設 {DEFAULT_DAYS}）')
    p.add_argument('--report-only', action='store_true', help='不掃描，只印目前待審清單')
    args = p.parse_args()

    if not DB_URL:
        logger.error('缺 SUPABASE_DB_URL')
        return 1

    conn = psycopg2.connect(DB_URL)
    window = f'{args.days} days'

    try:
        if not args.report_only:
            # ── 1. S3 掃描（有 imo / call_sign / 尺寸）──
            s3 = S3Storage()
            today = date.today()
            n_ship = 0
            for i in range(args.days, 0, -1):
                d = today - timedelta(days=i)
                positions, registry = collect_day(s3, d)
                if not registry:
                    continue
                # 週掃只更新名冊；軌跡由 DB 端 sweep cron 每小時負責
                _, nr = write_day(conn, positions, registry, registry_only=True)
                n_ship += nr
                logger.info(f'  S3 {d}: {nr} 艘')
            logger.info(f'S3 掃描完成，累計 {n_ship} 艘次')

            # ── 2. 母表補掃（S3 尚未打包的最近幾天）──
            with conn.cursor() as cur:
                cur.execute(SWEEP_SQL, (window,))
                swept = cur.fetchone()[0]
                cur.execute(REGISTRY_FROM_DB_SQL, (window, window))
            conn.commit()
            logger.info(f'母表補掃：軌跡 +{swept} 筆，名冊已同步')

        # ── 3. 報告 ──
        with conn.cursor() as cur:
            cur.execute(SUMMARY_SQL)
            summary = cur.fetchall()
            cur.execute(PENDING_SQL, (window,))
            pending = cur.fetchall()

        print('\n═══ 名冊現況（已排除人工標記的誤收）═══')
        print_table(summary, ['分類', '艘數', '已人工確認'])

        print(f'\n═══ 待你確認（規則認不出，或最近 {args.days} 天內有活動）═══')
        print_table(pending, ['MMSI', 'IMO', '呼號', '看過的船名', '規則判定', '依據', '最後出現'])
        if pending:
            print(f'\n  共 {len(pending)} 艘待審。標記方式見本檔檔頭 docstring。')

        # ── 4. VZ-8：壞 MMSI 守門規則（純唯讀計算，不寫 DB）──
        vz8_hits, aton_rows = scan_vz8(conn)

        with conn.cursor() as cur:
            cur.execute(REVIEW_STATUS_SQL, (list(vz8_hits),))
            review_status = {r[0]: r for r in cur.fetchall()}

        new_rows, reviewed_rows = [], []
        for mmsi in sorted(vz8_hits):
            h = vz8_hits[mmsi]
            st = review_status.get(mmsi)
            already_reviewed = bool(st and (st[1] or st[2]))  # is_excluded or confirmed_class
            names = st[3] if st else '-'
            row = (mmsi, ''.join(h['rules']), h['evidence'], names)
            (reviewed_rows if already_reviewed else new_rows).append(row)

        print('\n═══ VZ-8 壞 MMSI 守門規則命中（規則只找不判，一律待人工審）═══')
        print_table(new_rows, ['MMSI', '規則', '依據', '看過的船名'])
        if new_rows:
            print(f'\n  共 {len(new_rows)} 艘本次新圈出、尚未人工審過，標記方式見檔頭 docstring。')
        if reviewed_rows:
            print(f'\n  另有 {len(reviewed_rows)} 艘命中規則但已有人工標記'
                  f'（is_excluded 或 confirmed_class），僅供核對，不必重審：'
                  f' {", ".join(r[0] for r in reviewed_rows)}')

        print('\n═══ 99 開頭（AtoN／助航設施，不套用一般船舶身份規則）═══')
        aton_print_rows = [
            (m, '✓' if excl else '-', conf or '-', npos, (note or '-'))
            for m, names, excl, conf, note, npos in aton_rows
        ]
        print_table(aton_print_rows, ['MMSI', '已排除', '已確認分類', '軌跡筆數', '備註'])
    finally:
        conn.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
