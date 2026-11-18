"""collector 執行收據（gis-platform migration 558 的 public.report_collector_run）。

每輪結束時 writer 呼叫一次 report_collector_run(name, payload jsonb)，除了舊心跳的成功／失敗／送出筆數，
另外帶：
  new_rows / rows_basis  實際新增列數（RETURNING 計數）與它的意義：
                         'dedup'＝有去重（ON CONFLICT 或 history dedup），新增列數代表新資料；
                         'append'＝單純 INSERT，每輪都會新增，DB 不拿來判定有無新資料
  parse_ok               collector 在結果帶 `_parse`（True／False）時才送
  source_date            結果帶 `_source_date`（來源提供日期）
  upstream_max_at        結果帶 `_upstream_max_at`，或 TABLE_MAP 的 `upstream_time_col` 欄的最大值
  expected_interval_min / freshness_kind
                         依 config/realtime_tables.yaml（同 tasks/staleness_watch 的判斷）：
                         有正常表 → regular（取最小 expected_interval_min）；
                         全是事件型 → event；全在季外 → off_season。pulse-data 用「×3」當「有跑但沒新資料」門檻。

沒填就不送（DB 記未知），絕不用 0 代替。所有函式失敗只回空值，不影響寫入路徑。
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)

TAIPEI_TZ = timezone(timedelta(hours=8))
MAX_FUTURE = timedelta(days=1)          # 晚於現在 1 天以上的上游時間視為錯誤（DB 端也會擋）
ROWS_BASES = ('dedup', 'append')

_DATE_RE = re.compile(r'^\s*(\d{3,4})[-/.]?(\d{1,2})[-/.]?(\d{1,2})(?:\D.*)?$')


def to_aware(value: Any) -> Optional[datetime]:
    """ISO 字串／datetime → tz-aware datetime（naive 視為台北時間）；無法解析回 None。"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        return None
    else:
        text = str(value).strip()
        if text.endswith('Z'):
            text = text[:-1] + '+00:00'
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TAIPEI_TZ)
    return dt


def max_upstream(values: Iterable[Any], now: Optional[datetime] = None) -> Optional[datetime]:
    """取可解析、且不晚於 now + 1 天的最大時間。"""
    limit = (now or datetime.now(TAIPEI_TZ)) + MAX_FUTURE
    best = None
    for v in values:
        dt = to_aware(v)
        if dt is not None and dt <= limit and (best is None or dt > best):
            best = dt
    return best


def max_upstream_iso(values: Iterable[Any]) -> Optional[str]:
    """collector 結果用：max_upstream 的 ISO 字串版（結果 dict 會存成本機 JSON）。"""
    best = max_upstream(values)
    return best.isoformat() if best is not None else None


def norm_source_date(value: Any, today: Optional[date] = None) -> Optional[str]:
    """來源提供日期 → 'YYYY-MM-DD'。接受 date／datetime、2026-11-17、2026/11/17、20261117、
    民國 115/11/17；不合法或晚於明天回 None。"""
    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        d = value.date()
    elif isinstance(value, date):
        d = value
    else:
        m = _DATE_RE.match(str(value))
        if not m:
            return None
        y, mo, da = (int(x) for x in m.groups())
        if y < 1000:
            y += 1911
        try:
            d = date(y, mo, da)
        except ValueError:
            return None
    today = today or datetime.now(TAIPEI_TZ).date()
    if d.year < 1900 or d > today + timedelta(days=1):
        return None
    return d.isoformat()


def run_info(result: Optional[dict], records: Optional[list] = None,
             upstream_col: Optional[str] = None, rows: Optional[tuple] = None) -> dict:
    """從 collector 結果與寫入結果組出收據欄位（只放已知的值）。"""
    info: dict = {}
    try:
        if rows and rows[0] is not None:
            info['new_rows'] = int(rows[0])
            if rows[1] in ROWS_BASES:
                info['rows_basis'] = rows[1]
        result = result or {}
        if isinstance(result.get('_parse'), bool):
            info['parse_ok'] = result['_parse']
        if result.get('_source_date') is not None:
            info['source_date'] = result['_source_date']
        candidates = [result.get('_upstream_max_at')]
        if upstream_col and records:
            candidates.extend(r.get(upstream_col) for r in records if isinstance(r, dict))
        up = max_upstream(candidates)
        if up is not None:
            info['upstream_max_at'] = up
    except Exception as e:  # 收據只是觀測，不能影響寫入
        logger.debug(f'run_info 失敗: {e}')
    return info


# ── 預期間隔與類型（realtime_tables.yaml）────────────────────────────────────
_policy_cache: dict[str, tuple[date, dict]] = {}


def freshness_policy(collector_name: str, now: Optional[datetime] = None) -> dict:
    """{'expected_interval_min': int, 'freshness_kind': 'regular'|'event'|'off_season'}；查不到回 {}。
    判斷沿用 tasks.monitoring.stale_check_skip_reason（與即時停更告警同一套規則），每個台北日算一次。"""
    now = (now or datetime.now(TAIPEI_TZ)).astimezone(TAIPEI_TZ)
    hit = _policy_cache.get(collector_name)
    if hit and hit[0] == now.date():
        return hit[1]
    policy: dict = {}
    try:
        from tasks import monitoring
        tables = monitoring.load_realtime_tables()
        cmap = monitoring.load_cross_layer_map()
        cfg = cmap.get(collector_name) if isinstance(cmap.get(collector_name), dict) else {}
        listed = set(cfg.get('supabase_tables') or [])
        own = [t for t in tables
               if t.get('owner_collector') == collector_name or f"{t['schema']}.{t['table']}" in listed]
        reasons = [monitoring.stale_check_skip_reason(t, cmap, now) for t in own]
        for kind, want in (('regular', None), ('event', 'event_driven'), ('off_season', 'off_season')):
            group = [t for t, r in zip(own, reasons) if r == want]
            if group:
                intervals = [int(t.get('expected_interval_min') or 0) for t in group]
                intervals = [i for i in intervals if i > 0]
                policy = {'freshness_kind': kind}
                if intervals:
                    policy['expected_interval_min'] = min(intervals)
                break
    except Exception as e:
        logger.debug(f'freshness_policy({collector_name}) 失敗: {e}')
        policy = {}
    _policy_cache[collector_name] = (now.date(), policy)
    return policy


def build_payload(collector_name: str, success: bool, records: int = 0, error: Optional[str] = None,
                  run: Optional[dict] = None, now: Optional[datetime] = None) -> dict:
    """report_collector_run 的 payload。未知的欄位不放（DB 記 NULL＝未知）。"""
    payload = {'success': bool(success), 'records': max(int(records or 0), 0), 'error': error}
    run = run or {}
    new_rows = run.get('new_rows')
    if isinstance(new_rows, int) and not isinstance(new_rows, bool) and new_rows >= 0:
        payload['new_rows'] = new_rows
        if run.get('rows_basis') in ROWS_BASES:
            payload['rows_basis'] = run['rows_basis']
    if isinstance(run.get('parse_ok'), bool):
        payload['parse_ok'] = run['parse_ok']
    source_date = norm_source_date(run.get('source_date'))
    if source_date:
        payload['source_date'] = source_date
    up = max_upstream([run.get('upstream_max_at')], now)
    if up is not None:
        payload['upstream_max_at'] = up.isoformat()
    payload.update(freshness_policy(collector_name, now))
    return payload
