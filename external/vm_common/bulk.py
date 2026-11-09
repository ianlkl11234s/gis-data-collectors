"""固定形狀的批次寫入（鐵則 R5）。stdlib + psycopg2 only.

背景：psycopg2.extras.execute_values 會把每一列展開成 ``VALUES (...),(...)``，
每批列數不同，pg_stat_statements 就把每種列數記成一筆不同的語句（查詢文字累積到
數百 MB，postgres_exporter 讀它時大量寫 temp）。

本模組把整批資料序列化成「一個 JSON 參數」，SQL 文字因此與批量無關：

    INSERT INTO t AS a (c1, c2) VALUES %s ON CONFLICT ...
      ↓
    INSERT INTO t AS a (c1, c2)
    SELECT _bulk.c0, _bulk.c1
    FROM json_to_recordset(%s::json) AS _bulk(c0 <c1 型別>, c1 <c2 型別>)

JSON 的 key 用短的位置名（c0、c1…、額外參數 p0…）並以緊湊分隔符輸出，payload 約為
舊 VALUES 文字的 1.2 倍（逐欄寫全名會到 2 倍）。
    ON CONFLICT ...

- ``execute_values(cur, sql, rows, template=None, page_size=100, fetch=False)``：
  與 psycopg2.extras.execute_values 同簽名的替代品，只支援
  ``INSERT INTO <table> [AS alias] (<cols>) VALUES %s <tail>``。ON CONFLICT／
  RETURNING 尾段原樣保留；``fetch=True`` 收齊每一頁的 RETURNING；分頁仍照
  page_size（每頁 SQL 文字相同）。欄位型別取自目標表的 catalog（每個 process
  每張表查一次，查詢本身也是固定形狀；遇到快取裡沒有的欄位會重查一次）。
- ``execute_json(cur, sql, rows, page_size=100, fetch=False)``：給非 INSERT 的寫法
  （例如 ``UPDATE ... FROM json_to_recordset(%s::json) AS v(...)``），呼叫端自己
  寫好欄位定義，rows 是 dict list。

template 支援的元素（逐欄、以頂層逗號分隔）：
- ``%s``                → 目標欄位型別
- ``%s::T``             → 目標欄位型別再轉 T
- 不含 ``%s`` 的運算式  → 原樣（例如 ``now()``）
- 其他含 ``%s`` 的運算式 → 每個 ``%s`` 以 text 傳入（例如 ``ST_GeomFromText(%s,4326)``、
  ``ST_GeomFromGeoJSON(%s)``）；需要數值的參數請在 template 寫明轉型，例如
  ``ST_MakePoint(%s::float8,%s::float8)``。

與舊寫法的值語意對齊（JSON 本身無法表達的部分）：
- float NaN／±Infinity 以字串 "NaN"／"Infinity" 傳（PG 數值輸入接受）。
- jsonb 欄位收到 str 時先 json.loads（舊寫法把字串當 JSON 文字解析）。
- json 欄位（保留原文）以 text 傳入再 ``::json``，存下的文字與舊寫法逐字相同。
- 整數欄位收到 float 時依 PG numeric→int 規則四捨五入（遠離零）。
- psycopg2 Json(...) 解開成原物件；datetime／date／time 用 isoformat；
  Decimal／UUID 用 str；timedelta 轉 interval 字串；bytes 轉 bytea hex。

注意：``cur.rowcount`` 與舊行為相同，只反映最後一頁。

本檔與 external/vm_common/bulk.py 必須逐位元組相同（tests/test_bulk.py 檢查）。
"""
from __future__ import annotations

import datetime as _dt
import decimal
import json
import math
import re
import uuid
from typing import Any, Iterable, Sequence

try:  # psycopg2 是 runtime 依賴；單元測試不一定要有連線
    from psycopg2.extras import Json as _PgJson
except Exception:  # noqa: BLE001
    _PgJson = None
try:  # psycopg2.Binary(...) 回傳的 adapter（bytea 欄位）
    from psycopg2.extensions import Binary as _PgBinary
except Exception:  # noqa: BLE001
    _PgBinary = None

__all__ = ['execute_values', 'execute_json', 'build_insert_sql', 'serialize_rows', 'clear_type_cache']

_INSERT_RE = re.compile(
    r'^\s*INSERT\s+INTO\s+(?P<table>[\w."]+)(?:\s+AS\s+(?P<alias>\w+))?\s*'
    r'\((?P<cols>[^()]*)\)\s*VALUES\s+%s(?P<tail>.*)$',
    re.IGNORECASE | re.DOTALL,
)
_CAST_RE = re.compile(r'^%s\s*::\s*(?P<type>.+)$', re.DOTALL)
_INT_TYPES = frozenset({'int2', 'int4', 'int8'})

# table -> {column: (format_type, base typname)}
_TYPE_CACHE: dict[str, dict[str, tuple[str, str]]] = {}

_CATALOG_SQL = (
    "SELECT a.attname, format_type(a.atttypid, a.atttypmod), "
    "COALESCE(bt.typname, t.typname) "
    "FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid "
    "LEFT JOIN pg_type bt ON bt.oid = NULLIF(t.typbasetype, 0) "
    "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped"
)


def clear_type_cache() -> None:
    _TYPE_CACHE.clear()


def _table_types(cur, table: str) -> dict[str, tuple[str, str]]:
    types = _TYPE_CACHE.get(table)
    if types is None:
        cur.execute(_CATALOG_SQL, (table,))
        types = {name: (fmt, base) for name, fmt, base in cur.fetchall()}
        if not types:
            raise ValueError(f'bulk: 找不到表 {table} 的欄位')
        _TYPE_CACHE[table] = types
    return types


def _split_top_level(text: str) -> list[str]:
    parts, depth, buf, quote = [], 0, [], False
    for ch in text:
        if ch == "'":
            quote = not quote
        elif not quote:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            elif ch == ',' and depth == 0:
                parts.append(''.join(buf).strip())
                buf = []
                continue
        buf.append(ch)
    parts.append(''.join(buf).strip())
    return parts


def _parse_template(template: str | None, ncols: int) -> list[str]:
    if template is None:
        return ['%s'] * ncols
    body = template.strip()
    if not (body.startswith('(') and body.endswith(')')):
        raise ValueError(f'bulk: template 必須以括號包住：{template!r}')
    elems = _split_top_level(body[1:-1])
    if len(elems) != ncols:
        raise ValueError(f'bulk: template 有 {len(elems)} 個元素，欄位有 {ncols} 個')
    if '%(' in body:
        raise ValueError('bulk: 不支援具名 template 參數')
    return elems


def build_insert_sql(sql: str, types: dict[str, tuple[str, str]], template: str | None = None):
    """回傳 (固定形狀 SQL, 參數規格)。參數規格是 list[(json key, base typname)]，順序同每列 tuple。"""
    m = _INSERT_RE.match(sql)
    if not m:
        raise ValueError(f'bulk: 只支援 INSERT INTO <table> (<cols>) VALUES %s ...，收到：{sql[:120]!r}')
    table, alias, cols_text, tail = m.group('table'), m.group('alias'), m.group('cols'), m.group('tail')
    if '%s' in tail or '%(' in tail:
        raise ValueError('bulk: VALUES %s 之後不可再有參數')
    cols = [c.strip() for c in cols_text.split(',') if c.strip()]
    elems = _parse_template(template, len(cols))

    select_exprs: list[str] = []
    defs: list[str] = []
    spec: list[tuple[str, str]] = []
    extra = 0
    for idx, (col, elem) in enumerate(zip(cols, elems)):
        name = col.strip('"')
        if name not in types:
            raise ValueError(f'bulk: 表 {table} 沒有欄位 {name}')
        fmt, base = types[name]
        key = f'c{idx}'
        ref = f'_bulk.{key}'
        if base == 'json':  # 保留原文：以 text 傳、再轉 json
            fmt, ref = 'text', f'{ref}::json'
        n = elem.count('%s')
        if n == 0:
            select_exprs.append(elem)
            continue
        if elem == '%s':
            select_exprs.append(ref)
            defs.append(f'{key} {fmt}')
            spec.append((key, base))
            continue
        cast = _CAST_RE.match(elem)
        if cast and n == 1:
            select_exprs.append(f'{ref}::{cast.group("type").strip()}')
            defs.append(f'{key} {fmt}')
            spec.append((key, base))
            continue
        pieces = elem.split('%s')
        expr = pieces[0]
        for piece in pieces[1:]:
            pkey = f'p{extra}'
            extra += 1
            expr += f'_bulk.{pkey}' + piece
            defs.append(f'{pkey} text')
            spec.append((pkey, 'text'))
        select_exprs.append(expr)

    target = table + (f' AS {alias}' if alias else '')
    fixed = (
        f'INSERT INTO {target} ({", ".join(cols)}) '
        f'SELECT {", ".join(select_exprs)} '
        f'FROM json_to_recordset(%s::json) AS _bulk({", ".join(defs)})'
        f'{tail}'
    )
    return fixed, spec


def _interval(td: _dt.timedelta) -> str:
    return f'{td.days} days {td.seconds} seconds {td.microseconds} microseconds'


def _plain(value: Any) -> Any:
    """把單一值轉成 JSON 可表達、且 PG 輸入函式語意與 psycopg2 literal 相同的形式。"""
    if value is None or isinstance(value, (bool, str, int)):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return 'NaN'
        if math.isinf(value):
            return 'Infinity' if value > 0 else '-Infinity'
        return value
    if _PgJson is not None and isinstance(value, _PgJson):
        return value.adapted
    if _PgBinary is not None and isinstance(value, _PgBinary):
        return _plain(value.adapted)
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, _dt.timedelta):
        return _interval(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return '\\x' + bytes(value).hex()
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _to_int(value: float) -> Any:
    if math.isnan(value) or math.isinf(value):
        return _plain(value)  # 交給 PG 報錯，與舊寫法相同
    return int(decimal.Decimal(repr(value)).to_integral_value(rounding=decimal.ROUND_HALF_UP))


def _convert(value: Any, base: str) -> Any:
    if base == 'json':
        if value is None or isinstance(value, str):
            return value
        if _PgJson is not None and isinstance(value, _PgJson):
            return value.dumps(value.adapted)
        return json.dumps(value)
    if base == 'jsonb':
        if isinstance(value, str):
            return json.loads(value)
        if _PgJson is not None and isinstance(value, _PgJson):
            return value.adapted
        return value
    if base in _INT_TYPES and isinstance(value, float):
        return _to_int(value)
    return _plain(value)


def serialize_rows(rows: Iterable[Sequence[Any]], spec: list[tuple[str, str]]) -> str:
    out = []
    width = len(spec)
    for row in rows:
        if len(row) != width:
            raise ValueError(f'bulk: 每列需 {width} 個值，收到 {len(row)} 個')
        out.append({key: _convert(v, base) for (key, base), v in zip(spec, row)})
    return json.dumps(out, separators=(',', ':'))


def _pages(rows: Sequence[Any], page_size: int):
    for i in range(0, len(rows), page_size):
        yield rows[i:i + page_size]


def execute_values(cur, sql: str, argslist, template: str | None = None,
                   page_size: int = 100, fetch: bool = False):
    """psycopg2.extras.execute_values 的固定形狀替代品（見模組說明）。"""
    rows = list(argslist)
    if not rows:
        return [] if fetch else None
    m = _INSERT_RE.match(sql)
    if not m:
        raise ValueError(f'bulk: 只支援 INSERT INTO <table> (<cols>) VALUES %s ...，收到：{sql[:120]!r}')
    table = m.group('table')
    try:
        fixed, spec = build_insert_sql(sql, _table_types(cur, table), template)
    except ValueError:
        if table not in _TYPE_CACHE:
            raise
        # 型別快取可能早於 migration（process 未重啟）→ 重查一次
        del _TYPE_CACHE[table]
        fixed, spec = build_insert_sql(sql, _table_types(cur, table), template)
    result = []
    for page in _pages(rows, page_size):
        cur.execute(fixed, (serialize_rows(page, spec),))
        if fetch:
            result.extend(cur.fetchall())
    return result if fetch else None


def execute_json(cur, sql: str, rows: Sequence[dict], page_size: int = 100, fetch: bool = False):
    """sql 只含一個 %s（JSON 陣列），呼叫端自行寫 json_to_recordset(%s::json) AS v(欄 型別, ...)。"""
    rows = list(rows)
    if not rows:
        return [] if fetch else None
    result = []
    for page in _pages(rows, page_size):
        payload = json.dumps([{k: _plain(v) for k, v in r.items()} for r in page], separators=(',', ':'))
        cur.execute(sql, (payload,))
        if fetch:
            result.extend(cur.fetchall())
    return result if fetch else None
