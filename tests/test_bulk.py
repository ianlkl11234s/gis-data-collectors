"""storage/bulk.py：固定形狀批次寫入（鐵則 R5）。

單元測試不需資料庫。整合測試（新舊寫法逐列等價、pg_stat_statements 只留一筆）
走既有的 live_supabase 開關：TEST_SUPABASE_DB_URL 指向本機拋棄式 PG，加
--run-live-supabase。只動自建的 bulk_t schema，不 reset pg_stat_statements。
"""
import datetime as dt
import decimal
import json
import math
import os
import uuid
from pathlib import Path

import pytest
from psycopg2.extras import Json

from storage import bulk

ROOT = Path(__file__).resolve().parents[1]

TYPES = {
    'id': ('integer', 'int4'),
    'name': ('text', 'text'),
    'val': ('double precision', 'float8'),
    'payload': ('jsonb', 'jsonb'),
    'geom': ('geometry(Point,4326)', 'geometry'),
    'tags': ('text[]', 'text'),
    'updated_at': ('timestamp with time zone', 'timestamptz'),
}


class RecordingCursor:
    def __init__(self, types=None, returning=None):
        self.types = types or TYPES
        self.calls = []
        self.returning = returning or []
        self._last = None

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if sql == bulk._CATALOG_SQL:
            self._last = [(k, fmt, base) for k, (fmt, base) in self.types.items()]
        else:
            self._last = list(self.returning)

    def fetchall(self):
        return self._last


@pytest.fixture(autouse=True)
def _clear_cache():
    bulk.clear_type_cache()
    yield
    bulk.clear_type_cache()


def _data_sql(cur):
    return [sql for sql, _ in cur.calls if sql != bulk._CATALOG_SQL]


SQL = ("INSERT INTO live.demo AS t (id, name, val) VALUES %s "
       "ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name WHERE t.val IS DISTINCT FROM EXCLUDED.val")


def test_same_sql_text_for_any_batch_size():
    seen = set()
    for n in (1, 2, 7, 99, 100, 101, 1000, 1500):
        cur = RecordingCursor()
        bulk.execute_values(cur, SQL, [(i, f'n{i}', i * 1.5) for i in range(n)], page_size=1000)
        texts = set(_data_sql(cur))
        assert len(texts) == 1
        seen |= texts
    assert len(seen) == 1
    (only,) = seen
    assert 'VALUES' not in only
    assert only.count('%s') == 1
    assert 'json_to_recordset(%s::json) AS _bulk' in only
    assert only.endswith("ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name WHERE t.val IS DISTINCT FROM EXCLUDED.val")
    assert only.startswith('INSERT INTO live.demo AS t (id, name, val) SELECT _bulk.c0, _bulk.c1, _bulk.c2 FROM')
    assert 'AS _bulk(c0 integer, c1 text, c2 double precision)' in only


def test_pages_respect_page_size_and_catalog_cached():
    cur = RecordingCursor()
    bulk.execute_values(cur, SQL, [(i, 'x', 1.0) for i in range(250)], page_size=100)
    bulk.execute_values(cur, SQL, [(1, 'x', 1.0)], page_size=100)
    catalog = [c for c in cur.calls if c[0] == bulk._CATALOG_SQL]
    assert len(catalog) == 1 and catalog[0][1] == ('live.demo',)
    sizes = [len(json.loads(p[0])) for s, p in cur.calls if s != bulk._CATALOG_SQL]
    assert sizes == [100, 100, 50, 1]


def test_stale_type_cache_is_refreshed_once():
    cur = RecordingCursor(types={'id': ('integer', 'int4')})
    bulk.execute_values(cur, "INSERT INTO live.demo (id) VALUES %s", [(1,)])
    cur.types = {'id': ('integer', 'int4'), 'name': ('text', 'text')}  # migration 加欄位
    bulk.execute_values(cur, "INSERT INTO live.demo (id, name) VALUES %s", [(1, 'a')])
    assert sum(1 for sql, _ in cur.calls if sql == bulk._CATALOG_SQL) == 2
    with pytest.raises(ValueError):
        bulk.execute_values(cur, "INSERT INTO live.demo (id, nope) VALUES %s", [(1, 'a')])


def test_empty_rows_noop():
    cur = RecordingCursor()
    assert bulk.execute_values(cur, SQL, []) is None
    assert bulk.execute_values(cur, SQL, [], fetch=True) == []
    assert cur.calls == []


def test_fetch_collects_every_page():
    cur = RecordingCursor(returning=[('r',)])
    out = bulk.execute_values(cur, SQL + ' RETURNING id', [(i, 'x', 1.0) for i in range(5)],
                              page_size=2, fetch=True)
    assert out == [('r',)] * 3


def test_template_elements():
    sql = "INSERT INTO live.demo (id, name, geom, tags, updated_at) VALUES %s ON CONFLICT DO NOTHING"
    tmpl = "(%s, %s, ST_SetSRID(ST_MakePoint(%s::float8, %s::float8), 4326), %s::text[], now())"
    fixed, spec = bulk.build_insert_sql(sql, TYPES, tmpl)
    assert ('SELECT _bulk.c0, _bulk.c1, '
            'ST_SetSRID(ST_MakePoint(_bulk.p0::float8, _bulk.p1::float8), 4326), '
            '_bulk.c3::text[], now() FROM') in fixed
    assert 'AS _bulk(c0 integer, c1 text, p0 text, p1 text, c3 text[])' in fixed
    assert [k for k, _ in spec] == ['c0', 'c1', 'p0', 'p1', 'c3']


def test_template_geomfromtext_passes_text():
    sql = "INSERT INTO live.demo (id, geom) VALUES %s"
    fixed, spec = bulk.build_insert_sql(sql, TYPES, '(%s,ST_GeomFromText(%s,4326))')
    assert 'ST_GeomFromText(_bulk.p0,4326)' in fixed
    assert spec == [('c0', 'int4'), ('p0', 'text')]


@pytest.mark.parametrize('sql', [
    "UPDATE live.demo SET x=1 FROM (VALUES %s) AS v(a)",
    "INSERT INTO live.demo (id) VALUES %s ON CONFLICT (id) DO UPDATE SET id=%s",
    "INSERT INTO live.demo (nope) VALUES %s",
])
def test_rejects_unsupported(sql):
    with pytest.raises(ValueError):
        bulk.build_insert_sql(sql, TYPES)


def test_template_arity_and_named_rejected():
    with pytest.raises(ValueError):
        bulk.build_insert_sql("INSERT INTO live.demo (id, name) VALUES %s", TYPES, '(%s)')
    with pytest.raises(ValueError):
        bulk.build_insert_sql("INSERT INTO live.demo (id) VALUES %s", TYPES, '(%(id)s)')


def test_value_conversion():
    spec = [('id', 'int4'), ('val', 'float8'), ('payload', 'jsonb'), ('tags', 'text'),
            ('updated_at', 'timestamptz'), ('name', 'text')]
    t = dt.datetime(2026, 11, 9, 12, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))
    rows = [
        (5.5, float('nan'), '{"a": 1}', ['x', 'y'], t, decimal.Decimal('1.10')),
        (-2.5, float('inf'), Json({'b': [1]}), None, dt.date(2026, 1, 2), uuid.UUID(int=1)),
        (3, -math.inf, {'c': None}, ('z',), None, b'\x01\xff'),
    ]
    out = json.loads(bulk.serialize_rows(rows, spec))
    assert out[0] == {'id': 6, 'val': 'NaN', 'payload': {'a': 1}, 'tags': ['x', 'y'],
                      'updated_at': '2026-11-09T12:00:00+08:00', 'name': '1.10'}
    assert out[1]['id'] == -3 and out[1]['val'] == 'Infinity' and out[1]['payload'] == {'b': [1]}
    assert out[1]['updated_at'] == '2026-01-02' and out[1]['name'] == str(uuid.UUID(int=1))
    assert out[2]['val'] == '-Infinity' and out[2]['payload'] == {'c': None} and out[2]['name'] == '\\x01ff'
    with pytest.raises(ValueError):
        bulk.serialize_rows([(1,)], spec)
    # json（非 jsonb）欄位以原文字串傳，內容與 psycopg2 Json 的輸出逐字相同
    out = json.loads(bulk.serialize_rows([(Json({'a': 1}),), ('{"b":2}',), (None,)], [('c0', 'json')]))
    assert [r['c0'] for r in out] == ['{"a": 1}', '{"b":2}', None]


def test_json_column_is_text_then_cast():
    types = {'id': ('integer', 'int4'), 'doc': ('json', 'json')}
    fixed, _ = bulk.build_insert_sql("INSERT INTO live.demo (id, doc) VALUES %s", types)
    assert 'SELECT _bulk.c0, _bulk.c1::json FROM' in fixed and 'c1 text' in fixed


def test_no_variable_shape_execute_values_in_runtime_code():
    """R5（必須）：常駐程式不可再用 psycopg2 的 execute_values（一次性 scripts/ 不在此限）。"""
    offenders = []
    for top in ('storage', 'collectors', 'workers', 'tasks', 'external', 'utils'):
        for path in (ROOT / top).rglob('*.py'):
            if path.name == 'bulk.py':
                continue
            text = path.read_text(encoding='utf-8')
            if 'psycopg2.extras import' in text and 'execute_values' in text.split('psycopg2.extras import', 1)[1].split('\n', 1)[0]:
                offenders.append(str(path.relative_to(ROOT)))
            if 'extras.execute_values' in text:
                offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []


def test_vm_copy_is_identical():
    assert (ROOT / 'storage/bulk.py').read_bytes() == (ROOT / 'external/vm_common/bulk.py').read_bytes()


# ---------------------------------------------------------------- integration (本機 PG)

DSN = os.environ.get('TEST_SUPABASE_DB_URL', '').strip()


def pg(fn):
    fn = pytest.mark.live_supabase(fn)
    return pytest.mark.skipif(not DSN, reason='TEST_SUPABASE_DB_URL 未設定')(fn)

DDL = """
DROP SCHEMA IF EXISTS bulk_t CASCADE;
CREATE SCHEMA bulk_t;
CREATE FUNCTION bulk_t.st_geomfromtext(text, int) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'SRID=' || $2 || ';' || $1 $$;
CREATE FUNCTION bulk_t.st_makepoint(float8, float8) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'POINT(' || $1 || ' ' || $2 || ')' $$;
CREATE FUNCTION bulk_t.st_setsrid(text, int) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'SRID=' || $2 || ';' || $1 $$;
CREATE TABLE bulk_t.{name} (
    id int PRIMARY KEY, small smallint, big bigint, val double precision, r real, num numeric(10,2),
    flag boolean, label varchar(8), body text, day date, ts timestamptz, tsn timestamp, gap interval,
    raw bytea, uid uuid, payload jsonb, payload_json json, tags text[], times timestamptz[], reals real[],
    geom text, made_at timestamptz DEFAULT '2000-01-01'
);
"""
COLS = ['id', 'small', 'big', 'val', 'r', 'num', 'flag', 'label', 'body', 'day', 'ts', 'tsn', 'gap',
        'raw', 'uid', 'payload', 'payload_json', 'tags', 'times', 'reals', 'geom']
TEMPLATE = '(' + ','.join(['%s'] * 18 + ['%s::timestamptz[]', '%s', 'ST_GeomFromText(%s,4326)']) + ')'


def _rows(n, offset=0):
    tz = dt.timezone(dt.timedelta(hours=8))
    out = []
    for i in range(offset, offset + n):
        out.append((
            i, i % 300, 2 ** 40 + i, (float('nan') if i % 7 == 0 else i / 3), i * 0.25,
            decimal.Decimal(f'{i}.125'), i % 2 == 0, f'中{i}'[:8], f"it's \"{i}\" \\ 中文",
            dt.date(2026, 1, 1) + dt.timedelta(days=i % 30),
            dt.datetime(2026, 11, 9, 1, 2, 3, i, tzinfo=tz), dt.datetime(2026, 11, 9, 4, 5, 6),
            dt.timedelta(days=1, seconds=i, microseconds=7), bytes([i % 256, 0, 255]), str(uuid.UUID(int=i)),
            (Json({'k': i, 's': '中'}) if i % 3 else json.dumps({'k': i})), Json({'k': i, 'z': [1, 2]}),
            [f't{i}', 'x,y', None], [dt.datetime(2026, 1, 1, tzinfo=tz)], [1.5, float('inf')],
            (None if i % 5 == 0 else f'POINT({120 + i / 1000} 23.5)'),
        ))
    return out


@pytest.fixture
def conn():
    import psycopg2
    c = psycopg2.connect(DSN)
    c.autocommit = True
    head, table = DDL.split('CREATE TABLE')
    with c.cursor() as cur:
        cur.execute(head)
        for name in ('old_t', 'new_t'):
            cur.execute('CREATE TABLE' + table.replace('{name}', name))
        cur.execute("SET search_path = bulk_t, public")
    yield c
    with c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS bulk_t CASCADE")
    c.close()


def _upsert(table):
    return (f"INSERT INTO bulk_t.{table} AS t ({','.join(COLS)}) VALUES %s "
            f"ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body || '!', payload = EXCLUDED.payload "
            f"WHERE t.val IS DISTINCT FROM EXCLUDED.val RETURNING id")


@pg
def test_new_equals_old_row_for_row(conn):
    from psycopg2.extras import execute_values as old_execute_values
    rows = _rows(40)
    again = _rows(40)[::3]  # 重送部分列 → 走 ON CONFLICT DO UPDATE ... WHERE
    with conn.cursor() as cur:
        r_old = old_execute_values(cur, _upsert('old_t'), rows, template=TEMPLATE, page_size=15, fetch=True)
        r_new = bulk.execute_values(cur, _upsert('new_t'), rows, template=TEMPLATE, page_size=15, fetch=True)
        assert r_old == r_new
        r_old = old_execute_values(cur, _upsert('old_t'), again, template=TEMPLATE, page_size=15, fetch=True)
        r_new = bulk.execute_values(cur, _upsert('new_t'), again, template=TEMPLATE, page_size=15, fetch=True)
        assert sorted(r_old) == sorted(r_new)
        cur.execute("SELECT count(*) FROM (SELECT o::text FROM bulk_t.old_t o EXCEPT SELECT n::text FROM bulk_t.new_t n) d")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM (SELECT n::text FROM bulk_t.new_t n EXCEPT SELECT o::text FROM bulk_t.old_t o) d")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM bulk_t.new_t")
        assert cur.fetchone()[0] == 40
        # 整數欄位收到 float 的四捨五入與 PG 一致；jsonb 收到 json.dumps 字串仍是物件
        cur.execute("SELECT jsonb_typeof(payload) FROM bulk_t.new_t WHERE id = 0")
        assert cur.fetchone()[0] == 'object'


@pg
def test_int_rounding_matches_pg(conn):
    from psycopg2.extras import execute_values as old_execute_values
    sql = "INSERT INTO bulk_t.{t} (id, small, big) VALUES %s"
    rows = [(1, 2.5, -2.5), (2, 0.49999999999999994, 1e3), (3, -0.5, 7.0)]
    with conn.cursor() as cur:
        old_execute_values(cur, sql.format(t='old_t'), rows)
        bulk.execute_values(cur, sql.format(t='new_t'), rows)
        cur.execute("SELECT id, small, big FROM bulk_t.old_t ORDER BY id")
        a = cur.fetchall()
        cur.execute("SELECT id, small, big FROM bulk_t.new_t ORDER BY id")
        assert a == cur.fetchall()


@pg
def test_pg_stat_statements_single_entry(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
        if not cur.fetchone():
            pytest.skip('pg_stat_statements 未安裝')
        uniq = 'n' + uuid.uuid4().hex[:12]
        cur.execute(f"CREATE TABLE bulk_t.{uniq} (LIKE bulk_t.new_t INCLUDING ALL)")
        offset = 0
        for n in (1, 7, 500, 1000, 1500):
            bulk.execute_values(cur, _upsert(uniq), _rows(n, offset), template=TEMPLATE, page_size=1000)
            offset += n
        cur.execute("SELECT count(*), sum(calls) FROM pg_stat_statements "
                    "WHERE query ILIKE %s", (f'INSERT INTO bulk_t.{uniq} %',))
        entries, calls = cur.fetchone()
        assert entries == 1
        assert calls == 6  # 1 + 1 + 1 + 1 + 2 頁
