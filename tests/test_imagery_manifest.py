"""影像 R2 每日 manifest（storage/imagery_manifest.py）＋三支影像 collector 接線。

不打真實 R2 / DB：用記憶體假 R2（get_json / put_json / upload_image）。
"""
from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone

import pytest

from storage.imagery_manifest import (
    MANIFEST_CACHE_CONTROL,
    frame_entry,
    manifest_key,
    merge_frames,
    publish_frames,
)

TAIPEI_TZ = timezone(timedelta(hours=8))


class FakeR2:
    def __init__(self, fail_put=False):
        self.objects: dict[str, object] = {}
        self.cache: dict[str, str] = {}
        self.images: dict[str, bytes] = {}
        self.puts = 0
        self.fail_put = fail_put

    def get_json(self, key):
        return self.objects.get(key)

    def put_json(self, key, payload, cache_control):
        if self.fail_put:
            raise RuntimeError('boom')
        self.puts += 1
        self.objects[key] = payload
        self.cache[key] = cache_control

    def upload_image(self, key, data, content_type, cache_control=None):
        self.images[key] = data


def _e(product, obs, key='k', size=10):
    return frame_entry(product, obs, key, 'image/png', size, (120, 21.9, 122, 25.3))


# ── 純函式 ───────────────────────────────────────────────

def test_frame_entry_normalises_time_to_utc_z():
    e = _e('AQI', datetime(2026, 11, 7, 16, 0, tzinfo=TAIPEI_TZ))
    assert e['observed_at'] == '2026-11-07T08:00:00Z'
    assert e['bbox'] == [120.0, 21.9, 122.0, 25.3]


def test_frame_entry_accepts_iso_string_and_naive():
    assert _e('a', '2026-11-07T08:00:00+00:00')['observed_at'] == '2026-11-07T08:00:00Z'
    assert _e('a', datetime(2026, 11, 7, 8, 0))['observed_at'] == '2026-11-07T08:00:00Z'


def test_merge_dedupes_same_product_and_time():
    a = _e('AQI', '2026-11-07T08:00:00Z')
    frames, changed = merge_frames([a], [dict(a)])
    assert frames == [a] and changed is False
    b = _e('AQI', '2026-11-07T09:00:00Z')
    frames, changed = merge_frames([a], [b])
    assert [f['observed_at'] for f in frames] == ['2026-11-07T08:00:00Z', '2026-11-07T09:00:00Z']
    assert changed is True


def test_merge_sorts_by_product_then_time():
    frames, _ = merge_frames([], [_e('b', '2026-11-07T02:00:00Z'), _e('a', '2026-11-07T03:00:00Z'),
                                  _e('a', '2026-11-07T01:00:00Z')])
    assert [(f['product'], f['observed_at'][11:13]) for f in frames] == [('a', '01'), ('a', '03'), ('b', '02')]


# ── publish_frames ──────────────────────────────────────

def test_publish_groups_by_utc_day_and_writes_latest():
    r2 = FakeR2()
    # 台北 11/8 07:00 = UTC 11/7 23:00；台北 11/8 09:00 = UTC 11/8 01:00
    ok = publish_frames(r2, 'aqi', [
        _e('AQI', datetime(2026, 11, 8, 7, 0, tzinfo=TAIPEI_TZ), key='k1'),
        _e('AQI', datetime(2026, 11, 8, 9, 0, tzinfo=TAIPEI_TZ), key='k2'),
    ])
    assert ok is True
    d7 = r2.objects[manifest_key('aqi', '2026-11-07')]
    d8 = r2.objects[manifest_key('aqi', '2026-11-08')]
    assert [f['key'] for f in d7['frames']] == ['k1']
    assert [f['key'] for f in d8['frames']] == ['k2']
    assert d8['version'] == 1 and d8['source'] == 'aqi' and d8['date'] == '2026-11-08'
    assert d8['date_basis'] == 'UTC'
    assert r2.cache[manifest_key('aqi', '2026-11-08')] == MANIFEST_CACHE_CONTROL
    latest = r2.objects['imagery/aqi/latest.json']
    assert latest['products']['AQI']['key'] == 'k2'


def test_publish_skips_write_when_unchanged():
    r2 = FakeR2()
    e = _e('AQI', '2026-11-07T08:00:00Z')
    publish_frames(r2, 'aqi', [e])
    puts = r2.puts
    publish_frames(r2, 'aqi', [dict(e)])
    assert r2.puts == puts


def test_latest_does_not_go_backwards_on_backfill():
    r2 = FakeR2()
    publish_frames(r2, 'cwa', [_e('X', '2026-11-07T08:00:00Z', key='new')])
    publish_frames(r2, 'cwa', [_e('X', '2026-11-01T08:00:00Z', key='old')])
    assert r2.objects['imagery/cwa/latest.json']['products']['X']['key'] == 'new'


def test_publish_failure_is_best_effort():
    r2 = FakeR2(fail_put=True)
    assert publish_frames(r2, 'aqi', [_e('AQI', '2026-11-07T08:00:00Z')]) is False


def test_publish_noop_without_r2_or_entries():
    assert publish_frames(None, 'aqi', [_e('AQI', '2026-11-07T08:00:00Z')]) is True
    assert publish_frames(FakeR2(), 'aqi', []) is True


# ── collector 接線 ──────────────────────────────────────

def test_precip_key_and_bbox():
    from collectors.precipitation_raster import frame_bbox, imagery_r2_key
    assert imagery_r2_key(3, '2026-11-07T16:00:00+08:00') == 'imagery/precip/ch3/20261107/080000.png'
    assert imagery_r2_key(24, datetime(2026, 11, 7, 8, 0)) == 'imagery/precip/ch24/20261107/080000.png'
    assert frame_bbox({'ul_lat': 25.34, 'ul_lng': 119.98, 'br_lat': 21.86, 'br_lng': 122.04}) == (
        119.98, 21.86, 122.04, 25.34)
    assert frame_bbox({'ul_lat': None, 'ul_lng': 1, 'br_lat': 1, 'br_lng': 1}) is None


def _precip_frame(ch, empty=False):
    png = b'\x89PNG' + b'0' * 200
    return {
        'cumulative_hours': ch, 'observed_at': '2026-11-07T08:00:00+00:00',
        'image_bytes_b64': None if empty else base64.b64encode(png).decode(),
        'image_size': 0 if empty else len(png), 'is_empty': empty,
        'ul_lat': 25.34, 'ul_lng': 119.98, 'br_lat': 21.86, 'br_lng': 122.04,
        'width_m': 426, 'height_m': 773, 'source_url': 'u',
    }


def test_precip_collect_uploads_and_publishes(monkeypatch):
    import collectors.precipitation_raster as mod
    r2 = FakeR2()
    monkeypatch.setattr(mod, 'get_r2_storage', lambda: r2)
    c = mod.PrecipitationRasterCollector()
    c.ch_list = [1, 24]
    monkeypatch.setattr(c, '_fetch_raster', lambda ch: _precip_frame(ch, empty=(ch == 24)))
    result = c.collect()
    assert set(r2.images) == {'imagery/precip/ch1/20261107/080000.png'}
    frames = r2.objects['imagery/precip/manifest-2026-11-07.json']['frames']
    assert [f['product'] for f in frames] == ['ch1']
    assert frames[0]['bbox'] == [119.98, 21.86, 122.04, 25.34]
    # 空圖不上傳、仍照常進 data（DB 照寫 metadata）
    assert [f['cumulative_hours'] for f in result['data']] == [1, 24]
    assert result['data'][1]['image_key'] is None


def test_precip_collect_without_r2_still_returns_frames(monkeypatch):
    import collectors.precipitation_raster as mod
    monkeypatch.setattr(mod, 'get_r2_storage', lambda: None)
    c = mod.PrecipitationRasterCollector()
    c.ch_list = [1]
    monkeypatch.setattr(c, '_fetch_raster', lambda ch: _precip_frame(ch))
    result = c.collect()
    assert result['data'][0]['image_key'] is None
    assert result['data'][0]['image_bytes_b64']


def test_precip_transform_does_not_send_image_key():
    """DB 表沒有 image_key 欄；transformer 不可把它送進 INSERT。"""
    from storage.supabase_writer import SupabaseWriter
    frame = _precip_frame(1)
    frame['image_key'] = 'imagery/precip/ch1/20261107/080000.png'
    rows = SupabaseWriter._transform_precipitation_raster(None, {'data': [frame]}, datetime.now(timezone.utc))
    assert 'image_key' not in rows[0]


def test_aqi_collect_publishes_manifest(monkeypatch):
    import collectors.air_quality_imagery as mod
    r2 = FakeR2()
    monkeypatch.setattr(mod, 'get_r2_storage', lambda: r2)
    c = mod.AirQualityImageryCollector()
    c.products = ['AQI', 'PM25']
    target = datetime(2026, 11, 7, 16, 0, tzinfo=TAIPEI_TZ)
    monkeypatch.setattr(c, '_find_latest_hour', lambda now, max_lookback=3: target)
    monkeypatch.setattr(c, '_fetch_png', lambda url: (b'PNG', ''))
    c.collect()
    frames = r2.objects['imagery/aqi/manifest-2026-11-07.json']['frames']
    assert [(f['product'], f['key']) for f in frames] == [
        ('AQI', 'imagery/aqi/AQI/20261107/080000.png'),
        ('PM25', 'imagery/aqi/PM25/20261107/080000.png'),
    ]
    assert frames[0]['bbox'] == [120.0, 21.9, 122.0, 25.3]


def test_cwa_collect_publishes_manifest(monkeypatch):
    import collectors.cwa_satellite as mod
    monkeypatch.setattr(mod.config, 'CWA_API_KEY', 'test', raising=False)
    r2 = FakeR2()
    monkeypatch.setattr(mod, 'get_r2_storage', lambda: r2)
    c = mod.CWASatelliteCollector()
    c.datasets = ['O-A0058-005']
    obs = datetime(2026, 11, 7, 20, 10, tzinfo=TAIPEI_TZ)
    monkeypatch.setattr(c, '_fetch_metadata', lambda ds: {})
    monkeypatch.setattr(c, '_extract_frame', lambda ds, payload: {
        'dataset_id': ds, 'observed_at': obs, 'product_url': 'x.png', 'mime_type': 'image/png',
        'lon_min': 115.0, 'lon_max': 126.5, 'lat_min': 17.75, 'lat_max': 29.25,
    })
    monkeypatch.setattr(c, '_download_png', lambda url: b'PNG')
    c.collect()
    frames = r2.objects['imagery/cwa/manifest-2026-11-07.json']['frames']
    assert frames[0]['key'] == 'imagery/cwa/O-A0058-005/20261107/121000.png'
    assert frames[0]['observed_at'] == '2026-11-07T12:10:00Z'
    assert frames[0]['bbox'] == [115.0, 17.75, 126.5, 29.25]
