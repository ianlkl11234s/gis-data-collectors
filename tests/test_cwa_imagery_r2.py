"""CWA Imagery R2 雙寫（AR-11 read-path-cdn）單元測試。

不打真實 R2 / DB。驗證重點：
1. R2 object key 生成 — observed_at → UTC 轉換、ext 由 mime 判定
2. 雙寫 best-effort — R2 拋錯 / 未設定 → image_key=None，但 frame 仍照常
   進 result['data']（含 image_b64），確保 DB 照寫、不因 CDN 壞掉丟資料
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from collectors.cwa_satellite import (
    CWASatelliteCollector,
    imagery_r2_key,
    _ext_from_mime,
)

TAIPEI_TZ = timezone(timedelta(hours=8))


# ── 1. Key 生成 ─────────────────────────────────────────────

def test_key_utc_conversion_from_aware_datetime():
    """+08:00 16:10 → UTC 08:10 → HHMMSS 081000，日期同步回退到 UTC 當日。"""
    obs = datetime(2026, 4, 7, 16, 10, 0, tzinfo=TAIPEI_TZ)
    key = imagery_r2_key('O-A0058-005', obs, 'image/png')
    assert key == 'imagery/cwa/O-A0058-005/20260407/081000.png'


def test_key_utc_crosses_date_boundary():
    """+08:00 05:30 → 前一天 UTC 21:30，YYYYMMDD 要退一天。"""
    obs = datetime(2026, 4, 7, 5, 30, 0, tzinfo=TAIPEI_TZ)
    key = imagery_r2_key('O-C0042-004', obs, 'image/jpeg')
    assert key == 'imagery/cwa/O-C0042-004/20260406/213000.jpg'


def test_key_from_iso_string():
    """observed_at 也可為 ISO 字串（backfill / JSON 路徑）。"""
    key = imagery_r2_key('O-A0058-005', '2026-04-07T16:10:00+08:00', 'image/png')
    assert key == 'imagery/cwa/O-A0058-005/20260407/081000.png'


def test_key_naive_datetime_treated_as_utc():
    obs = datetime(2026, 4, 7, 8, 10, 0)  # naive → 視為 UTC
    key = imagery_r2_key('O-A0058-005', obs, 'image/png')
    assert key == 'imagery/cwa/O-A0058-005/20260407/081000.png'


def test_ext_from_mime():
    assert _ext_from_mime('image/png') == 'png'
    assert _ext_from_mime('image/jpeg') == 'jpg'
    assert _ext_from_mime('IMAGE/PNG') == 'png'  # 大小寫容錯
    assert _ext_from_mime('application/octet-stream') == 'bin'
    assert _ext_from_mime(None) == 'bin'


def test_key_ext_matches_mime():
    obs = datetime(2026, 4, 7, 16, 10, 0, tzinfo=TAIPEI_TZ)
    assert imagery_r2_key('D', obs, 'image/png').endswith('.png')
    assert imagery_r2_key('D', obs, 'image/jpeg').endswith('.jpg')


# ── 2. 雙寫 best-effort 行為 ────────────────────────────────

@pytest.fixture
def collector(monkeypatch):
    """建構 collector，R2 預設停用（get_r2_storage → None），單一資料集。"""
    monkeypatch.setattr('config.CWA_API_KEY', 'test-key')
    monkeypatch.setattr('collectors.cwa_satellite.get_r2_storage', lambda: None)
    c = CWASatelliteCollector()
    c.datasets = ['O-A0058-005']

    frame = {
        'dataset_id': 'O-A0058-005',
        'observed_at': datetime(2026, 4, 7, 16, 10, 0, tzinfo=TAIPEI_TZ),
        'product_url': 'https://example/radar.png',
        'mime_type': 'image/png',
        'lon_min': 115.0, 'lon_max': 126.5,
        'lat_min': 17.75, 'lat_max': 29.25,
        'width': 3600, 'height': 3600,
        'resource_desc': 'radar',
    }
    # 攔掉所有網路：metadata / extract / download 都回固定值
    monkeypatch.setattr(c, '_fetch_metadata', lambda ds: {})
    monkeypatch.setattr(c, '_extract_frame', lambda ds, payload: dict(frame))
    monkeypatch.setattr(c, '_download_png', lambda url: b'FAKE-PNG-BYTES')
    return c


def test_double_write_r2_error_keeps_db_write(collector):
    """R2 上傳拋錯 → image_key=None，但 frame 仍進 data（含 image_b64）→ DB 照寫。"""
    mock_r2 = MagicMock()
    mock_r2.upload_image.side_effect = RuntimeError("boom")
    collector._r2 = mock_r2

    result = collector.collect()

    assert result['frame_count'] == 1
    f = result['data'][0]
    assert f['image_key'] is None          # 雙寫失敗
    assert f['image_b64']                  # DB 照寫（bytes 仍在）
    assert f['image_size'] == len(b'FAKE-PNG-BYTES')
    mock_r2.upload_image.assert_called_once()


def test_double_write_r2_disabled(collector):
    """R2 未設定（_r2 is None）→ image_key=None，frame 照常。"""
    collector._r2 = None
    result = collector.collect()
    f = result['data'][0]
    assert f['image_key'] is None
    assert f['image_b64']


def test_double_write_r2_success(collector):
    """R2 上傳成功 → image_key = 規約 key，且帶 mime 作為 content_type。"""
    mock_r2 = MagicMock()
    collector._r2 = mock_r2

    result = collector.collect()

    f = result['data'][0]
    assert f['image_key'] == 'imagery/cwa/O-A0058-005/20260407/081000.png'
    args, kwargs = mock_r2.upload_image.call_args
    # upload_image(key, data, content_type)
    assert args[0] == 'imagery/cwa/O-A0058-005/20260407/081000.png'
    assert args[1] == b'FAKE-PNG-BYTES'
    assert args[2] == 'image/png'
