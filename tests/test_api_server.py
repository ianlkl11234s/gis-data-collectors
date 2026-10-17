"""api/server.py 存取控制測試

背景（2026-10-17）：API key 外洩於 PUBLIC repo，且 /api/download/<collector>/<path>
不限制 collector → 持有 key 可讀 S3 主 bucket 任意物件（supabase-snapshots/、
private-research/…），collector='..' 還能讀資料目錄外的本地檔。
本檔驗證：只接受已註冊 collector、拒絕路徑遍歷、只接受 header 帶 key。
不打真網路：S3 以假物件取代。
"""

import pytest

import config
from api import server


KEY = 'test-key-not-secret'


class FakeS3:
    def __init__(self):
        self.requested = []

    def get_file(self, key):
        self.requested.append(key)
        return b'{"ok": true}'

    def extract_file_from_archive(self, collector, date, filename):
        self.requested.append(f'archive:{collector}/{date}/{filename}')
        return None

    def list_dates(self, collector):
        self.requested.append(f'dates:{collector}')
        return ['2026-10-01']

    def list_archive_files(self, collector, date):
        return []

    def list_files_by_date(self, collector, date):
        return []


@pytest.fixture
def client(monkeypatch, tmp_path):
    monkeypatch.setattr(config, 'API_KEY', KEY)
    monkeypatch.setattr(config, 'LOCAL_DATA_DIR', tmp_path)
    fake = FakeS3()
    monkeypatch.setattr(server, 'get_s3_storage', lambda: fake)
    app = server.create_app()
    app.testing = True
    c = app.test_client()
    c.fake_s3 = fake
    return c


H = {'X-API-Key': KEY}


def test_registered_collector_download_reaches_s3(client):
    r = client.get('/api/download/flight_fr24/2026/03/01/a/b.json', headers=H)
    assert r.status_code == 200
    assert client.fake_s3.requested == ['flight_fr24/2026/03/01/a/b.json']


@pytest.mark.parametrize('collector', [
    'supabase-snapshots', 'private-research', 'deploy-assets', 'mini-taipei',
    'archives', 'not_a_collector', '%2e%2e', '..', '.',
])
def test_unknown_or_denied_collector_rejected(client, collector):
    r = client.get(f'/api/download/{collector}/2026/01/01/x.json', headers=H)
    assert r.status_code == 404
    assert client.fake_s3.requested == []


@pytest.mark.parametrize('path', [
    '/api/data/%2e%2e',
    '/api/data/%2e%2e/latest',
    '/api/data/%2e%2e/dates',
    '/api/data/supabase-snapshots/2026-01-01',
    '/api/data/private-research/dates',
])
def test_other_collector_endpoints_reject_unknown(client, path):
    r = client.get(path, headers=H)
    assert r.status_code == 404
    assert client.fake_s3.requested == []


@pytest.mark.parametrize('filepath', [
    '2026/%2e%2e/%2e%2e/secret.json',
    '2026/./x.json',
    '2026//x.json',
    '2026/a%5Cb.json',
])
def test_unsafe_filepath_rejected(client, filepath):
    r = client.get(f'/api/download/youbike/{filepath}', headers=H)
    assert r.status_code == 400
    assert client.fake_s3.requested == []


def test_query_string_key_not_accepted(client):
    r = client.get(f'/api/collectors?api_key={KEY}')
    assert r.status_code == 401


def test_wrong_key_rejected(client):
    r = client.get('/api/collectors', headers={'X-API-Key': 'wrong'})
    assert r.status_code == 403


def test_header_key_accepted(client):
    r = client.get('/api/collectors', headers=H)
    assert r.status_code == 200


def test_dates_for_registered_collector(client):
    r = client.get('/api/data/flight_opensky/dates', headers=H)
    assert r.status_code == 200
    assert client.fake_s3.requested == ['dates:flight_opensky']


def test_known_collectors_include_api_callers():
    known = server.get_known_collectors()
    for name in ('youbike', 'flight_fr24', 'flight_fr24_zone', 'flight_opensky'):
        assert name in known
    assert not (known & server.DENIED_PREFIXES)
