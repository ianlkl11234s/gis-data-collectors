"""npa_traffic_accident_a1：resource 網址由 data.gov.tw 解析（上游會換 resource id）。"""
from unittest.mock import MagicMock

import requests

from collectors import npa_traffic_accident_a1 as a1

NEW_URL = (
    "https://opdadm.moi.gov.tw/api/v1/no-auth/resource/api/dataset/"
    "F4077949-50CC-4640-8114-79958CC8BBEA/resource/NEWID/download"
)


def _meta(*dist):
    return {"success": True, "result": {"distribution": list(dist)}}


def test_pick_download_url_prefers_json_opdadm():
    meta = _meta(
        {"resourceFormat": "CSV", "resourceDownloadUrl": "https://opdadm.moi.gov.tw/x.csv"},
        {"resourceFormat": "JSON", "resourceDownloadUrl": "https://evil.example/x.json"},
        {"resourceFormat": "json", "resourceDownloadUrl": NEW_URL},
    )
    assert a1._pick_download_url(meta) == NEW_URL


def test_pick_download_url_none_when_missing():
    assert a1._pick_download_url({}) is None
    assert a1._pick_download_url(_meta()) is None


def _collector_with(get):
    c = a1.NpaTrafficAccidentA1Collector.__new__(a1.NpaTrafficAccidentA1Collector)
    c.name = "npa_traffic_accident_a1"
    c._session = MagicMock()
    c._session.get.side_effect = get
    return c


def test_resolve_url_uses_data_gov_tw():
    resp = MagicMock()
    resp.json.return_value = _meta({"resourceFormat": "JSON", "resourceDownloadUrl": NEW_URL})
    c = _collector_with(lambda *a, **k: resp)
    assert c._resolve_url() == NEW_URL


def test_resolve_url_falls_back_on_error():
    def boom(*a, **k):
        raise requests.ConnectionError("down")
    c = _collector_with(boom)
    assert c._resolve_url() == a1.A1_URL
    assert "A3C4A73F" not in a1.A1_URL  # 舊 resource 已凍結，不可再當退路
