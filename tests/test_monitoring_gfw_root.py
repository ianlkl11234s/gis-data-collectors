from __future__ import annotations

import io
import json

import pytest

from tasks import monitoring


def _root(release_id: str = "2026-08-21") -> dict:
    return {
        "release_id": release_id,
        "release_path": f"releases/{release_id}",
        "published_releases": [{"release_id": release_id}],
    }


def test_parse_gfw_root_manifest_accepts_only_active_reader_release():
    assert monitoring.parse_gfw_hourly_root_manifest(_root()) == "2026-08-21"

    with pytest.raises(ValueError, match="release_path"):
        monitoring.parse_gfw_hourly_root_manifest({
            **_root(), "release_path": "releases/2026-08-20",
        })
    with pytest.raises(ValueError, match="attest"):
        monitoring.parse_gfw_hourly_root_manifest({
            **_root(), "published_releases": [{"release_id": "2026-08-20"}],
        })


def test_read_gfw_root_never_treats_invalid_root_as_fresh(monkeypatch):
    class _S3:
        def __init__(self, body: bytes):
            self.s3 = self
            self.body = body
            self.calls = []

        def get_object(self, **kwargs):
            self.calls.append(kwargs)
            return {"Body": io.BytesIO(self.body)}

    monkeypatch.setattr(monitoring.config, "S3_BUCKET", "gfw-release-test")
    good = _S3(json.dumps(_root()).encode())
    assert monitoring.read_gfw_hourly_root_release(good) == "2026-08-21"
    assert good.calls == [{
        "Bucket": "gfw-release-test",
        "Key": "deploy-assets/global-maritime/gfw-hourly/manifest.json",
    }]

    bad = _S3(b"{}")
    assert monitoring.read_gfw_hourly_root_release(bad) is None
