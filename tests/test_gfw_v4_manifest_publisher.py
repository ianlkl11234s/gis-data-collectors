from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import pytest

from tasks.gfw_v4_manifest_publisher import (
    RELEASE_CACHE_CONTROL,
    ROOT_CACHE_CONTROL,
    ROOT_CONTENT_TYPE,
    ROOT_MANIFEST_KEY,
    V4ManifestPublishError,
    V4ManifestPublisher,
    validate_v4_release_candidate,
)


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.calls: list[tuple[str, str]] = []

    def put_object(self, **kwargs):
        body = bytes(kwargs["Body"])
        sha = hashlib.sha256(body).hexdigest()
        self.objects[kwargs["Key"]] = (
            body,
            {
                "ContentLength": str(len(body)),
                "ETag": f'"{sha}"',
                "ContentType": kwargs["ContentType"],
                "ContentEncoding": kwargs.get("ContentEncoding", "identity"),
                "CacheControl": kwargs["CacheControl"],
            },
        )
        self.calls.append(("put", kwargs["Key"]))

    def head_object(self, *, Bucket: str, Key: str):
        del Bucket
        self.calls.append(("head", Key))
        return self.objects[Key][1]

    def get_object(self, *, Bucket: str, Key: str):
        del Bucket
        body, _ = self.objects[Key]
        return {"Body": io.BytesIO(body)}

    def delete_object(self, *, Bucket: str, Key: str):
        del Bucket
        self.calls.append(("delete", Key))
        self.objects.pop(Key, None)


class FakeLedger:
    def __init__(self) -> None:
        self.payloads: list[dict] = []

    def write(self, payload: dict) -> None:
        self.payloads.append(payload)


def _make_release(tmp_path: Path, day: str = "2026-08-21") -> Path:
    release_id = f"{day}__public-global-presence-v4.0"
    root = tmp_path / "candidate"
    release_dir = root / "releases" / release_id
    assets = []
    types = (
        "tracks_day_pmtiles", "track_frame_pmtiles", "track_detail_bucket",
        "grid_hour_pmtiles", "grid_detail_bucket", "fishing_effort_day",
    )
    for index, asset_type in enumerate(types):
        path = f"releases/{release_id}/data/{index}.bin"
        local = root / path
        local.parent.mkdir(parents=True, exist_ok=True)
        body = f"asset-{day}-{index}".encode()
        local.write_bytes(body)
        sha = hashlib.sha256(body).hexdigest()
        semantic_counts = {"count": index}
        asset: dict[str, object] = {
            "path": path, "type": asset_type, "bytes": len(body),
            "content_length": len(body), "sha256": sha, "etag": f'"{sha}"',
            "content_type": "application/octet-stream", "content_encoding": "identity",
            "cache_control": RELEASE_CACHE_CONTROL, "semantic_counts": semantic_counts,
        }
        if asset_type == "track_frame_pmtiles":
            semantic_counts.update({"observed_at": f"{day}T00:00:00Z", "bucket": "FISHING", "feature_count": 1})
            asset["spatial_contract"] = {
                "fixed_zoom": 6, "source_feature_count": 1, "decoded_feature_count": 1,
                "identity_duplicate_count": 0, "identity_missing_count": 0,
            }
        assets.append(asset)
    manifest = {
        "schema_version": 4, "required_top_level_fields": [
            "schema_version", "release_id", "selected_utc_date", "bbox",
            "source_dataset_id", "resolved_dataset_version", "days", "grid",
            "tracks", "fishing_effort", "layer_separation", "artifacts", "release_truth",
        ],
        "release_id": release_id, "selected_utc_date": day,
        "bbox": [115.9, 20.3, 134.7, 36.5], "source_dataset_id": "public-global-presence",
        "resolved_dataset_version": "public-global-presence:v4.0", "days": [day],
        "grid": {}, "fishing_effort": {},
        "layer_separation": {
            "grid": "gfwHourlyGrid", "tracks": "gfwHourlyTracks",
            "fishing_effort": "gfwFishingEffort", "dark_vessels": "gfwDarkVessels",
        },
        "taxonomy": {
            "tanker": "quarantine", "carrier": "independent_default_off",
            "gear_fad": "independent_non_vessel_observation",
        },
        "cache_contract": {"root": ROOT_CACHE_CONTROL, "release": RELEASE_CACHE_CONTROL},
        "tracks": {
            "buckets": ["FISHING", "CARGO", "PASSENGER", "CARRIER", "OTHER", "UNKNOWN"],
            "default_buckets": ["FISHING", "CARGO", "PASSENGER"],
        },
        "artifacts": assets,
        "release_truth": {
            "tier1_status": "passed", "tier2_status": "passed", "readback_status": "passed",
            "production_cutover": "passed", "tier2_evidence_id": "tier2-ok",
        },
        "production_cutover": "passed",
    }
    release_dir.mkdir(parents=True, exist_ok=True)
    (release_dir / "manifest.json").write_bytes(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode())
    release_body = (release_dir / "manifest.json").read_bytes()
    root_manifest = {
        "schema_version": 4, "immutable_release_contract": True, "release_id": release_id,
        "selected_utc_date": day,
        "release_manifest": {
            "path": f"releases/{release_id}/manifest.json", "bytes": len(release_body),
            "sha256": hashlib.sha256(release_body).hexdigest(),
        },
        "production_cutover": "passed",
    }
    (root / "manifest.json").write_bytes(json.dumps(root_manifest, sort_keys=True, separators=(",", ":")).encode())
    return root


def test_preflight_blocks_before_any_client_call(tmp_path: Path) -> None:
    root = _make_release(tmp_path)
    manifest = json.loads((root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json").read_text())
    manifest["release_truth"]["tier2_status"] = "not_run"
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    release_path.write_text(json.dumps(manifest), encoding="utf-8")
    fake = FakeS3()
    with pytest.raises(V4ManifestPublishError, match="Tier 1, Tier 2"):
        V4ManifestPublisher(fake, FakeLedger(), "bucket", root, "tier2-ok").publish()
    assert fake.calls == []


def test_publish_uploads_assets_then_release_then_root_and_writes_v4_ledger(tmp_path: Path) -> None:
    root = _make_release(tmp_path)
    s3, ledger = FakeS3(), FakeLedger()
    result = V4ManifestPublisher(s3, ledger, "bucket", root, "tier2-ok", run_id="00000000-0000-4000-8000-000000000001").publish()
    put_keys = [key for operation, key in s3.calls if operation == "put"]
    assert put_keys[-2:] == [
        "deploy-assets/global-maritime/gfw-hourly/v4/releases/2026-08-21__public-global-presence-v4.0/manifest.json",
        ROOT_MANIFEST_KEY,
    ]
    assert put_keys[-1] == ROOT_MANIFEST_KEY
    assert result["root_manifest_etag"] == f'"{result["root_manifest_sha256"]}"'
    assert [payload["status"] for payload in ledger.payloads] == ["running", "succeeded"]
    succeeded = ledger.payloads[-1]
    assert succeeded["manifest_schema_version"] == 4
    assert succeeded["release_prefix"].endswith("/releases/2026-08-21__public-global-presence-v4.0/")
    assert succeeded["root_manifest_content_type"] == ROOT_CONTENT_TYPE
    assert succeeded["root_manifest_content_encoding"] == "identity"
    assert succeeded["tier2_summary"] == {"evidence_id": "tier2-ok"}


def test_candidate_rejects_full_s3_path_and_parent_traversal(tmp_path: Path) -> None:
    root = _make_release(tmp_path)
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    manifest = json.loads(release_path.read_text())
    manifest["artifacts"][0]["path"] = "deploy-assets/global-maritime/gfw-hourly/v4/../bad"
    release_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(V4ManifestPublishError, match="unsafe"):
        validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")


def _refresh_root_release_pointer(root: Path) -> None:
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    body = release_path.read_bytes()
    root_path = root / "manifest.json"
    manifest = json.loads(root_path.read_text())
    manifest["release_manifest"] = {
        "path": "releases/2026-08-21__public-global-presence-v4.0/manifest.json",
        "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
    }
    root_path.write_text(json.dumps(manifest), encoding="utf-8")


def test_candidate_rejects_legacy_or_invalid_spatial_track_frame(tmp_path: Path) -> None:
    root = _make_release(tmp_path)
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    manifest = json.loads(release_path.read_text())
    frame = next(asset for asset in manifest["artifacts"] if asset["type"] == "track_frame_pmtiles")
    frame["type"] = "track_frame_hour"
    release_path.write_text(json.dumps(manifest), encoding="utf-8")
    _refresh_root_release_pointer(root)
    with pytest.raises(V4ManifestPublishError, match="unsupported"):
        validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")

    frame["type"] = "track_frame_pmtiles"
    frame["spatial_contract"]["fixed_zoom"] = 7
    release_path.write_text(json.dumps(manifest), encoding="utf-8")
    _refresh_root_release_pointer(root)
    with pytest.raises(V4ManifestPublishError, match="fixed z6"):
        validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")


def test_rollback_uses_only_enumerated_release_and_does_not_delete(tmp_path: Path) -> None:
    root = _make_release(tmp_path)
    s3, ledger = FakeS3(), FakeLedger()
    publisher = V4ManifestPublisher(
        s3, ledger, "bucket", root, "tier2-ok",
        run_id="00000000-0000-4000-8000-000000000001",
    )
    publisher.publish()
    current = json.loads(s3.objects[ROOT_MANIFEST_KEY][0])
    delete_count = len([call for call in s3.calls if call[0] == "delete"])
    result = publisher.rollback(current_root_manifest=current, target_release_id=current["release_id"])
    assert result["target_release_id"] == current["release_id"]
    assert len([call for call in s3.calls if call[0] == "delete"]) == delete_count
