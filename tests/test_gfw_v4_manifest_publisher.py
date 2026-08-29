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
    TIER2_BINDING_ALGORITHM,
    V4ManifestPublishError,
    V4ManifestPublisher,
    tier2_binding_failure,
    tier2_core_digest,
    validate_v4_release_candidate,
)


def bind_tier2_evidence(manifest: dict, *, evidence_id: str) -> dict:
    """Attach Tier 2 evidence bound to this manifest's own core digest."""
    digest = tier2_core_digest(manifest)
    manifest["tier2_evidence_id"] = evidence_id
    manifest["tier2_evidence"] = {
        "schema_version": 1, "kind": "gfw_v4_tier2_browser_evidence",
        "evidence_id": evidence_id, "release_manifest": {"core_sha256": digest},
    }
    manifest["tier2_binding"] = {
        "algorithm": TIER2_BINDING_ALGORITHM, "core_sha256": digest,
    }
    return manifest


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
    # A release may only claim Tier 2 passed while carrying evidence bound to its
    # own core digest, so a realistic candidate fixture must be self-bound.
    bind_tier2_evidence(manifest, evidence_id="tier2-ok")
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


def _write_release(root: Path, manifest: dict) -> None:
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    release_path.write_text(json.dumps(manifest), encoding="utf-8")
    _refresh_root_release_pointer(root)


def test_tier2_passed_requires_evidence_bound_to_this_manifest(tmp_path: Path) -> None:
    """A manifest may claim Tier 2 passed only while its evidence binds to itself."""
    root = _make_release(tmp_path)
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    manifest = json.loads(release_path.read_text())

    # Self-bound evidence: accepted.
    assert tier2_binding_failure(manifest) is None
    validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")


def test_tier2_passed_rejected_when_evidence_targets_another_manifest(tmp_path: Path) -> None:
    """Regression for the v8 release: passed truth + evidence bound elsewhere.

    The installed v8 manifest (513,811 B) embedded evidence whose target was the
    pre-promotion candidate (510,986 B), so it certified a document that was not
    the one shipped.  Both binding shapes that produce it must fail closed.
    """
    root = _make_release(tmp_path)
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    manifest = json.loads(release_path.read_text())

    # (a) bound to a foreign core digest
    foreign = json.loads(release_path.read_text())
    foreign["release_id"] = "2026-08-20__public-global-presence-v4.0"
    manifest["tier2_evidence"]["release_manifest"] = {"core_sha256": tier2_core_digest(foreign)}
    assert "bound to core digest" in (tier2_binding_failure(manifest) or "")
    _write_release(root, manifest)
    with pytest.raises(V4ManifestPublishError, match="bound to core digest"):
        validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")

    # (b) the exact v8 shape: legacy whole-manifest bytes/SHA binding
    manifest["tier2_evidence"]["release_manifest"] = {
        "bytes": 510986,
        "sha256": "5df1ec6bfd0def781014b48ba9cad60f6b91e553681c6692482f4ed3f3165a37",
    }
    assert "core_sha256" in (tier2_binding_failure(manifest) or "")
    _write_release(root, manifest)
    with pytest.raises(V4ManifestPublishError, match="core_sha256"):
        validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")

    # (c) passed truth with no evidence at all
    manifest.pop("tier2_evidence")
    _write_release(root, manifest)
    with pytest.raises(V4ManifestPublishError, match="no tier2_evidence"):
        validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")


def test_tier2_pending_manifest_is_not_subject_to_binding(tmp_path: Path) -> None:
    """A candidate that honestly reports pending Tier 2 needs no binding...

    ...but must still be refused at publish, because publishing is what the
    Tier 2 gate protects.
    """
    root = _make_release(tmp_path)
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    manifest = json.loads(release_path.read_text())
    for pending in ("not_run", "pending", "blocked_until_tier2_passed"):
        manifest["release_truth"]["tier2_status"] = pending
        manifest.pop("tier2_evidence", None)
        assert tier2_binding_failure(manifest) is None
        _write_release(root, manifest)
        with pytest.raises(V4ManifestPublishError, match="Tier 1, Tier 2"):
            validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")


def test_core_digest_ignores_only_promotion_bookkeeping(tmp_path: Path) -> None:
    """Recording a Tier 2 verdict must not move the digest the evidence binds."""
    root = _make_release(tmp_path)
    release_path = root / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    candidate = json.loads(release_path.read_text())
    baseline = tier2_core_digest(candidate)

    promoted = dict(candidate)
    promoted["release_truth"] = {**candidate["release_truth"], "root_cutover": "passed_local"}
    promoted["production_cutover"] = True
    promoted["cutover_blocker"] = "anything"
    promoted["tier2_binding"] = {"algorithm": TIER2_BINDING_ALGORITHM, "core_sha256": baseline}
    assert tier2_core_digest(promoted) == baseline

    # Anything a browser actually measures is inside the bound core.
    for field, value in (
        ("artifacts", []), ("release_id", "2026-08-20__x"), ("grid", {"changed": 1}),
        ("tracks", {"changed": 1}), ("bbox", [0, 0, 1, 1]),
    ):
        assert tier2_core_digest({**candidate, field: value}) != baseline, field


def test_canonical_bytes_do_not_drift_between_modules() -> None:
    """The digest is only stable while every writer canonicalizes identically."""
    from scripts.gfw_v4_production_release import _canonical_bytes as builder_bytes
    from tasks.gfw_v4_daily_publish import _canonical_bytes as publish_bytes
    from tasks.gfw_v4_manifest_publisher import _canonical_bytes as validator_bytes

    sample = {
        "b": 1, "a": {"z": [1, 2, {"k": "值"}], "y": None},
        "unicode": "漁船 · GFW", "float": 1.5, "bool": True,
    }
    assert builder_bytes(sample) == validator_bytes(sample) == publish_bytes(sample)


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
    # Re-bind so the path check is what fails, not the (also correct) binding
    # mismatch this edit would otherwise trigger.
    bind_tier2_evidence(manifest, evidence_id="tier2-ok")
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
    bind_tier2_evidence(manifest, evidence_id="tier2-ok")
    release_path.write_text(json.dumps(manifest), encoding="utf-8")
    _refresh_root_release_pointer(root)
    with pytest.raises(V4ManifestPublishError, match="unsupported"):
        validate_v4_release_candidate(root, tier2_evidence_id="tier2-ok")

    frame["type"] = "track_frame_pmtiles"
    frame["spatial_contract"]["fixed_zoom"] = 7
    bind_tier2_evidence(manifest, evidence_id="tier2-ok")
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
