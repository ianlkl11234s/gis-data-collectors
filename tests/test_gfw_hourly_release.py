from __future__ import annotations

import json
import hashlib
from datetime import date, datetime, timezone

import pytest

from scripts.gfw_hourly_release import (
    build_daily_track_partition,
    manifest_assets,
    publish_release_to_s3,
    publish_staged_release,
    publish_track_release,
    stage_track_release,
)


class _FakeS3:
    def __init__(self, *, mismatch_key=None):
        self.objects = {}
        self.calls = []
        self.mismatch_key = mismatch_key

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        body = bytes(kwargs["Body"])
        self.calls.append(("put", key))
        self.objects[key] = {
            "Body": body,
            "Metadata": dict(kwargs.get("Metadata") or {}),
            "ContentType": kwargs.get("ContentType"),
            "CacheControl": kwargs.get("CacheControl"),
        }

    def head_object(self, **kwargs):
        key = kwargs["Key"]
        self.calls.append(("head", key))
        item = self.objects[key]
        metadata = dict(item["Metadata"])
        if key == self.mismatch_key:
            metadata["sha256"] = "0" * 64
        return {"ContentLength": len(item["Body"]), "Metadata": metadata}

    def delete_object(self, **kwargs):
        key = kwargs["Key"]
        self.calls.append(("delete", key))
        self.objects.pop(key, None)


def _sha(value):
    return hashlib.sha256(value).hexdigest()


def _track(
    vessel_id="v-1",
    times=None,
    coordinates=None,
):
    times = times or [
        "2026-08-14T23:00:00+00:00",
        "2026-08-15T00:00:00+00:00",
        "2026-08-16T00:00:00+00:00",
        "2026-08-16T02:00:00+00:00",
    ]
    coordinates = coordinates or [[0, 0], [1, 0], [25, 0], [27, 0]]
    return {
        "type": "Feature",
        "id": vessel_id,
        "properties": {
            "track_id": vessel_id,
            "vessel_id": vessel_id,
            "start_at": times[0],
            "end_at": times[-1],
            "observed_times": times,
            "point_count": len(times),
        },
        "geometry": {"type": "LineString", "coordinates": coordinates},
    }


def _collection(*features):
    return {"type": "FeatureCollection", "metadata": {}, "features": list(features)}


def test_daily_partition_has_bounded_overlap_and_interpolated_boundaries():
    partition = build_daily_track_partition(
        _collection(_track()), display_date=date(2026, 8, 15)
    )
    metadata = partition["metadata"]
    assert metadata["display_date"] == "2026-08-15"
    assert metadata["overlap"] == {
        "lookback_hours": 3.0,
        "lookahead_hours": 1.0,
        "window_start": "2026-08-14T21:00:00+00:00",
        "window_end": "2026-08-16T01:00:00+00:00",
    }
    assert metadata["supported_trail_hours"] == [0.5, 1.0, 2.0, 3.0]
    feature = partition["features"][0]
    assert feature["properties"]["observed_times"][-1] == "2026-08-16T01:00:00+00:00"
    assert feature["geometry"]["coordinates"][-1] == [26.0, 0.0]
    assert feature["properties"]["partition_boundary_interpolated"] is True


def test_stage_manifest_has_hash_size_feature_and_retention_contract(tmp_path):
    staging = stage_track_release(
        _collection(_track()),
        root=tmp_path,
        latest_complete_date="2026-08-15",
        date_start="2026-08-15",
        date_end="2026-08-15",
        generated_at="2026-08-16T00:00:00+00:00",
    )
    manifest = json.loads((staging / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["latest_complete_date"] == "2026-08-15"
    assert manifest["date_start"] == manifest["date_end"] == "2026-08-15"
    assert manifest["generated_at"] == "2026-08-16T00:00:00+00:00"
    assert manifest["retention"] == {
        "rolling_source_days": 1,
        "published_releases_kept": 2,
        "retained_release_day_payloads": 2,
        "calendar_date_union_if_published_daily": 2,
        "rollback_release_count": 1,
    }
    day = manifest["days"][0]
    assert len(day["sha256"]) == 64
    assert day["bytes"] > 0
    assert day["features"] == 1
    assert day["path"] == "days/2026-08-15.geojson"
    ledger = json.loads((staging / "run.json").read_text(encoding="utf-8"))
    assert ledger["raw_gfw_response_saved"] is False
    assert manifest["pipeline_limitations"]["shared_grid_track_normalized_fetch"] is False


def test_publish_manifest_last_keeps_current_and_previous_release(tmp_path):
    for day in ("2026-08-19", "2026-08-20", "2026-08-21"):
        publish_track_release(
            _collection(),
            root=tmp_path,
            latest_complete_date=day,
            date_start=day,
            date_end=day,
        )
    root_manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert root_manifest["release_id"] == "2026-08-21"
    assert root_manifest["days"][0]["path"].startswith(
        "releases/2026-08-21/days/"
    )
    assert sorted(path.name for path in (tmp_path / "releases").iterdir()) == [
        "2026-08-20",
        "2026-08-21",
    ]
    assert not (tmp_path / "releases" / "2026-08-19").exists()
    ledger = json.loads(
        (tmp_path / "run-ledger" / "2026-08-21.json").read_text(encoding="utf-8")
    )
    assert ledger["status"] == "published"


def test_validation_failure_preserves_staging_spool_and_old_manifest(tmp_path):
    publish_track_release(
        _collection(),
        root=tmp_path,
        latest_complete_date="2026-08-20",
        date_start="2026-08-20",
        date_end="2026-08-20",
    )
    old_manifest = (tmp_path / "manifest.json").read_bytes()
    staging = stage_track_release(
        _collection(_track(
            times=["2026-08-21T00:00:00+00:00", "2026-08-21T01:00:00+00:00"],
            coordinates=[[0, 0], [1, 0]],
        )),
        root=tmp_path,
        latest_complete_date="2026-08-21",
        date_start="2026-08-21",
        date_end="2026-08-21",
    )
    partition = staging / "days" / "2026-08-21.geojson"
    partition.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="FeatureCollection"):
        publish_staged_release(root=tmp_path, staging_dir=staging)
    assert staging.exists()
    assert partition.exists()
    assert (tmp_path / "manifest.json").read_bytes() == old_manifest


def test_stage_rejects_existing_staging_instead_of_deleting_it(tmp_path):
    staging = stage_track_release(
        _collection(),
        root=tmp_path,
        latest_complete_date="2026-08-21",
        date_start="2026-08-21",
        date_end="2026-08-21",
    )
    marker = staging / "operator-note.txt"
    marker.write_text("keep", encoding="utf-8")
    with pytest.raises(FileExistsError, match="preserved"):
        stage_track_release(
            _collection(),
            root=tmp_path,
            latest_complete_date="2026-08-21",
            date_start="2026-08-21",
            date_end="2026-08-21",
        )
    assert marker.read_text(encoding="utf-8") == "keep"


def test_cleanup_refuses_unexpected_file_and_keeps_old_release(tmp_path):
    for day in ("2026-08-19", "2026-08-20"):
        publish_track_release(
            _collection(), root=tmp_path,
            latest_complete_date=day, date_start=day, date_end=day,
        )
    protected = tmp_path / "releases" / "2026-08-19" / "operator-note.txt"
    protected.write_text("keep", encoding="utf-8")
    result = publish_track_release(
        _collection(), root=tmp_path,
        latest_complete_date="2026-08-21",
        date_start="2026-08-21", date_end="2026-08-21",
    )
    assert result["pruned_release_ids"] == []
    assert result["prune_warnings"][0]["release_id"] == "2026-08-19"
    assert protected.read_text(encoding="utf-8") == "keep"
    assert json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))[
        "release_id"
    ] == "2026-08-21"


def test_s3_uploads_generic_assets_verifies_head_and_puts_root_manifest_last(tmp_path):
    release = stage_track_release(
        _collection(), root=tmp_path,
        latest_complete_date="2026-08-21",
        date_start="2026-08-21", date_end="2026-08-21",
    )
    # Prove the publisher is generic: add a grid hourly asset while preserving
    # the frontend-specific tracks day and grid hours indexes.
    grid_path = release / "hours" / "20260821T00Z.geojson"
    grid_path.parent.mkdir()
    grid_body = b'{"type":"FeatureCollection","features":[]}'
    grid_path.write_bytes(grid_body)
    manifest_path = release / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    grid_asset = {
        "path": "hours/20260821T00Z.geojson",
        "sha256": _sha(grid_body),
        "bytes": len(grid_body),
        "type": "grid_hour",
        "features": 0,
    }
    manifest["assets"].append(grid_asset)
    manifest["tracks"] = {"days": manifest.pop("days")}
    manifest["grid"] = {
        "hours": [{"observed_at": "2026-08-21T00:00:00Z", **grid_asset}]
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    client = _FakeS3()
    result = publish_release_to_s3(
        client,
        release_dir=release,
        bucket="gfw-release-test",
        key_prefix="public/gfw-hourly",
        public_url_prefix="https://assets.example.test/gfw-hourly",
    )
    put_keys = [key for operation, key in client.calls if operation == "put"]
    root_key = "public/gfw-hourly/manifest.json"
    assert put_keys[-1] == root_key
    assert put_keys[:-1] == [
        "public/gfw-hourly/releases/2026-08-21/days/2026-08-21.geojson",
        "public/gfw-hourly/releases/2026-08-21/hours/20260821T00Z.geojson",
        "public/gfw-hourly/releases/2026-08-21/run.json",
        "public/gfw-hourly/releases/2026-08-21/manifest.json",
    ]
    for key in put_keys:
        assert ("head", key) in client.calls
        assert client.objects[key]["Metadata"]["sha256"] == _sha(
            client.objects[key]["Body"]
        )
    for key in put_keys[:-1]:
        assert client.objects[key]["CacheControl"] == (
            "public,max-age=604800,s-maxage=604800,immutable"
        )
    assert client.objects[root_key]["CacheControl"] == (
        "public,max-age=60,s-maxage=60,stale-while-revalidate=300"
    )
    root = json.loads(client.objects[root_key]["Body"])
    assert root["origin_mapping"] == {
        "s3_key_prefix": "public/gfw-hourly",
        "public_url_prefix": "https://assets.example.test/gfw-hourly",
        "path_rule": "public_url_prefix + '/' + key relative to s3_key_prefix",
    }
    assert {asset["type"] for asset in root["assets"]} == {
        "tracks_day", "grid_hour"
    }
    assert root["tracks"]["days"][0]["path"] == (
        "releases/2026-08-21/days/2026-08-21.geojson"
    )
    assert root["grid"]["hours"][0]["path"] == (
        "releases/2026-08-21/hours/20260821T00Z.geojson"
    )
    assert result["public_manifest_url"].endswith("/manifest.json")


def test_s3_asset_head_hash_mismatch_never_cuts_over_or_prunes(tmp_path):
    release = stage_track_release(
        _collection(), root=tmp_path,
        latest_complete_date="2026-08-21",
        date_start="2026-08-21", date_end="2026-08-21",
    )
    asset_key = "public/gfw-hourly/releases/2026-08-21/days/2026-08-21.geojson"
    client = _FakeS3(mismatch_key=asset_key)
    with pytest.raises(RuntimeError, match="sha256 metadata mismatch"):
        publish_release_to_s3(
            client, release_dir=release, bucket="gfw-release-test",
            key_prefix="public/gfw-hourly",
            public_url_prefix="https://assets.example.test/gfw-hourly",
        )
    assert "public/gfw-hourly/manifest.json" not in client.objects
    assert not any(operation == "delete" for operation, _ in client.calls)
    assert release.exists()


def _previous_release(release_id):
    prefix = f"public/gfw-hourly/releases/{release_id}"
    return {
        "release_id": release_id,
        "manifest_key": f"{prefix}/manifest.json",
        "object_keys": [
            f"{prefix}/days/{release_id}.geojson",
            f"{prefix}/run.json",
            f"{prefix}/manifest.json",
        ],
    }


def test_s3_prunes_only_manifest_enumerated_exact_old_release_keys_after_cutover(tmp_path):
    release = stage_track_release(
        _collection(), root=tmp_path,
        latest_complete_date="2026-08-21",
        date_start="2026-08-21", date_end="2026-08-21",
    )
    previous = {
        "published_releases": [
            _previous_release("2026-08-20"),
            _previous_release("2026-08-19"),
        ]
    }
    client = _FakeS3()
    result = publish_release_to_s3(
        client, release_dir=release, bucket="gfw-release-test",
        key_prefix="public/gfw-hourly",
        public_url_prefix="https://assets.example.test/gfw-hourly",
        previous_root_manifest=previous,
    )
    expected_deleted = _previous_release("2026-08-19")["object_keys"]
    assert result["deleted_object_keys"] == expected_deleted
    root_put_index = client.calls.index(("put", "public/gfw-hourly/manifest.json"))
    assert all(
        client.calls.index(("delete", key)) > root_put_index
        for key in expected_deleted
    )
    assert not any("2026-08-20" in key for key in result["deleted_object_keys"])


def test_s3_unknown_previous_key_fails_closed_before_upload_or_delete(tmp_path):
    release = stage_track_release(
        _collection(), root=tmp_path,
        latest_complete_date="2026-08-21",
        date_start="2026-08-21", date_end="2026-08-21",
    )
    bad = _previous_release("2026-08-19")
    bad["object_keys"].append("other-prefix/operator-file.txt")
    client = _FakeS3()
    with pytest.raises(ValueError, match="unknown or unsafe"):
        publish_release_to_s3(
            client, release_dir=release, bucket="gfw-release-test",
            key_prefix="public/gfw-hourly",
            public_url_prefix="https://assets.example.test/gfw-hourly",
            previous_root_manifest={"published_releases": [bad]},
        )
    assert client.calls == []
    assert release.exists()


def test_manifest_assets_derives_legacy_tracks_days():
    day = {
        "path": "days/2026-08-21.geojson",
        "sha256": "a" * 64,
        "bytes": 12,
        "features": 3,
    }
    assert manifest_assets({"days": [day]}) == [{
        **day,
        "type": "tracks_day",
    }]
