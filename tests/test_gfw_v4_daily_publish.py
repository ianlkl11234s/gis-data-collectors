from __future__ import annotations

from pathlib import Path
from datetime import date
import gzip
import hashlib
import json

import pytest
import schedule

import main

from tasks.gfw_v4_daily_publish import (
    DefaultV4CandidateFinalizer,
    GFWV4DailyPublishSettings,
    GFWV4DailyPublishTask,
    GFWV4SourceContractBlocked,
    default_v4_spatial_repackager,
    promote_v4_candidate_with_tier2_evidence,
    validate_normalized_v4_daily_source,
)
from tasks.gfw_v4_manifest_publisher import (
    TIER2_BINDING_ALGORITHM,
    tier2_binding_failure,
    tier2_core_digest,
)


def _bind_tier2(manifest: dict, *, evidence_id: str) -> dict:
    """Bind minimal Tier 2 evidence to this manifest's own core digest."""
    digest = tier2_core_digest(manifest)
    manifest["tier2_evidence_id"] = evidence_id
    manifest["tier2_evidence"] = {
        "schema_version": 1, "kind": "gfw_v4_tier2_browser_evidence",
        "evidence_id": evidence_id, "release_manifest": {"core_sha256": digest},
    }
    manifest["tier2_binding"] = {"algorithm": TIER2_BINDING_ALGORITHM, "core_sha256": digest}
    manifest.setdefault("release_truth", {})["tier2_evidence_id"] = evidence_id
    return manifest


def _settings() -> GFWV4DailyPublishSettings:
    return GFWV4DailyPublishSettings(True, True, True, "tier2-accepted", "token", "postgresql://db", "bucket", "deploy-assets/global-maritime/gfw-hourly/v4", "https://cdn.example/v4")


def _source() -> dict:
    return {
        "schema_version": 1,
        "kind": "gfw_v4_normalized_daily_source",
        "selected_utc_date": "2026-08-21",
        "raw_response_saved": False,
        "presence": {
            "next_offset_complete": True,
            "resolved_dataset_version": "public-global-presence:v4.0",
            "records": [],
        },
        "fishing_effort": {
            "next_offset_complete": True,
            "resolved_dataset_version": "public-global-fishing-effort:v3.0",
            "records": [],
        },
    }


def test_v4_source_contract_rejects_poc_and_raw_payloads():
    with pytest.raises(GFWV4SourceContractBlocked, match="POC"):
        validate_normalized_v4_daily_source({"poc": True})
    with pytest.raises(GFWV4SourceContractBlocked, match="raw"):
        validate_normalized_v4_daily_source({**_source(), "raw_response_saved": True})


def test_v4_task_is_unschedulable_without_reviewed_adapter():
    with pytest.raises(GFWV4SourceContractBlocked, match="blocked"):
        GFWV4DailyPublishTask(settings=_settings(), asset_toolchain_preflight=lambda: None).run()


def test_v4_task_only_publishes_reviewed_normalized_source():
    published = []
    result = GFWV4DailyPublishTask(
        source_provider=_source,
        publisher=lambda value: published.append(value) or {"status": "published"},
        settings=_settings(), asset_toolchain_preflight=lambda: None,
    ).run()
    assert result == {"status": "published"}
    assert published == [_source()]


def test_v4_task_injects_source_finalizer_and_manifest_publisher():
    events = []

    def finalize(value):
        events.append(("finalize", value))
        return Path("/tmp/final-schema4-release")

    def publisher_factory(path, settings):
        events.append(("publisher_factory", path, settings.tier2_evidence_id))
        return lambda value: events.append(("publish", value)) or {"status": "published"}

    task = GFWV4DailyPublishTask(
        source_provider=_source,
        candidate_finalizer=finalize,
        publisher_factory=publisher_factory,
        settings=_settings(),
        asset_toolchain_preflight=lambda: None,
    )
    assert task.run() == {"status": "published"}
    assert [event[0] for event in events] == ["finalize", "publisher_factory", "publish"]


def test_default_finalizer_calls_schema4_builder_only_with_reviewed_inputs(tmp_path):
    calls = []

    def builder(*, output_root, sqlite_path, high_ndjson, high_metrics_path,
                fishing_asset, selected_day, fishing_asset_sha256=None,
                fishing_lineage=None):
        kwargs = {
            "output_root": output_root, "sqlite_path": sqlite_path,
            "high_ndjson": high_ndjson, "high_metrics_path": high_metrics_path,
            "fishing_asset": fishing_asset, "selected_day": selected_day,
            "fishing_asset_sha256": fishing_asset_sha256,
            "fishing_lineage": fishing_lineage,
        }
        calls.append(kwargs)
        output = kwargs["output_root"]
        output.mkdir(parents=True)
        release = output / "releases" / "2026-08-21__public-global-presence-v4.0"
        release.mkdir(parents=True)
        frame = release / "tracks" / "fishing" / "frames" / "00.pmtiles"
        frame.parent.mkdir(parents=True)
        frame.write_bytes(b"pmtiles")
        frame_path = "releases/2026-08-21__public-global-presence-v4.0/tracks/fishing/frames/00.pmtiles"
        built = {
            "release_id": "2026-08-21__public-global-presence-v4.0",
            "artifacts": [{
                "path": frame_path, "type": "track_frame_pmtiles", "bytes": 7,
                "sha256": hashlib.sha256(b"pmtiles").hexdigest(),
                "semantic_counts": {"bucket": "fishing", "observed_at": "2026-08-21T00:00:00Z"},
            }],
            "tracks": {"bucket_data": {"fishing": {"frames": [{"path": "tracks/fishing/frames/00.pmtiles"}]}}},
            "release_truth": {"tier2_status": "passed", "readback_status": "passed"},
        }
        _bind_tier2(built, evidence_id="tier2-accepted")
        (release / "manifest.json").write_text(json.dumps(built), encoding="utf-8")
        (output / "manifest.json").write_text(
            '{"production_cutover":"passed",'
            '"release_manifest":{"path":"releases/2026-08-21__public-global-presence-v4.0/manifest.json"}}',
            encoding="utf-8",
        )
        return {"output_root": str(output)}

    source = {
        **_source(),
        "finalizer_inputs": {
            "presence_compare": str(tmp_path / "presence.sqlite"),
            "high_ndjson": str(tmp_path / "high.ndjson"),
            "high_metrics": str(tmp_path / "high.metrics.json"),
            "fishing_asset": str(tmp_path / "fishing.geojson.gz"),
            "fishing_asset_sha256": "a" * 64,
            "fishing_lineage": {"date": "2026-08-21"},
        },
    }
    candidate = DefaultV4CandidateFinalizer(
        tmp_path, builder=builder, spatial_repackager=lambda path: path,
    )(source)
    assert candidate.is_dir()
    assert calls[0]["selected_day"] == date(2026, 8, 21)
    assert calls[0]["fishing_asset"] == tmp_path / "fishing.geojson.gz"
    assert calls[0]["fishing_asset_sha256"] == "a" * 64
    assert calls[0]["fishing_lineage"] == {"date": "2026-08-21"}


def test_spatial_repackager_replaces_frames_and_nested_index(monkeypatch, tmp_path):
    candidate = tmp_path / "candidate"
    release_dir = candidate / "releases" / "2026-08-21__public-global-presence-v4.0"
    frame = release_dir / "tracks" / "fishing" / "frames" / "00.geojson.gz"
    frame.parent.mkdir(parents=True)
    frame.write_bytes(gzip.compress(b'{"type":"FeatureCollection","features":[]}', mtime=0))
    old_path = "releases/2026-08-21__public-global-presence-v4.0/tracks/fishing/frames/00.geojson.gz"
    new_path = old_path.replace(".geojson.gz", ".pmtiles")
    release = {
        "release_id": "2026-08-21__public-global-presence-v4.0",
        "artifacts": [{"path": old_path, "type": "track_frame_hour", "semantic_counts": {"bucket": "fishing", "observed_at": "2026-08-21T00:00:00Z"}}],
        "tracks": {"bucket_data": {"fishing": {"frames": [{"path": "tracks/fishing/frames/00.geojson.gz"}]}}},
        "release_truth": {
            "tier2_status": "passed", "tier2_evidence_id": "tier2-accepted",
            "readback_status": "passed", "production_cutover": "passed",
        },
    }
    (release_dir / "manifest.json").write_text(json.dumps(release), encoding="utf-8")
    (candidate / "manifest.json").write_text(json.dumps({
        "release_manifest": {"path": "releases/2026-08-21__public-global-presence-v4.0/manifest.json"},
        "production_cutover": "passed",
    }), encoding="utf-8")

    def fake_spatial_frame(*, source, output, observed_at, bucket, release_root):
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"pmtiles-fixed-z6")
        sha = hashlib.sha256(output.read_bytes()).hexdigest()
        return {
            "path": new_path, "type": "track_frame_pmtiles", "bytes": output.stat().st_size,
            "content_length": output.stat().st_size, "sha256": sha, "etag": f'"{sha}"',
            "content_type": "application/octet-stream", "content_encoding": "identity",
            "cache_control": "public,max-age=604800,s-maxage=604800,immutable",
            "semantic_counts": {"bucket": bucket, "observed_at": observed_at, "feature_count": 0},
            "spatial_contract": {"source_feature_count": 0, "decoded_feature_count": 0, "identity_duplicate_count": 0, "identity_missing_count": 0},
        }

    import scripts.gfw_v4_spatial_frames as spatial
    monkeypatch.setattr(spatial, "build_spatial_frame", fake_spatial_frame)
    output = default_v4_spatial_repackager(candidate, tier2_evidence_id="tier2-accepted")
    manifest = json.loads((output / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json").read_text())
    assert manifest["artifacts"][0]["type"] == "track_frame_pmtiles"
    assert manifest["tracks"]["bucket_data"]["fishing"]["frames"][0]["path"] == "tracks/fishing/frames/00.pmtiles"
    assert manifest["release_truth"]["tier2_evidence_id"] == "tier2-accepted"


def _promotion_candidate(tmp_path):
    candidate = tmp_path / "candidate"
    release_id = "2026-08-21__public-global-presence-v4.0"
    release_dir = candidate / "releases" / release_id
    frame = release_dir / "tracks" / "fishing" / "frames" / "00.pmtiles"
    frame.parent.mkdir(parents=True)
    frame.write_bytes(b"pmtiles-formal")
    path = f"releases/{release_id}/tracks/fishing/frames/00.pmtiles"
    sha = hashlib.sha256(frame.read_bytes()).hexdigest()
    manifest = {
        "schema_version": 4, "release_id": release_id, "selected_utc_date": "2026-08-21",
        "release_truth": {"tier1_status": "passed", "tier2_status": "not_run", "readback_status": "passed", "root_cutover": "blocked_until_tier2_passed"},
        "artifacts": [{"path": path, "type": "track_frame_pmtiles", "bytes": frame.stat().st_size, "sha256": sha,
                       "semantic_counts": {"bucket": "fishing", "observed_at": "2026-08-21T00:00:00Z", "feature_count": 1}}],
        "tracks": {"bucket_data": {"fishing": {"frames": [{"path": "tracks/fishing/frames/00.pmtiles"}]}}},
    }
    release_dir.mkdir(parents=True, exist_ok=True)
    release_file = release_dir / "manifest.json"
    release_file.write_bytes(json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode())
    body = release_file.read_bytes()
    (candidate / "manifest.json").write_bytes(json.dumps({
        "schema_version": 4, "release_manifest": {"path": f"releases/{release_id}/manifest.json", "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()},
        "production_cutover": False,
    }, sort_keys=True, separators=(",", ":")).encode())
    return candidate, tier2_core_digest(manifest)


def _tier2_evidence(core_sha256, *, heap_status="passed", range_206=True):
    def profile():
        return {
            "status": "passed",
            "desktop": {"status": "passed", "metrics": {"initial_bytes": 10, "transfer_bytes": 20, "max_heap_bytes": 30, "frame_p95_ms": 10.0, "frame_updates": 96}, "heap": {"status": heap_status, "bytes": 30}, "wire": {"status": "passed", "range_206": range_206}},
            "mobile": {"status": "passed", "metrics": {"initial_bytes": 10, "transfer_bytes": 20, "max_heap_bytes": 30, "frame_p95_ms": 20.0, "frame_updates": 96}, "heap": {"status": heap_status, "bytes": 30}, "wire": {"status": "passed", "range_206": range_206}},
        }
    return {"schema_version": 1, "kind": "gfw_v4_tier2_browser_evidence", "evidence_id": "tier2-browser-20260829",
            "release_manifest": {"core_sha256": core_sha256}, "thresholds": {
                "default": {"desktop": {"initial_bytes_max": 100, "transfer_bytes_max": 100, "max_heap_bytes_max": 100, "frame_p95_ms_max": 16.7, "frame_updates_min": 96}, "mobile": {"initial_bytes_max": 100, "transfer_bytes_max": 100, "max_heap_bytes_max": 100, "frame_p95_ms_max": 33, "frame_updates_min": 96}},
                "all": {"desktop": {"initial_bytes_max": 100, "transfer_bytes_max": 100, "max_heap_bytes_max": 100, "frame_p95_ms_max": 33, "frame_updates_min": 96}, "mobile": {"initial_bytes_max": 100, "transfer_bytes_max": 100, "max_heap_bytes_max": 100, "frame_p95_ms_max": 50, "frame_updates_min": 96}}},
            "profiles": {"default": profile(), "all": profile()},
            "asset_budgets": {"track_frame_pmtiles": {"max_bytes": 100, "max_feature_count": 10}}}


def test_promotion_creates_new_immutable_formal_root_from_bound_evidence(tmp_path):
    candidate, core = _promotion_candidate(tmp_path)
    original = (candidate / "manifest.json").read_bytes()
    promoted = promote_v4_candidate_with_tier2_evidence(
        candidate, _tier2_evidence(core), tmp_path / "promoted",
    )
    assert promoted != candidate and promoted.is_dir()
    assert (candidate / "manifest.json").read_bytes() == original
    root = json.loads((promoted / "manifest.json").read_text())
    release = json.loads((promoted / root["release_manifest"]["path"]).read_text())
    assert root["production_cutover"] is True
    assert release["release_truth"]["tier2_evidence_id"] == "tier2-browser-20260829"
    assert release["release_truth"]["root_cutover"] == "passed_local"
    assert "blocked_until_tier2_passed" not in json.dumps(release)
    body = (promoted / root["release_manifest"]["path"]).read_bytes()
    assert root["release_manifest"]["sha256"] == hashlib.sha256(body).hexdigest()
    assert root["release_manifest"]["bytes"] == len(body)


def test_promoted_manifest_evidence_binds_to_the_manifest_that_ships(tmp_path):
    """The promoted manifest must certify itself, not the candidate it came from.

    v8 shipped a manifest whose embedded evidence named the pre-promotion
    candidate, because promotion validated the binding and *then* changed the
    bytes.  Binding on the core digest survives that rewrite.
    """
    candidate, core = _promotion_candidate(tmp_path)
    promoted = promote_v4_candidate_with_tier2_evidence(
        candidate, _tier2_evidence(core), tmp_path / "promoted",
    )
    root = json.loads((promoted / "manifest.json").read_text())
    release = json.loads((promoted / root["release_manifest"]["path"]).read_text())

    assert release["release_truth"]["tier2_status"] == "passed"
    # The shipped manifest satisfies the fail-closed gate on its own bytes.
    assert tier2_binding_failure(release) is None
    assert release["tier2_evidence"]["release_manifest"]["core_sha256"] == tier2_core_digest(release)
    assert release["tier2_binding"]["core_sha256"] == core
    # And no stale blocker text contradicting the verdict.
    assert "cutover_blocker" not in release


def test_promotion_rejects_evidence_bound_to_a_different_release(tmp_path):
    candidate, core = _promotion_candidate(tmp_path)
    foreign = "f" * 64
    assert foreign != core
    with pytest.raises(GFWV4SourceContractBlocked, match="core digest"):
        promote_v4_candidate_with_tier2_evidence(
            candidate, _tier2_evidence(foreign), tmp_path / "promoted",
        )
    assert not (tmp_path / "promoted").exists()


def test_promotion_rejects_legacy_whole_manifest_byte_binding(tmp_path):
    """Byte binding cannot survive promotion, so it must never be accepted."""
    candidate, core = _promotion_candidate(tmp_path)
    release_file = candidate / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    body = release_file.read_bytes()
    evidence = _tier2_evidence(core)
    evidence["release_manifest"] = {"bytes": len(body), "sha256": hashlib.sha256(body).hexdigest()}
    with pytest.raises(GFWV4SourceContractBlocked, match="core digest"):
        promote_v4_candidate_with_tier2_evidence(candidate, evidence, tmp_path / "promoted")


def test_promotion_fails_closed_if_it_mutates_a_bound_field(tmp_path, monkeypatch):
    """Any future promotion edit outside the excluded set must fail, not ship."""
    import tasks.gfw_v4_daily_publish as daily

    real = daily.tier2_core_digest
    calls = {"n": 0}

    def drifting(manifest):
        # Simulate promotion touching a bound field: the post-mutation digest
        # no longer matches what the evidence was validated against.
        calls["n"] += 1
        if calls["n"] == 1:
            return real(manifest)
        return real({**manifest, "release_id": "2026-08-20__drifted"})

    candidate, core = _promotion_candidate(tmp_path)
    monkeypatch.setattr(daily, "tier2_core_digest", drifting)
    with pytest.raises(GFWV4SourceContractBlocked, match="mutated a Tier2-bound"):
        promote_v4_candidate_with_tier2_evidence(
            candidate, _tier2_evidence(core), tmp_path / "promoted",
        )
    assert not (tmp_path / "promoted").exists()


@pytest.mark.parametrize("kwargs,match", [
    ({"heap_status": "unavailable"}, "heap"),
    ({"range_206": False}, "206"),
])
def test_promotion_rejects_incomplete_browser_evidence(tmp_path, kwargs, match):
    candidate, core = _promotion_candidate(tmp_path)
    with pytest.raises(GFWV4SourceContractBlocked, match=match):
        promote_v4_candidate_with_tier2_evidence(
            candidate, _tier2_evidence(core, **kwargs), tmp_path / "promoted",
        )


@pytest.mark.parametrize("mutate,match", [
    (lambda evidence: evidence["profiles"]["default"]["desktop"]["metrics"].update(frame_p95_ms=16.7), "frame budget"),
    (lambda evidence: evidence["profiles"]["all"]["mobile"]["metrics"].update(frame_updates=95), "frame budget"),
    (lambda evidence: evidence["profiles"]["default"]["mobile"]["metrics"].update(max_heap_bytes=31), "heap"),
])
def test_promotion_enforces_frozen_frame_and_heap_contract(tmp_path, mutate, match):
    candidate, core = _promotion_candidate(tmp_path)
    evidence = _tier2_evidence(core)
    mutate(evidence)
    with pytest.raises(GFWV4SourceContractBlocked, match=match):
        promote_v4_candidate_with_tier2_evidence(candidate, evidence, tmp_path / "promoted")


@pytest.mark.parametrize("bad_path", [
    "deploy-assets/global-maritime/gfw-hourly/v4/releases/bad.pmtiles",
    "/releases/2026-08-21__public-global-presence-v4.0/tracks/fishing/frames/00.pmtiles",
    "releases/2026-08-21__public-global-presence-v4.0/tracks/../bad.pmtiles",
])
def test_promotion_rejects_noncanonical_artifact_prefix(tmp_path, bad_path):
    candidate, _core = _promotion_candidate(tmp_path)
    release_file = candidate / "releases" / "2026-08-21__public-global-presence-v4.0" / "manifest.json"
    release = json.loads(release_file.read_text())
    release["artifacts"][0]["path"] = bad_path
    release_file.write_bytes(json.dumps(release, sort_keys=True, separators=(",", ":")).encode())
    body = release_file.read_bytes()
    root_file = candidate / "manifest.json"
    root = json.loads(root_file.read_text())
    root["release_manifest"].update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest())
    root_file.write_bytes(json.dumps(root, sort_keys=True, separators=(",", ":")).encode())
    with pytest.raises(GFWV4SourceContractBlocked, match="exact releases"):
        promote_v4_candidate_with_tier2_evidence(
            # Bind to the mutated manifest so the path check is what fails.
            candidate, _tier2_evidence(tier2_core_digest(release)), tmp_path / "promoted",
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"db_url": ""},
        {"public_url_prefix": "http://cdn.example/v4"},
        {"public_url_prefix": "https://cdn.example/v4?cache=bypass"},
    ],
)
def test_v4_preflight_rejects_incomplete_runtime_configuration(kwargs):
    values = {
        "enabled": True, "redistribution_approved": True, "single_writer": True,
        "tier2_evidence_id": "tier2-accepted", "token": "token",
        "db_url": "postgresql://db", "bucket": "bucket",
        "s3_prefix": "deploy-assets/global-maritime/gfw-hourly/v4",
        "public_url_prefix": "https://cdn.example/v4",
    }
    values.update(kwargs)
    with pytest.raises(GFWV4SourceContractBlocked):
        GFWV4DailyPublishSettings(**values).validate_preflight()


def test_main_registers_only_with_all_runtime_dependencies(monkeypatch):
    for name, value in {
        "GFW_V4_DAILY_PUBLISH_ENABLED": True,
        "GFW_V4_DAILY_REDISTRIBUTION_APPROVED": True,
        "GFW_V4_DAILY_SINGLE_WRITER": True,
        "GFW_V4_DAILY_TIER2_EVIDENCE_ID": "tier2-accepted",
        "GFW_ACCESS_TOKEN": "token",
        "SUPABASE_DB_URL": "postgresql://db",
        "S3_BUCKET": "bucket",
        "GFW_V4_DAILY_S3_PREFIX": "deploy-assets/global-maritime/gfw-hourly/v4",
        "GFW_V4_DAILY_PUBLIC_URL_PREFIX": "https://cdn.example/v4",
    }.items():
        monkeypatch.setattr(main.config, name, value)

    schedule.clear()
    try:
        task = main.run_gfw_v4_daily_publish_task(
            source_provider=_source,
            candidate_finalizer=lambda value: Path("/tmp/final-schema4-release"),
            publisher_factory=lambda path, settings: lambda value: {"status": "published"},
            asset_toolchain_preflight=lambda: None,
        )
        assert task is not None
        assert any(job.at_time.strftime("%H:%M") == "08:30" for job in schedule.jobs)
    finally:
        schedule.clear()


def test_main_does_not_register_when_public_url_is_missing(monkeypatch):
    for name, value in {
        "GFW_V4_DAILY_PUBLISH_ENABLED": True,
        "GFW_V4_DAILY_REDISTRIBUTION_APPROVED": True,
        "GFW_V4_DAILY_SINGLE_WRITER": True,
        "GFW_V4_DAILY_TIER2_EVIDENCE_ID": "tier2-accepted",
        "GFW_ACCESS_TOKEN": "token",
        "SUPABASE_DB_URL": "postgresql://db",
        "S3_BUCKET": "bucket",
        "GFW_V4_DAILY_S3_PREFIX": "deploy-assets/global-maritime/gfw-hourly/v4",
        "GFW_V4_DAILY_PUBLIC_URL_PREFIX": "",
    }.items():
        monkeypatch.setattr(main.config, name, value)
    schedule.clear()
    try:
        assert main.run_gfw_v4_daily_publish_task(
            source_provider=_source,
            candidate_finalizer=lambda value: Path("/tmp/final-schema4-release"),
            publisher_factory=lambda path, settings: lambda value: {"status": "published"},
            asset_toolchain_preflight=lambda: None,
        ) is None
        assert schedule.jobs == []
    finally:
        schedule.clear()
