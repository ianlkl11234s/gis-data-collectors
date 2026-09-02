import hashlib
import io
import json
import sys
import types
import zipfile

import pytest
import requests

from collectors.global_events import (
    GKGArtifact,
    GKGIndexGap,
    GlobalEventsCollector,
    artifact_sha256,
    build_compact_batch,
    content_sha256,
    parse_gkg_artifact,
    parse_master_index,
    selected_artifact_manifest,
    validate_stage1,
)


def _zip(rows):
    raw = ("\n".join("\t".join(row) for row in rows) + "\n").encode()
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        archive.writestr("20260901120000.gkg.csv", raw)
    return out.getvalue()


def _row(domain, title):
    row = [
        "20260901120000-id",
        "20260901120000",
        "1",
        domain,
        f"https://{domain}/story",
    ] + [""] * 22
    row[7] = "DISASTER_EARTHQUAKE"
    row[9] = "1#City#US#US.01#1#2#x"
    row[26] = f"<PAGE_TITLE>{title}</PAGE_TITLE>"
    return row


def test_gkg_parser_metadata_only_and_title():
    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    artifact = GKGArtifact(
        "standard",
        "20260901120000",
        len(payload),
        hashlib.md5(payload).hexdigest(),
        "https://example",
    )
    records = parse_gkg_artifact(artifact, payload)
    assert records[0]["title"] == "Major earthquake kills dozens"
    assert records[0]["impact_signals"] == ["major_disaster"]
    assert "body" not in records[0]


def test_master_index_and_contiguous_gap():
    text = "\n".join(
        [
            "10 aaaa https://data.gdeltproject.org/20260901120000.gkg.csv.zip",
            "10 bbbb https://data.gdeltproject.org/20260901123000.gkg.csv.zip",
        ]
    )
    artifacts = parse_master_index(text, "standard")
    assert [item.slot for item in artifacts] == ["20260901120000", "20260901123000"]
    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._select_pending(artifacts, None)
    with pytest.raises(GKGIndexGap):
        collector._select_pending(artifacts, "20260901114500")


def test_indexed_translation_404_is_explicit_artifact_unavailable(monkeypatch):
    from collectors.global_events import GKGArtifactUnavailable

    artifact = GKGArtifact(
        "translation", "20260901163000", 10, "a" * 32, "https://example/404"
    )

    class NotReadyResponse:
        status_code = 404
        content = b""

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = type(
        "NotReadySession", (), {"get": lambda self, url, timeout: NotReadyResponse()}
    )()
    with pytest.raises(GKGArtifactUnavailable, match="HTTP 404"):
        collector._download_artifact(artifact)


def test_openrouter_none_content_has_single_clear_failure(monkeypatch):
    import config

    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {"finish_reason": "stop", "message": {"content": None}}
                ]
            }

    class Session:
        def post(self, *args, **kwargs):
            return Response()

    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = Session()
    monkeypatch.setattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)
    with pytest.raises(ValueError, match="content must be a non-empty string"):
        collector._request_stage1([])
    assert getattr(collector, "_raw_response_sha256", None) is None


@pytest.mark.parametrize(
    ("content", "finish_reason", "message"),
    [
        ('{"assessments": [', "stop", "JSON decode failed"),
        ('{"assessments": [', "length", "incomplete response"),
    ],
)
def test_openrouter_malformed_or_truncated_stage1_is_observable_and_rejected(
    monkeypatch, content, finish_reason, message
):
    import config

    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    calls = []

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "finish_reason": finish_reason,
                        "message": {"content": content},
                    }
                ],
                "usage": {
                    "prompt_tokens": 7,
                    "completion_tokens": 9,
                    "completion_tokens_details": {"reasoning_tokens": 2},
                    "cost": 0.001,
                },
            }

    class Session:
        def post(self, *args, **kwargs):
            calls.append((args, kwargs))
            return Response()

    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = Session()
    monkeypatch.setattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)
    candidate = {
        "routing_rank": 1,
        "candidate_id": "cand_" + "a" * 24,
        "representative_documents": [
            {"title": "Major earthquake", "url": "https://example.test/story"}
        ],
        "coverage": {"documents": 1},
        "routing_evidence": {"impact_signals": ["major_disaster"]},
    }
    with pytest.raises(ValueError, match=message):
        collector._request_stage1([candidate])

    payload = calls[0][1]["json"]
    assert payload["response_format"] == {"type": "json_object"}
    assert payload["reasoning"] == {"effort": "none"}
    assert "provider" not in payload
    assert "json_schema" not in payload
    request_content = json.loads(payload["messages"][1]["content"])
    assert request_content["output_contract"]["assessment_count"] == 1
    assert request_content["output_contract"][
        "assessment_additional_properties"
    ] is False
    assert set(request_content["output_contract"]["assessment_required_fields"]) == {
        "candidate_id",
        "candidate_rank",
        "decision",
        "event_group",
        "title_zh_tw",
        "summary_zh_tw",
        "category",
        "severity",
        "severity_source",
        "taiwan_relationship",
        "taiwan_impact_zh_tw",
        "confidence",
        "reason_zh_tw",
    }
    assert collector._stage1_observation == {
        "finish_reason": finish_reason,
        "content_length": len(content),
        "usage": {
            "input_units": 7,
            "output_units": 9,
            "reasoning_units": 2,
            "cost_usd": 0.001,
        },
    }
    assert collector._raw_response_sha256 == hashlib.sha256(
        content.encode("utf-8")
    ).hexdigest()
    assert "assessments" not in str(collector._stage1_observation)


def test_compact_batch_hash_and_full_producer_lineage():
    batch = build_compact_batch(
        [],
        source_manifest_sha256="a" * 64,
        source_registry_sha256="b" * 64,
        producer_git_commit="c" * 40,
    )
    assert batch["batch_id"] == "batch_" + batch["content_sha256"][:24]
    assert batch["content_sha256"] == content_sha256(
        {"schema_version": batch["schema_version"], "payload": batch["payload"]}
    )
    with pytest.raises(ValueError, match="full 40"):
        build_compact_batch(
            [],
            source_manifest_sha256="a" * 64,
            source_registry_sha256=None,
            producer_git_commit="short",
        )


def test_selected_manifest_hash_ignores_later_index_rows():
    first = GKGArtifact("standard", "20260901120000", 10, "a" * 32, "https://a")
    later = GKGArtifact("standard", "20260901121500", 11, "b" * 32, "https://b")
    selected_from_short_index = content_sha256(
        {"standard": selected_artifact_manifest([first])}
    )
    selected_from_later_index = content_sha256(
        {"standard": selected_artifact_manifest([first])}
    )
    assert selected_from_short_index == selected_from_later_index
    assert selected_artifact_manifest([first, later]) != selected_artifact_manifest(
        [first]
    )


def test_stage1_opencc_normalization_is_recorded_in_lineage(monkeypatch):
    class FakeOpenCC:
        def __init__(self, _profile):
            pass

        def convert(self, value):
            return value.replace("台灣", "臺灣").replace("對台", "對臺")

    monkeypatch.setitem(sys.modules, "opencc", types.SimpleNamespace(OpenCC=FakeOpenCC))
    candidate = {"routing_rank": 1, "candidate_id": "cand_" + "a" * 24}
    result = {
        "assessments": [
            {
                "candidate_id": candidate["candidate_id"],
                "candidate_rank": 1,
                "decision": "keep_watch",
                "event_group": "E001",
                "title_zh_tw": "台灣地震",
                "summary_zh_tw": "造成重大影響",
                "category": "disaster",
                "severity": 2,
                "severity_source": "inferred",
                "taiwan_relationship": "unknown",
                "taiwan_impact_zh_tw": "尚無明確對台影響",
                "confidence": 0.5,
                "reason_zh_tw": "資料有限",
            }
        ]
    }
    lineage = []
    normalized = validate_stage1(result, [candidate], lineage)
    assert normalized["assessments"][0]["title_zh_tw"] == "臺灣地震"
    assert {entry["field"] for entry in lineage} == {
        "title_zh_tw",
        "taiwan_impact_zh_tw",
    }


def test_disabled_collector_never_fetches(monkeypatch, tmp_path):
    import config

    monkeypatch.setattr(config, "GLOBAL_EVENTS_ENABLED", False)
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    collector = GlobalEventsCollector()
    result = collector.collect()
    assert result["status"] == "disabled"
    assert result["db_contract_status"] == "migration_389_receipts"


def test_hourly_operating_defaults(monkeypatch):
    import config

    monkeypatch.delenv("GLOBAL_EVENTS_INTERVAL", raising=False)
    assert config.GLOBAL_EVENTS_INTERVAL == 60
    assert config.GLOBAL_EVENTS_MAX_FILES_PER_STREAM == 8
    assert config.GLOBAL_EVENTS_INITIAL_SLOTS == 4
    assert config.GLOBAL_EVENTS_QWEN_MAX_CANDIDATES == 15
    assert config.GLOBAL_EVENTS_QWEN_MAX_COST_USD == 0.02


def test_checkpoint_is_mutable_across_successive_runs(monkeypatch, tmp_path):
    import config

    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    collector = GlobalEventsCollector()
    collector._save_checkpoints({"standard": "20260901120000"})
    collector._save_checkpoints({"standard": "20260901121500"})
    assert collector._load_checkpoints() == {"standard": "20260901121500"}


def test_run_returns_base_stats_when_disabled(monkeypatch, tmp_path):
    import config

    monkeypatch.setattr(config, "GLOBAL_EVENTS_ENABLED", False)
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    stats = GlobalEventsCollector().run()
    assert "timestamp" in stats
    assert "error" not in stats


def test_handoff_uploads_compact_and_run_manifest_last_not_raw(
    monkeypatch, tmp_path
):
    import config
    import storage.s3

    calls = []

    class FakeS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            calls.append((path.name, key))
            return True

    monkeypatch.setattr(config, "S3_BUCKET", "private-test")
    monkeypatch.setattr(storage.s3, "S3Storage", FakeS3)
    batch = tmp_path / "batch_aaaaaaaaaaaaaaaaaaaaaaaa.json"
    run_manifest = tmp_path / "run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
    batch.write_text("{}", encoding="utf-8")
    run_manifest.write_text('{"schema_version":"global-events-stage1-shadow/v1"}', encoding="utf-8")
    raw = tmp_path / "standard_20260901120000_deadbeef.zip"
    raw.write_bytes(b"raw")
    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    assert collector._upload_handoff(
        batch,
        run_manifest,
        {"batch_id": batch.stem},
        "run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    assert [name for name, _ in calls] == [
        batch.name,
        run_manifest.name,
        f"{batch.stem}.manifest.json",
    ]
    assert not any(name.endswith(".zip") for name, _ in calls)


class _FakeResponse:
    def __init__(self, *, text=None, content=None):
        self.text = text
        self.content = content

    def raise_for_status(self):
        return None


class _FakeGKGSession:
    def __init__(self, payload):
        self.headers = {}
        self.payload = payload

    def get(self, url, timeout):
        if url.endswith("masterfilelist.txt"):
            suffix = ".gkg.csv.zip"
        elif url.endswith("masterfilelist-translation.txt"):
            suffix = ".translation.gkg.csv.zip"
        else:
            return _FakeResponse(content=self.payload)
        name = f"https://data.gdeltproject.org/20260901120000{suffix}"
        text = f"{len(self.payload)} {hashlib.md5(self.payload).hexdigest()} {name}\n"
        return _FakeResponse(text=text)


def _configure_enabled_collector(monkeypatch, tmp_path, payload):
    import config

    monkeypatch.setattr(config, "GLOBAL_EVENTS_ENABLED", True)
    monkeypatch.setattr(config, "SUPABASE_ENABLED", False)
    monkeypatch.setattr(config, "S3_BUCKET", "private-test")
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_PRODUCER_GIT_COMMIT", "a" * 40)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_RAW_RETENTION_HOURS", 72)
    collector = GlobalEventsCollector()
    collector.supabase_writer = type(
        "NoopWriter", (), {"write": lambda self, *args, **kwargs: True}
    )()
    collector._session = _FakeGKGSession(payload)
    return collector


def test_s3_manifest_failure_does_not_advance_checkpoint_or_mark_raw(
    monkeypatch, tmp_path
):
    import storage.s3

    payload = _zip([_row("one.example", "Routine world update")])
    calls = []

    class FailingManifestS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            calls.append(key)
            return "/manifests/" not in key

    monkeypatch.setattr(storage.s3, "S3Storage", FailingManifestS3)
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    result = collector.collect()

    assert "_collector_error" in result
    assert len(result["_supabase_receipts"]) == 1
    run_receipt = result["_supabase_receipts"][0]
    assert run_receipt["_type"] == "collector_run"
    assert run_receipt["status"] == "failed"
    assert run_receipt["batch_id"] is None
    assert run_receipt["archive_eligible"] is False
    assert run_receipt["error_type"] == "handoff_failed"
    assert calls[-1].startswith("global_events/handoff/manifests/")
    assert not (tmp_path / "global_events" / "checkpoint.json").exists()
    raw = list((tmp_path / "global_events" / "raw").glob("**/*.zip"))
    assert len(raw) == 2
    assert not list((tmp_path / "global_events" / "raw").glob("**/*.success"))


def test_s3_manifest_success_advances_both_checkpoints_after_run(
    monkeypatch, tmp_path
):
    import storage.s3

    payload = _zip([_row("one.example", "Routine world update")])
    calls = []

    class SuccessfulS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            calls.append(key)
            return True

    monkeypatch.setattr(storage.s3, "S3Storage", SuccessfulS3)
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    stats = collector.run()

    assert "error" not in stats
    batch_receipt, run_receipt = stats["_supabase_receipts"]
    assert run_receipt["status"] == "accepted"
    assert batch_receipt["archive_eligible"] is True
    assert run_receipt["archive_eligible"] is True
    assert run_receipt["model"] == "qwen/qwen3.7-flash"
    assert run_receipt["raw_response_sha256"] == content_sha256(
        {"assessments": []}
    )
    assert batch_receipt["production_publishable"] is False
    assert run_receipt["production_publishable"] is False
    checkpoint = collector._load_checkpoints()
    assert checkpoint == {
        "standard": "20260901120000",
        "translation": "20260901120000",
    }
    raw = list((tmp_path / "global_events" / "raw").glob("**/*.zip"))
    assert len(raw) == 2
    assert len(list((tmp_path / "global_events" / "raw").glob("**/*.success"))) == 2
    assert any("/runs/run_" in key for key in calls)
    run_files = list((tmp_path / "global_events" / "handoff").glob("**/run_*.json"))
    assert len(run_files) == 1
    run_manifest = json.loads(run_files[0].read_text(encoding="utf-8"))
    assert run_manifest["schema_version"] == "global-events-stage1-shadow/v1"
    assert run_manifest["run_id"].startswith("run_")
    assert run_manifest["input_batch_id"] == batch_receipt["batch_id"]
    assert run_manifest["input_content_sha256"] == batch_receipt["content_sha256"]
    assert run_manifest["archive_eligible"] is True
    assert run_manifest["production_publishable"] is False
    assert all("token" not in key.lower() for key in run_manifest["usage"])
    assert run_manifest["traditional_chinese_gate"] == "canonical_final_passed"
    assert set(run_manifest["result"]) == {"assessments"}
    handoff_manifest = stats["handoff_manifest"]
    batch_file = next(
        file
        for file in (tmp_path / "global_events" / "handoff").glob("**/batch_*.json")
        if not file.name.endswith(".manifest.json")
    )
    assert handoff_manifest["batch_sha256"] == artifact_sha256(batch_file)
    assert handoff_manifest["batch_content_sha256"] == batch_receipt["content_sha256"]
    assert handoff_manifest["run_sha256"] == artifact_sha256(run_files[0])
    assert handoff_manifest["archive_eligible"] is True
    assert handoff_manifest["batch_key"].endswith(batch_file.name)
    assert handoff_manifest["run_key"].endswith(run_files[0].name)
    assert calls[-1].startswith("global_events/handoff/manifests/")


def test_supabase_writer_failed_receipt_does_not_reserve_batch(monkeypatch):
    from contextlib import contextmanager

    import storage.supabase_writer as writer_module
    from storage.supabase_writer import SupabaseWriter

    writer = SupabaseWriter.__new__(SupabaseWriter)
    calls = []

    @contextmanager
    def fake_txn(_conn):
        yield object()

    monkeypatch.setattr(writer, "_txn", fake_txn)
    monkeypatch.setattr(
        writer_module, "execute_values", lambda _cur, sql, values: calls.append((sql, values))
    )
    failed = {
        "_type": "collector_run",
        "run_id": "run_" + "a" * 32,
        "status": "failed",
    }
    writer._write_multi_table(None, "global_events", [failed])
    assert len(calls) == 1
    with pytest.raises(ValueError, match="accepted.*batch"):
        writer._write_multi_table(None, "global_events", [
            {"_type": "collector_run", "status": "accepted"}
        ])


def test_stage1_schema_failure_writes_only_failed_run_and_no_handoff(
    monkeypatch, tmp_path
):
    import storage.s3

    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    calls = []

    class UnexpectedS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            calls.append(key)
            return True

    monkeypatch.setattr(storage.s3, "S3Storage", UnexpectedS3)
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    malformed_content = '{"assessments": ['

    class Stage1Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "finish_reason": "length",
                        "message": {"content": malformed_content},
                    }
                ],
                "usage": {
                    "prompt_tokens": 100,
                    "completion_tokens": 4096,
                    "reasoning_tokens": 512,
                    "cost": 0.01,
                },
            }

    collector._session.post = lambda *args, **kwargs: Stage1Response()
    result = collector.collect()

    assert "_collector_error" in result
    assert len(result["_supabase_receipts"]) == 1
    assert result["_supabase_receipts"][0]["status"] == "failed"
    assert result["_supabase_receipts"][0]["error_type"] == "source_or_stage1_failed"
    assert result["_supabase_receipts"][0]["receipt"]["stage1_observation"] == {
        "finish_reason": "length",
        "content_length": len(malformed_content),
        "usage": {
            "input_units": 100,
            "output_units": 4096,
            "reasoning_units": 512,
            "cost_usd": 0.01,
        },
    }
    assert result["_supabase_receipts"][0]["raw_response_sha256"] == hashlib.sha256(
        malformed_content.encode("utf-8")
    ).hexdigest()
    assert calls == []
    assert not (tmp_path / "global_events" / "checkpoint.json").exists()
    assert not list((tmp_path / "global_events" / "raw").glob("**/*.success"))


def test_translation_404_fails_closed_without_stage1_s3_or_checkpoint(
    monkeypatch, tmp_path
):
    import storage.s3

    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    base_session = collector._session
    openrouter_calls = []

    class NotReadyResponse:
        status_code = 404
        content = b""

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    class Translation404Session:
        headers = {}

        def get(self, url, timeout):
            if url.endswith(".translation.gkg.csv.zip"):
                return NotReadyResponse()
            return base_session.get(url, timeout)

        def post(self, *args, **kwargs):
            openrouter_calls.append((args, kwargs))
            raise AssertionError("OpenRouter must not be called after a source error")

    class UnexpectedS3:
        def __init__(self):
            raise AssertionError("S3 must not be called after a source error")

    monkeypatch.setattr(storage.s3, "S3Storage", UnexpectedS3)
    collector._session = Translation404Session()
    result = collector.collect()

    assert "_collector_error" in result
    assert "translation:" in result["_collector_error"]
    assert "HTTP 404" in result["_collector_error"]
    assert result["health"]["status"] == "ERROR"
    assert openrouter_calls == []
    assert len(result["_supabase_receipts"]) == 1
    receipt = result["_supabase_receipts"][0]
    assert receipt["status"] == "failed"
    assert receipt["error_type"] == "source_or_stage1_failed"
    assert receipt["batch_id"] is None
    assert receipt["output_artifact_sha256"] is None
    assert receipt["raw_response_sha256"] is None
    assert not (tmp_path / "global_events" / "checkpoint.json").exists()
    raw = list((tmp_path / "global_events" / "raw").glob("**/*.zip"))
    assert len(raw) == 1
    assert raw[0].name.startswith("standard_")
    assert not list((tmp_path / "global_events" / "raw").glob("**/*.success"))
