import hashlib
import io
import json
import os
import re
import sys
import types
import zipfile
from datetime import datetime, timezone

import pytest
import requests

from collectors.global_events import (
    GKGArtifact,
    GKGIndexGap,
    GlobalEventsCollector,
    artifact_sha256,
    batch_country_anchors,
    build_compact_batch,
    candidate_display_records,
    content_sha256,
    gazetteer_entries_from_locations,
    gazetteer_lookup,
    headline_gazetteer_places,
    parse_gkg_artifact,
    parse_master_index,
    selected_artifact_manifest,
    title_impact_signals,
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


def _stage1_assessment(candidate, event_group="E001"):
    return {
        "candidate_id": candidate["candidate_id"],
        "candidate_rank": candidate["routing_rank"],
        "decision": "keep_watch",
        "event_group": event_group,
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


class _OpenRouterResponse:
    def __init__(self, status_code, *, retry_after=None, payload=None):
        self.status_code = status_code
        self.headers = {}
        if retry_after is not None:
            self.headers["Retry-After"] = retry_after
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self._payload


def _openrouter_success_payload():
    return {
        "choices": [
            {
                "finish_reason": "stop",
                "message": {"content": '{"assessments": []}'},
            }
        ]
    }


class _OpenRouterSequence:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.responses[len(self.calls) - 1]


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
    assert collector._select_pending(artifacts, None)[1] == []
    selected, skipped = collector._select_pending(artifacts, "20260901114500")
    # A real hole no longer freezes the cursor: it is skipped and reported so
    # the following slot can still be processed.
    assert [item.slot for item in selected] == [
        "20260901120000",
        "20260901123000",
    ]
    assert skipped == ["20260901121500"]
    assert issubclass(GKGIndexGap, RuntimeError)


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
                "choices": [{"finish_reason": "stop", "message": {"content": None}}]
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


def test_openrouter_429_retries_once_then_succeeds(monkeypatch):
    import config

    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    sleeps = []
    monkeypatch.setattr("collectors.global_events.time.sleep", sleeps.append)
    session = _OpenRouterSequence(
        [
            _OpenRouterResponse(429, retry_after="2"),
            _OpenRouterResponse(200, payload=_openrouter_success_payload()),
        ]
    )
    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = session
    monkeypatch.setattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)

    assert collector._request_stage1([]) == {"assessments": []}
    assert len(session.calls) == 2
    assert sleeps == [2.0]
    assert session.calls[0][1]["json"]["model"] == session.calls[1][1]["json"]["model"]


def test_openrouter_non_429_does_not_retry(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    sleeps = []
    monkeypatch.setattr("collectors.global_events.time.sleep", sleeps.append)
    session = _OpenRouterSequence([_OpenRouterResponse(401)])
    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = session

    with pytest.raises(requests.HTTPError):
        collector._request_stage1([])
    assert len(session.calls) == 1
    assert sleeps == []


def test_openrouter_retry_after_is_capped(monkeypatch):
    import config

    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    sleeps = []
    monkeypatch.setattr("collectors.global_events.time.sleep", sleeps.append)
    session = _OpenRouterSequence(
        [
            _OpenRouterResponse(429, retry_after="120"),
            _OpenRouterResponse(200, payload=_openrouter_success_payload()),
        ]
    )
    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = session
    monkeypatch.setattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)

    assert collector._request_stage1([]) == {"assessments": []}
    assert len(session.calls) == 2
    assert sleeps == [30.0]


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
    assert request_content["output_contract"]["traditional_chinese_locale"].startswith(
        "zh-TW"
    )
    assert "不得混入簡體字" in payload["messages"][0]["content"]
    assert (
        request_content["output_contract"]["assessment_additional_properties"] is False
    )
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
    assert (
        collector._raw_response_sha256
        == hashlib.sha256(content.encode("utf-8")).hexdigest()
    )
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
    result = {"assessments": [_stage1_assessment(candidate)]}
    lineage = []
    normalized = validate_stage1(result, [candidate], lineage)
    assert normalized["assessments"][0]["title_zh_tw"] == "臺灣地震"
    assert {entry["field"] for entry in lineage} == {
        "title_zh_tw",
        "taiwan_impact_zh_tw",
    }


def test_stage1_opencc_non_idempotent_dictionary_runs_exactly_once(monkeypatch):
    class FakeOpenCC:
        def __init__(self, _profile):
            pass

        def convert(self, value):
            if "進程" in value:
                return value.replace("進程", "程序")
            if "程序" in value:
                return value.replace("程序", "程式")
            return value

    monkeypatch.setitem(sys.modules, "opencc", types.SimpleNamespace(OpenCC=FakeOpenCC))
    candidate = {"routing_rank": 1, "candidate_id": "cand_" + "a" * 24}
    assessment = _stage1_assessment(candidate)
    assessment["summary_zh_tw"] = "進程仍在進行"
    result = validate_stage1({"assessments": [assessment]}, [candidate])

    assert result["assessments"][0]["summary_zh_tw"] == "程序仍在進行"


def test_official_opencc_s2tw_preserves_canonical_wording():
    from opencc import OpenCC

    converter = OpenCC("s2tw.json")
    assert converter.convert("真實模式與演算法") == "真實模式與演算法"
    assert converter.convert("进程影响台灣") == "進程影響臺灣"


def test_stage1_isolates_one_invalid_candidate_and_keeps_valid_candidate(monkeypatch):
    class FakeOpenCC:
        def __init__(self, _profile):
            pass

        def convert(self, value):
            return value

    monkeypatch.setitem(sys.modules, "opencc", types.SimpleNamespace(OpenCC=FakeOpenCC))
    candidates = [
        {"routing_rank": 1, "candidate_id": "cand_" + "a" * 24},
        {"routing_rank": 2, "candidate_id": "cand_" + "b" * 24},
    ]
    invalid = _stage1_assessment(candidates[0], None)
    valid = _stage1_assessment(candidates[1], "E002")
    rejections = []

    result = validate_stage1(
        {"assessments": [invalid, valid]},
        candidates,
        validation_rejections=rejections,
    )

    assert [item["candidate_id"] for item in result["assessments"]] == [
        candidates[1]["candidate_id"]
    ]
    assert rejections == [
        {
            "candidate_id": candidates[0]["candidate_id"],
            "candidate_rank": 1,
            "error_code": "invalid_assessment",
            "field": "event_group",
            "error": "invalid Stage1 event_group",
        }
    ]


def test_stage1_reconciles_wrong_id_duplicate_unknown_and_omitted(monkeypatch):
    class FakeOpenCC:
        def __init__(self, _profile):
            pass

        def convert(self, value):
            return value

    monkeypatch.setitem(sys.modules, "opencc", types.SimpleNamespace(OpenCC=FakeOpenCC))
    candidates = [
        {"routing_rank": rank, "candidate_id": f"cand_{str(rank) * 24}"}
        for rank in (1, 2, 3)
    ]
    wrong_id = _stage1_assessment(candidates[0])
    wrong_id["candidate_id"] = "cand_" + "9" * 24
    valid = _stage1_assessment(candidates[1], "E002")
    duplicate = _stage1_assessment(candidates[1], "E003")
    unknown = _stage1_assessment(
        {"routing_rank": 99, "candidate_id": "cand_" + "8" * 24}, "E099"
    )
    rejections = []
    diagnostics = []

    result = validate_stage1(
        {"assessments": [wrong_id, valid, duplicate, unknown, "not-an-object"]},
        candidates,
        validation_rejections=rejections,
        validation_diagnostics=diagnostics,
    )

    assert [item["candidate_rank"] for item in result["assessments"]] == [2]
    assert [(item["candidate_rank"], item["error_code"]) for item in rejections] == [
        (1, "invalid_assessment"),
        (3, "omitted_candidate"),
    ]
    assert [item["reported_candidate_rank"] for item in diagnostics] == [2, 99, None]
    assert len(result["assessments"]) + len(rejections) == len(candidates)


@pytest.mark.parametrize("event_group", [None, 123, "", " ", "event-001", "E12"])
def test_stage1_event_group_rejects_non_string_blank_and_invalid_values(
    monkeypatch, event_group
):
    class FakeOpenCC:
        def __init__(self, _profile):
            pass

        def convert(self, value):
            return value

    monkeypatch.setitem(sys.modules, "opencc", types.SimpleNamespace(OpenCC=FakeOpenCC))
    candidate = {"routing_rank": 1, "candidate_id": "cand_" + "a" * 24}
    result = {"assessments": [_stage1_assessment(candidate, event_group)]}

    with pytest.raises(ValueError, match="invalid Stage1 event_group"):
        validate_stage1(result, [candidate])


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
    assert config.GLOBAL_EVENTS_QWEN_MAX_CANDIDATES == 100
    assert config.GLOBAL_EVENTS_QWEN_CHUNK_SIZE == 10
    assert config.GLOBAL_EVENTS_QWEN_MAX_OUTPUT_TOKENS == 8192
    assert config.GLOBAL_EVENTS_QWEN_MAX_COST_USD == 0.02


def test_registry_preflight_requires_openrouter_key(monkeypatch, capsys):
    import importlib
    import config
    from collectors.registry import get_entry_by_name
    from main import _init_collector_from_entry

    entry = get_entry_by_name("global_events")
    assert entry is not None

    with monkeypatch.context() as env:
        env.setenv("GLOBAL_EVENTS_ENABLED", "true")
        env.setenv("OPENROUTER_API_KEY", "fixture-secret-never-log")
        importlib.reload(config)
        assert isinstance(
            _init_collector_from_entry(entry, first=False),
            GlobalEventsCollector,
        )
        assert "fixture-secret-never-log" not in capsys.readouterr().out

    with monkeypatch.context() as env:
        env.setenv("GLOBAL_EVENTS_ENABLED", "true")
        env.delenv("OPENROUTER_API_KEY", raising=False)
        importlib.reload(config)
        assert _init_collector_from_entry(entry, first=False) is None
        output = capsys.readouterr().out
        assert "OPENROUTER_API_KEY 未設定" in output
        assert "fixture-secret-never-log" not in output

    importlib.reload(config)


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


def test_handoff_uploads_compact_and_run_manifest_last_not_raw(monkeypatch, tmp_path):
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
    run_manifest.write_text(
        '{"schema_version":"global-events-stage1-shadow/v1"}', encoding="utf-8"
    )
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
        f"{batch.stem}.{run_manifest.stem}.manifest.json",
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


def test_s3_manifest_success_advances_both_checkpoints_after_run(monkeypatch, tmp_path):
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
    assert run_receipt["raw_response_sha256"] == content_sha256({"assessments": []})
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
    assert run_manifest["schema_version"] == "global-events-stage1-shadow/v3"
    assert run_manifest["run_id"].startswith("run_")
    assert run_manifest["input_batch_id"] == batch_receipt["batch_id"]
    assert run_manifest["input_content_sha256"] == batch_receipt["content_sha256"]
    assert run_manifest["archive_eligible"] is True
    assert run_manifest["production_publishable"] is False
    assert all("token" not in key.lower() for key in run_manifest["usage"])
    assert run_manifest["traditional_chinese_gate"] == "canonical_all_passed"
    assert run_manifest["validation_status"] == "accepted_all"
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
        writer_module,
        "execute_values",
        lambda _cur, sql, values: calls.append((sql, values)),
    )
    failed = {
        "_type": "collector_run",
        "run_id": "run_" + "a" * 32,
        "status": "failed",
    }
    writer._write_multi_table(None, "global_events", [failed])
    assert len(calls) == 1
    with pytest.raises(ValueError, match="accepted.*batch"):
        writer._write_multi_table(
            None, "global_events", [{"_type": "collector_run", "status": "accepted"}]
        )


def test_stage1_schema_failure_writes_only_failed_run_and_no_handoff(
    monkeypatch, tmp_path
):
    import storage.s3

    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    calls = []

    class RecordingS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            calls.append(key)
            return True

    monkeypatch.setattr(storage.s3, "S3Storage", RecordingS3)
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
    result = collector.run()

    # A truncated provider response is now the chunk's problem, not the round's:
    # the candidate ships as pending and the source cursor still advances.
    assert "error" not in result
    batch_receipt, run_receipt = result["_supabase_receipts"]
    assert run_receipt["status"] == "accepted"
    assert run_receipt["error_type"] is None
    assert run_receipt["error_message"] is None
    assert batch_receipt["archive_eligible"] is True
    chunks = run_receipt["receipt"]["stage1_chunks"]
    assert chunks["failed_chunk_count"] == 1
    assert chunks["content_failure_count"] == 1
    assert chunks["failed_candidate_count"] == 1
    failure = run_receipt["receipt"]["stage1_observation"]["chunk_failures"][0]
    assert failure["kind"] == "content"
    assert failure["observation"] == {
        "finish_reason": "length",
        "content_length": len(malformed_content),
        "usage": {
            "input_units": 100,
            "output_units": 4096,
            "reasoning_units": 512,
            "cost_usd": 0.01,
        },
    }
    assert [record["assessment_status"] for record in result["_candidate_display_records"]] == [
        "pending"
    ]
    assert calls[-1].startswith("global_events/handoff/manifests/")
    assert collector._load_checkpoints() == {
        "standard": "20260901120000",
        "translation": "20260901120000",
    }
    # The refused cohort stays queued with one attempt spent.
    queued = json.loads(collector.routing_pending_path.read_text())
    assert queued["version"] == 2
    assert [state["attempts"] for state in queued["queue_state"].values()] == [1]


def test_stage1_all_rejected_still_archives_and_advances_checkpoint(
    monkeypatch, tmp_path
):
    import storage.s3

    class FakeOpenCC:
        def __init__(self, _profile):
            pass

        def convert(self, value):
            return value

    monkeypatch.setitem(sys.modules, "opencc", types.SimpleNamespace(OpenCC=FakeOpenCC))
    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    calls = []

    class SuccessfulS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            calls.append(key)
            return True

    monkeypatch.setattr(storage.s3, "S3Storage", SuccessfulS3)
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)

    def null_event_group(candidates):
        return {"assessments": [_stage1_assessment(candidates[0], None)]}

    monkeypatch.setattr(collector, "_request_stage1", null_event_group)
    result = collector.run()

    assert "error" not in result
    batch_receipt, run_receipt = result["_supabase_receipts"]
    assert run_receipt["status"] == "accepted"
    assert run_receipt["batch_id"] == batch_receipt["batch_id"]
    assert run_receipt["archive_eligible"] is True
    assert collector._load_checkpoints() == {
        "standard": "20260901120000",
        "translation": "20260901120000",
    }
    run_file = next((tmp_path / "global_events" / "handoff").glob("**/run_*.json"))
    run_manifest = json.loads(run_file.read_text(encoding="utf-8"))
    assert run_manifest["result"] == {"assessments": []}
    assert run_manifest["valid_assessment_count"] == 0
    assert run_manifest["rejected_assessment_count"] == 1
    assert run_manifest["validation_status"] == "accepted_all_rejected"
    assert run_manifest["traditional_chinese_gate"] == "canonical_survivors_passed"
    assert (
        run_manifest["valid_assessment_count"]
        + run_manifest["rejected_assessment_count"]
        == run_manifest["candidate_count"]
    )
    assert len(run_manifest["validation_rejections"]) == 1
    rejection = run_manifest["validation_rejections"][0]
    assert rejection["candidate_id"].startswith("cand_")
    assert rejection["candidate_rank"] == 1
    assert rejection["error_code"] == "invalid_assessment"
    assert rejection["field"] == "event_group"
    assert rejection["error"] == "invalid Stage1 event_group"
    assert calls[-1].startswith("global_events/handoff/manifests/")
    assert len(list((tmp_path / "global_events" / "raw").glob("**/*.success"))) == 2


def test_openrouter_repeated_429_is_a_provider_outage_that_costs_no_attempt(
    monkeypatch, tmp_path
):
    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    sleeps = []
    calls = []
    monkeypatch.setattr("collectors.global_events.time.sleep", sleeps.append)

    def rate_limited(*args, **kwargs):
        calls.append((args, kwargs))
        return _OpenRouterResponse(429)

    collector._session.post = rate_limited
    result = collector.run()

    assert len(calls) == 3
    assert sleeps == [5.0, 5.0]
    # An OpenRouter incident must not burn the cohort's attempt budget, or a
    # provider outage would release the whole queue as unassessed.
    assert "error" not in result
    chunks = result["stage1_chunks"]
    assert chunks["provider_failure_count"] == 1
    assert chunks["content_failure_count"] == 0
    queued = json.loads(collector.routing_pending_path.read_text())
    assert [state["attempts"] for state in queued["queue_state"].values()] == [0]


def test_translation_404_is_stream_scoped_and_standard_still_advances(
    monkeypatch, tmp_path
):
    import config
    import storage.s3

    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    base_session = collector._session

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


    class SuccessfulS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            return True

    monkeypatch.setattr(storage.s3, "S3Storage", SuccessfulS3)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_ARTIFACT_STALE_HOURS", 1_000_000)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    collector._session = Translation404Session()
    result = collector.run()

    # One stream being unavailable is no longer allowed to hold the other one
    # hostage: standard ships and advances, translation simply retries.
    assert "error" not in result
    batch_receipt, run_receipt = result["_supabase_receipts"]
    assert run_receipt["status"] == "accepted"
    assert batch_receipt["archive_eligible"] is True
    streams = run_receipt["receipt"]["streams"]
    assert "artifact_unavailable" in streams["translation"]
    assert "HTTP 404" in streams["translation"]["artifact_unavailable"]
    assert "error" not in streams["standard"]
    assert collector._load_checkpoints() == {"standard": "20260901120000"}
    raw = list((tmp_path / "global_events" / "raw").glob("**/*.zip"))
    assert len(raw) == 1
    assert raw[0].name.startswith("standard_")
    assert len(list((tmp_path / "global_events" / "raw").glob("**/*.success"))) == 1


def _fake_assessments(collector, candidates, *, decision="drop_noise"):
    assessments = [_stage1_assessment(candidate) for candidate in candidates]
    for assessment in assessments:
        assessment["decision"] = decision
        assessment["taiwan_relationship"] = "none"
    result = {"assessments": assessments}
    collector._raw_response_sha256 = content_sha256(result)
    return result


def test_hundred_drop_noise_candidates_are_retained_and_overflow_drains_first(
    monkeypatch, tmp_path
):
    payload = _zip(
        [
            _row(f"news{index}.example", f"Major earthquake kills dozens {index}")
            for index in range(101)
        ]
    )
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    calls = []

    def assess(candidates):
        calls.append([candidate["candidate_id"] for candidate in candidates])
        return _fake_assessments(collector, candidates)

    monkeypatch.setattr(collector, "_request_stage1", assess)
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    first = collector.run()
    assert "error" not in first
    assert first["candidate_count"] == 100
    assert first["deferred_candidate_count"] == 1
    assert len(calls) == 10
    assert all(len(chunk) == 10 for chunk in calls)
    assert len(first["_candidate_display_records"]) == 100
    assert all(
        record["decision"] == "drop_noise" and record["taiwan_relationship"] == "none"
        for record in first["_candidate_display_records"]
    )
    queued = json.loads(collector.routing_pending_path.read_text())
    assert len(queued["candidates"]) == 1
    # The source cursor is no longer hostage to the assessment queue: the files
    # were fetched, parsed and durably queued, so they are done.
    assert collector._load_checkpoints() == {
        "standard": "20260901120000",
        "translation": "20260901120000",
    }
    assert len(list((tmp_path / "global_events" / "raw").glob("**/*.success"))) == 2

    refetches = []
    base_get = collector._session.get

    def counted(url, timeout):
        refetches.append(url)
        return base_get(url, timeout)

    collector._session.get = counted
    second = collector.run()
    assert "error" not in second
    # Every round looks for new slots; the queue no longer suppresses the fetch.
    assert any(url.endswith("masterfilelist.txt") for url in refetches)
    assert second["candidate_count"] == 1
    assert second["deferred_candidate_count"] == 0
    assert len(calls) == 11
    assert len({candidate_id for chunk in calls for candidate_id in chunk}) == 101
    assert not collector.routing_pending_path.exists()


def test_failed_model_chunk_reuses_prior_complete_output_without_losing_queue(
    monkeypatch, tmp_path
):
    payload = _zip(
        [
            _row(f"news{index}.example", f"Major earthquake kills dozens {index}")
            for index in range(21)
        ]
    )
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    calls = []

    def assess(candidates):
        calls.append([candidate["candidate_id"] for candidate in candidates])
        if len(calls) == 2:
            raise ValueError("provider incomplete response")
        return _fake_assessments(collector, candidates)

    monkeypatch.setattr(collector, "_request_stage1", assess)
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    first = collector.run()
    # One refused chunk no longer fails the round: the other 11 candidates ship
    # and only the refused cohort is requeued.
    assert "error" not in first
    assert first["stage1_chunks"]["failed_chunk_count"] == 1
    assert collector._load_checkpoints() == {
        "standard": "20260901120000",
        "translation": "20260901120000",
    }
    queued = json.loads(collector.routing_pending_path.read_text())
    assert len(queued["candidates"]) == 10
    assert {state["attempts"] for state in queued["queue_state"].values()} == {1}
    result = collector.run()
    assert "error" not in result
    assert len(calls) == 4  # 3 chunks, then only the requeued cohort.
    assert calls[1] == calls[3]
    assert len(result["_candidate_display_records"]) == 10
    run_files = sorted(
        (tmp_path / "global_events" / "handoff").glob("**/run_*.json"),
        key=lambda path: path.stat().st_mtime,
    )
    first_run = json.loads(run_files[0].read_text())
    # Separate Qwen requests cannot accidentally share an event_group.
    assert len({item["event_group"] for item in first_run["result"]["assessments"]}) == 2
    assert first_run["unassessed_candidate_count"] == 10
    assert (
        first_run["valid_assessment_count"]
        + first_run["rejected_assessment_count"]
        + first_run["unassessed_candidate_count"]
        == first_run["candidate_count"]
    )


def test_candidate_location_requires_selected_evidence_and_literal_source_basis(
    monkeypatch, tmp_path
):
    row = _row("one.example", "Major earthquake in Iran kills dozens")
    row[9] = "1#Iran#IR##32#54#IR;1#France#FR##46#2#FR"
    collector = _configure_enabled_collector(monkeypatch, tmp_path, _zip([row]))
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)

    def assess(candidates):
        result = _fake_assessments(collector, candidates)
        evidence = candidates[0]["location_evidence"]
        assert {item["name"] for item in evidence} == {"Iran", "France"}
        iran = next(item for item in evidence if item["name"] == "Iran")
        result["assessments"][0]["location_evidence_ids"] = [
            {
                "evidence_id": iran["evidence_id"],
                "role": "event_location",
                "basis": "earthquake in Iran",
            }
        ]
        return result

    monkeypatch.setattr(collector, "_request_stage1", assess)
    result = collector.run()
    record = result["_candidate_display_records"][0]
    assert record["decision"] == "drop_noise"
    assert record["assessment_status"] == "assessed"
    assert len(record["places"]) == 1
    place = record["places"][0]
    assert place["name"] == "Iran"
    assert place["location_kind"] == "country_center"
    assert place["country_code_scheme"] == "fips10"
    assert (place["longitude"], place["latitude"]) == (54, 32)
    assert place["evidence_basis"] == "earthquake in Iran"
    assert place["source_kind"] == "gdelt_metadata_mention"
    assert ":gdelt:metadata_fallback_" not in place["location_lineage"]
    assert set(record) == {
        "candidate_id",
        "observed_at",
        "assessed_at",
        "assessment_status",
        "ai_group_id",
        "source_urls",
        "source_headline",
        "places",
        "title_zh_tw",
        "summary_zh_tw",
        "category",
        "severity",
        "decision",
        "taiwan_relationship",
        "taiwan_impact_zh_tw",
        "confidence",
        "reason_zh_tw",
    }


def test_unselected_source_locations_are_explicit_approximate_fallback(
    monkeypatch, tmp_path
):
    row = _row("one.example", "Major earthquake kills dozens")
    row[9] = (
        "1#Iran#IR##32#54#IR;1#Iran#IR##32#54#IR;"
        "4#Tehran#IR##35.69#51.39#112931;2#State#US##30#40#state;"
        "4#Invalid#IR##95#200#bad"
    )
    collector = _configure_enabled_collector(monkeypatch, tmp_path, _zip([row]))
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    record = collector.run()["_candidate_display_records"][0]
    assert record["decision"] == "drop_noise"
    assert record["taiwan_relationship"] == "none"
    assert [(p["name"], p["longitude"], p["latitude"]) for p in record["places"]] == [
        ("Iran", 54, 32),
        ("Tehran", 51.39, 35.69),
    ]
    for place in record["places"]:
        assert ":gdelt:metadata_fallback_loc_" in place["location_lineage"]
        assert place["source_kind"] == "gdelt_metadata_mention"
        assert "未確認為精確發生地" in place["evidence_basis"]
        assert place["evidence_url"] in record["source_urls"]


def test_missing_source_locations_stay_unlocated(monkeypatch, tmp_path):
    row = _row("one.example", "Major earthquake in Iran kills dozens")
    row[9] = ""
    collector = _configure_enabled_collector(monkeypatch, tmp_path, _zip([row]))
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    record = collector.run()["_candidate_display_records"][0]
    assert record["places"] == []  # A country in the title is not invented geometry.


@pytest.mark.parametrize(
    "selection",
    [
        {
            "evidence_id": "loc_invented",
            "role": "event_location",
            "basis": "earthquake",
        },
        {
            "evidence_id": "loc_known",
            "role": "event_location",
            "basis": "fabricated quote",
        },
        {
            "evidence_id": "loc_known",
            "role": "speaker_nationality",
            "basis": "earthquake",
        },
        {
            "evidence_id": "loc_known",
            "role": "event_location",
            "basis": "earthquake",
            "longitude": 1,
        },
    ],
)
def test_invalid_optional_location_preserves_assessment_not_a_lost_candidate(
    monkeypatch, tmp_path, selection
):
    collector = _configure_enabled_collector(
        monkeypatch,
        tmp_path,
        _zip([_row("one.example", "Major earthquake kills dozens")]),
    )
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)

    def assess(candidates):
        result = _fake_assessments(collector, candidates)
        selected = dict(selection)
        if selected["evidence_id"] == "loc_known":
            selected["evidence_id"] = candidates[0]["location_evidence"][0][
                "evidence_id"
            ]
        result["assessments"][0]["location_evidence_ids"] = [selected]
        return result

    monkeypatch.setattr(collector, "_request_stage1", assess)
    result = collector.run()
    assert "error" not in result
    assert collector._load_checkpoints()
    record = result["_candidate_display_records"][0]
    assert record["assessment_status"] == "assessed"
    assert record["decision"] == "drop_noise"
    assert record["source_headline"] == "Major earthquake kills dozens"
    assert len(record["places"]) == 1
    assert ":gdelt:metadata_fallback_" in record["places"][0]["location_lineage"]


@pytest.mark.parametrize("relationship", ["none", "direct", "indirect", "unknown"])
def test_blank_taiwan_impact_only_normalizes_explicit_none(relationship):
    candidate = {"candidate_id": "cand_" + "a" * 24, "routing_rank": 1}
    assessment = _stage1_assessment(candidate)
    assessment.update(taiwan_relationship=relationship, taiwan_impact_zh_tw="")
    lineage, rejected = [], []
    result = validate_stage1(
        {"assessments": [assessment]}, [candidate], lineage, rejected, []
    )
    if relationship == "none":
        assert (
            result["assessments"][0]["taiwan_impact_zh_tw"]
            == "模型判斷無臺灣關聯，未提供補充說明。"
        )
        assert {
            "candidate_id": candidate["candidate_id"],
            "field": "taiwan_impact_zh_tw",
        } in lineage
        assert not rejected
    else:
        assert not result["assessments"]
        assert rejected[0]["field"] == "taiwan_impact_zh_tw"


def test_missing_rank_recovers_only_exact_input_candidate_id():
    candidate = {"candidate_id": "cand_" + "a" * 24, "routing_rank": 17}
    assessment = _stage1_assessment(candidate)
    del assessment["candidate_rank"]
    diagnostics = []
    result = validate_stage1(
        {"assessments": [assessment]}, [candidate], [], [], diagnostics
    )
    assert result["assessments"][0]["candidate_rank"] == 17
    assert diagnostics[0]["reported_candidate_rank"] is None
    assessment = _stage1_assessment(candidate)
    del assessment["candidate_rank"]
    assessment["candidate_id"] = "cand_" + "b" * 24
    rejected = []
    result = validate_stage1(
        {"assessments": [assessment]}, [candidate], [], rejected, []
    )
    assert result["assessments"] == []
    assert rejected[0]["error_code"] == "omitted_candidate"


def test_cached_maintenance_replay_preserves_provider_times_and_source_checkpoint(
    monkeypatch, tmp_path
):
    from scripts.replay_global_event_assessments import prepare_replay, apply_replay

    collector = _configure_enabled_collector(
        monkeypatch,
        tmp_path,
        _zip([_row("one.example", "Major earthquake kills dozens")]),
    )
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    stats = collector.run()
    source = next((tmp_path / "global_events").glob("20*/*/*/global_events_*.json"))
    # Test upload stub skips the commit marker but not the immutable source run.
    output = json.loads(source.read_text())
    source_manifest = output["handoff_manifest"]
    checkpoint_before = collector.checkpoint_path.read_bytes()
    plan = prepare_replay(
        source, source_manifest["batch_id"], source_manifest["run_id"]
    )
    assert plan["run"]["stage1_observation"]["provider_called"] is False
    assert plan["run"]["usage"]["cost_usd"] == 0
    assert (
        plan["output"]["_candidate_display_records"]
        == stats["_candidate_display_records"]
    )
    assert collector.checkpoint_path.read_bytes() == checkpoint_before
    assert not collector.routing_pending_path.exists()
    writes = []

    class Writer:
        def write(self, name, payload, timestamp):
            writes.append(payload)
            return True

    monkeypatch.setattr(GlobalEventsCollector, "_upload_handoff", lambda *args: True)
    apply_replay(plan, Writer())
    retry = prepare_replay(
        source, source_manifest["batch_id"], source_manifest["run_id"]
    )
    assert retry == plan
    apply_replay(retry, Writer())
    assert writes[0] == writes[1]
    assert collector.checkpoint_path.read_bytes() == checkpoint_before
    assert (
        len(list((tmp_path / "global_events" / "handoff").glob("**/run_*.json"))) == 2
    )
    # A missing cached response fails locally; it cannot call a provider.
    cache_path = next((tmp_path / "global_events" / "stage1_cache").glob("*.json"))
    cache_path.unlink()
    with pytest.raises(FileNotFoundError):
        prepare_replay(source, source_manifest["batch_id"], source_manifest["run_id"])


def test_db_failure_retries_same_durable_batch_with_new_manifest_marker(
    monkeypatch, tmp_path
):
    import storage.s3

    uploads = []

    class SuccessfulS3:
        def upload_file(self, path, key):
            uploads.append(key)
            return True

    monkeypatch.setattr(storage.s3, "S3Storage", SuccessfulS3)
    collector = _configure_enabled_collector(
        monkeypatch,
        tmp_path,
        _zip([_row("one.example", "Major earthquake kills dozens")]),
    )
    requests_made = []

    def assess(candidates):
        requests_made.append(candidates)
        return _fake_assessments(collector, candidates)

    monkeypatch.setattr(collector, "_request_stage1", assess)
    written_records = []

    def failing_write(_name, result, _timestamp):
        written_records.append(result["_candidate_display_records"])
        return False

    collector.supabase_writer.write = failing_write
    assert "error" in collector.run()
    assert not collector._load_checkpoints()
    assert collector.routing_pending_path.exists()
    collector.supabase_writer.write = lambda *args: True
    result = collector.run()
    assert "error" not in result
    assert len(requests_made) == 1
    assert result["_candidate_display_records"] == written_records[0]
    markers = [key for key in uploads if "/manifests/" in key]
    assert len(markers) == 2 and markers[0] != markers[1]
    assert collector._load_checkpoints()


def test_oversized_assessment_is_pending_while_other_candidate_advances(
    monkeypatch, tmp_path
):
    collector = _configure_enabled_collector(
        monkeypatch,
        tmp_path,
        _zip(
            [
                _row("one.example", "Major earthquake kills dozens one"),
                _row("two.example", "Major earthquake kills dozens two"),
            ]
        ),
    )
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)

    def assess(candidates):
        result = _fake_assessments(collector, candidates)
        result["assessments"][0]["summary_zh_tw"] = "訊息" * 1501
        return result

    monkeypatch.setattr(collector, "_request_stage1", assess)
    result = collector.run()
    assert "error" not in result
    assert [
        record["assessment_status"] for record in result["_candidate_display_records"]
    ] == ["pending", "assessed"]
    assert collector._load_checkpoints()


def test_oversized_source_url_stays_private_and_public_candidate_is_pending(
    monkeypatch, tmp_path
):
    long_row = _row("one.example", "Major earthquake kills dozens one")
    long_url = "https://one.example/" + "a" * 8192
    long_row[4] = long_url
    collector = _configure_enabled_collector(
        monkeypatch,
        tmp_path,
        _zip(
            [
                long_row,
                _row("two.example", "Major earthquake kills dozens two"),
            ]
        ),
    )
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    result = collector.run()
    assert "error" not in result
    records = result["_candidate_display_records"]
    assert sorted(record["assessment_status"] for record in records) == [
        "assessed",
        "pending",
    ]
    pending = next(
        record for record in records if record["assessment_status"] == "pending"
    )
    assert pending["source_urls"] == [] and pending["places"] == []
    assert pending["decision"] is None
    batch_file = next(
        path
        for path in (tmp_path / "global_events/handoff").glob("**/batch_*.json")
        if not path.name.endswith(".manifest.json")
    )
    batch = json.loads(batch_file.read_text())
    assert any(
        document["url"] == long_url
        for candidate in batch["payload"]["candidates"]
        for document in candidate["representative_documents"]
    )
    assert collector._load_checkpoints()


def test_supabase_candidate_ingestion_is_in_receipt_transaction_and_rejects_failed_run(
    monkeypatch,
):
    from contextlib import contextmanager
    from storage.supabase_writer import SupabaseWriter
    import storage.supabase_writer as writer_module

    calls = []

    class Cursor:
        def execute(self, sql, args):
            calls.append((sql, args[0].adapted))

    @contextmanager
    def txn(_conn):
        yield Cursor()

    writer = SupabaseWriter.__new__(SupabaseWriter)
    monkeypatch.setattr(writer, "_txn", txn)
    monkeypatch.setattr(writer_module, "execute_values", lambda *args: None)
    candidate = {"candidate_id": "cand_" + "a" * 24, "decision": "drop_noise"}
    records = [
        {
            "_type": "collector_run",
            "run_id": "run_" + "a" * 32,
            "status": "accepted",
            "archive_eligible": True,
        },
        {
            "_type": "collector_batch",
            "batch_id": "batch_" + "a" * 24,
            "archive_eligible": True,
        },
        {"_type": "candidate_display", "candidate": candidate},
    ]
    writer._write_multi_table(None, "global_events", records)
    assert calls == [
        ("SELECT public.ingest_global_event_candidates(%s::jsonb)", [candidate])
    ]
    with pytest.raises(ValueError, match="failed.*candidate"):
        writer._write_multi_table(
            None,
            "global_events",
            [
                {"_type": "collector_run", "status": "failed"},
                {"_type": "candidate_display", "candidate": candidate},
            ],
        )


# ──────────────────────────────────────────────────────────────────────
# 2026-09-05 停滯修復回歸測試（R1–R11）
# ──────────────────────────────────────────────────────────────────────


def test_index_get_retries_transient_5xx_then_succeeds(monkeypatch):
    """R1：非 429 的暫時性錯誤以前完全不重試，一次 502 就讓整輪 failed。"""
    sleeps = []
    monkeypatch.setattr("collectors.global_events.time.sleep", sleeps.append)
    responses = [
        _OpenRouterResponse(502),
        _OpenRouterResponse(503),
        _FakeResponse(text="ok"),
    ]
    calls = []

    class Flaky:
        def get(self, url, timeout):
            calls.append(url)
            return responses[len(calls) - 1]

    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = Flaky()
    assert collector._get_with_retry("https://example/index", timeout=5).text == "ok"
    assert len(calls) == 3
    assert sleeps == [2.0, 4.0]


def test_index_get_does_not_retry_deterministic_client_error(monkeypatch):
    """R1：401/404 重試只是浪費預算，必須立刻放棄。"""
    sleeps = []
    monkeypatch.setattr("collectors.global_events.time.sleep", sleeps.append)
    calls = []

    class Forbidden:
        def get(self, url, timeout):
            calls.append(url)
            return _OpenRouterResponse(403)

    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = Forbidden()
    with pytest.raises(requests.HTTPError):
        collector._get_with_retry("https://example/index", timeout=5)
    assert len(calls) == 1
    assert sleeps == []


def test_openrouter_retries_timeout_then_succeeds(monkeypatch):
    """R1：連線 timeout 以前零重試。"""
    import config

    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    sleeps = []
    monkeypatch.setattr("collectors.global_events.time.sleep", sleeps.append)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)
    calls = []

    class FlakySession:
        def post(self, *args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise requests.Timeout("read timed out")
            return _OpenRouterResponse(200, payload=_openrouter_success_payload())

    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = FlakySession()
    assert collector._request_stage1([]) == {"assessments": []}
    assert len(calls) == 2
    assert sleeps == [2.0]


def test_fallback_models_are_opt_in_and_absent_by_default(monkeypatch):
    """R9：預設 request body 必須逐位元不變，否則 stage1 cache key 會全部失效。"""
    import config

    monkeypatch.setenv("OPENROUTER_API_KEY", "fixture-key")
    monkeypatch.setattr(config, "GLOBAL_EVENTS_QWEN_MAX_COST_USD", 0.02)
    session = _OpenRouterSequence(
        [_OpenRouterResponse(200, payload=_openrouter_success_payload())] * 2
    )
    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    collector._session = session

    monkeypatch.setattr(config, "GLOBAL_EVENTS_QWEN_FALLBACK_MODELS", "")
    collector._request_stage1([])
    assert "models" not in session.calls[0][1]["json"]

    monkeypatch.setattr(
        config, "GLOBAL_EVENTS_QWEN_FALLBACK_MODELS", " openai/gpt-oss , "
    )
    collector._request_stage1([])
    assert session.calls[1][1]["json"]["models"] == [
        "qwen/qwen3.7-flash",
        "openai/gpt-oss",
    ]


def test_chunk_size_backs_off_then_releases():
    """R2：同一 chunk 連續失敗 → 10→5→2 → 放行。"""
    from collectors.global_events import (
        STAGE1_CHUNK_RELEASE_ATTEMPTS,
        stage1_chunk_size,
    )

    assert [stage1_chunk_size(attempts, 10) for attempts in range(4)] == [10, 10, 5, 2]
    assert STAGE1_CHUNK_RELEASE_ATTEMPTS == 4


def test_repeatedly_refused_chunk_is_split_then_released_as_pending(
    monkeypatch, tmp_path
):
    """R2：這正是停滯現場 —— 同一批候選每小時撞同一個 finish_reason='error'。"""
    payload = _zip(
        [
            _row(f"news{index}.example", f"Major earthquake kills dozens {index}")
            for index in range(4)
        ]
    )
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    sizes = []

    def always_refused(candidates):
        sizes.append(len(candidates))
        raise ValueError(
            "OpenRouter Stage1 incomplete response: finish_reason='error'"
        )

    monkeypatch.setattr(collector, "_request_stage1", always_refused)
    monkeypatch.setattr(config_module(), "GLOBAL_EVENTS_QWEN_CHUNK_SIZE", 4)

    for round_index in range(4):
        result = collector.run()
        assert "error" not in result, f"round {round_index} must stay accepted"
        assert all(
            record["assessment_status"] == "pending"
            for record in result["_candidate_display_records"]
        )
    # 4 → 4 → 2+2 → 1+1+1+1：拆半階梯確實生效
    assert sizes == [4, 4, 2, 2, 1, 1, 1, 1]
    # 累積 4 次後放行，佇列清空，不再無限重送
    assert not collector.routing_pending_path.exists()
    assert result["handoff_manifest"]["routing"]["released_pending_count"] == 4


def config_module():
    import config

    return config


def test_expired_queue_entries_are_reported_not_resent(monkeypatch, tmp_path):
    """R3：48h TTL —— 過期候選只記 receipt，不再佔用 LLM 預算。"""
    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)

    def refuse(candidates):
        raise ValueError("provider refused")

    monkeypatch.setattr(collector, "_request_stage1", refuse)
    assert "error" not in collector.run()
    queued = json.loads(collector.routing_pending_path.read_text())
    assert len(queued["candidates"]) == 1

    stale = "2026-01-01T00:00:00+00:00"
    for state in queued["queue_state"].values():
        state["queued_at"] = stale
    collector.routing_pending_path.write_text(json.dumps(queued), encoding="utf-8")

    sent = []
    monkeypatch.setattr(
        collector, "_request_stage1", lambda candidates: sent.append(candidates)
    )
    result = collector.run()
    routing = result["handoff_manifest"]["routing"]
    assert routing["expired_count"] == 1
    assert routing["expired_sample"] == sorted(queued["queue_state"])
    assert routing["pending_ttl_hours"] == 48
    assert sent == []


def test_catchup_widens_the_window_only_when_behind(monkeypatch):
    """R6：每輪 8 檔 = 2 小時，落後 29 小時永遠追不回來。"""
    import config
    from datetime import datetime, timedelta, timezone

    monkeypatch.setattr(config, "GLOBAL_EVENTS_MAX_FILES_PER_STREAM", 8)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_CATCHUP_FILES_PER_STREAM", 24)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_CATCHUP_LAG_HOURS", 6)
    collector = GlobalEventsCollector.__new__(GlobalEventsCollector)
    now = datetime.now(timezone.utc)
    recent = (now - timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
    behind = (now - timedelta(hours=29)).strftime("%Y%m%d%H%M%S")

    assert collector._max_files_per_stream(None) == 8
    assert collector._max_files_per_stream(recent) == 8
    assert collector._max_files_per_stream(behind) == 24


def test_long_unavailable_artifact_is_skipped_so_the_cursor_moves(
    monkeypatch, tmp_path
):
    """R5：index 有列、ZIP 永遠 404 的 slot 不得讓 stream 永久卡住。"""
    import config
    import storage.s3

    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    base_session = collector._session

    class NotReadyResponse:
        status_code = 404
        content = b""

        def raise_for_status(self):
            raise requests.HTTPError(response=self)

    class AllZipsMissing:
        headers = {}

        def get(self, url, timeout):
            if url.endswith(".zip"):
                return NotReadyResponse()
            return base_session.get(url, timeout)

    class SuccessfulS3:
        def __init__(self):
            self.bucket = "private-test"

        def upload_file(self, path, key):
            return True

    monkeypatch.setattr(storage.s3, "S3Storage", SuccessfulS3)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_ARTIFACT_STALE_HOURS", 1)
    collector._session = AllZipsMissing()
    result = collector.run()

    assert "error" not in result
    assert collector._load_checkpoints() == {
        "standard": "20260901120000",
        "translation": "20260901120000",
    }
    assert result["streams"]["standard"]["skipped_slots"] == ["20260901120000"]


def test_total_source_outage_still_fails_closed(monkeypatch, tmp_path):
    """R4 的界線：解耦不等於靜音 —— 兩條 stream 全掛且無佇列時仍要 failed receipt。"""
    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.setattr("collectors.global_events.time.sleep", lambda _: None)

    class Down:
        headers = {}

        def get(self, url, timeout):
            raise requests.ConnectionError("dns failure")

    collector._session = Down()
    result = collector.collect()

    assert "_collector_error" in result
    assert len(result["_supabase_receipts"]) == 1
    receipt = result["_supabase_receipts"][0]
    assert receipt["status"] == "failed"
    assert receipt["error_type"] == "source_or_stage1_failed"
    assert "standard:" in receipt["error_message"]
    assert "translation:" in receipt["error_message"]


def test_missing_api_key_fails_the_round_instead_of_releasing_the_queue(
    monkeypatch, tmp_path
):
    """R2 的界線：設定錯誤不得被當成 chunk 失敗，否則 4 輪後整個佇列被無評估放行。"""
    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    result = collector.collect()

    assert "_collector_error" in result
    assert "OPENROUTER_API_KEY" in result["_collector_error"]
    assert result["_supabase_receipts"][0]["status"] == "failed"


def test_local_artifact_retention_prunes_the_three_unswept_directories(
    monkeypatch, tmp_path
):
    """R11：raw/、handoff/、stage1_cache/ 都在 archive.py 的掃描範圍外。"""
    import os

    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    root = tmp_path / "global_events"
    stale = [
        root / "raw" / "2026" / "01" / "01" / "standard_20260101000000_dead.zip",
        root / "handoff" / "2026" / "01" / "01" / "batch_old.json",
        root / "stage1_cache" / f"{'a' * 64}.json",
    ]
    for path in stale:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        old = datetime_epoch() - 30 * 86400
        os.utime(path, (old, old))
    fresh = root / "handoff" / "2026" / "09" / "05" / "batch_new.json"
    fresh.parent.mkdir(parents=True, exist_ok=True)
    fresh.write_text("{}", encoding="utf-8")

    removed = collector._cleanup_local_artifacts()

    assert removed == {"raw": 1, "handoff": 1, "stage1_cache": 1}
    assert not any(path.exists() for path in stale)
    assert fresh.exists()


def datetime_epoch() -> float:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).timestamp()


def test_collector_version_prefers_deploy_env_then_configured_sha(monkeypatch):
    """R8：Zeabur image 不一定帶 .git，git rev-parse 會永遠回 unknown。"""
    import config
    from collectors.global_events import collector_version

    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "b" * 40)
    assert collector_version() == ("b" * 40, "env:ZEABUR_GIT_COMMIT_SHA")
    monkeypatch.delenv("ZEABUR_GIT_COMMIT_SHA")
    for name in ("ZEABUR_COMMIT_SHA", "GIT_COMMIT_SHA", "SOURCE_COMMIT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(config, "GLOBAL_EVENTS_PRODUCER_GIT_COMMIT", "c" * 40)
    version, source = collector_version()
    # 部署 image 沒有 .git（見 .dockerignore），production 會落到 config fallback
    assert source in {"git", "config:GLOBAL_EVENTS_PRODUCER_GIT_COMMIT"}
    assert version


def test_run_receipt_carries_collector_version(monkeypatch, tmp_path):
    """R8：receipts 表沒有自由欄位，版本標記只能進既有的 receipt jsonb。"""
    payload = _zip([_row("one.example", "Major earthquake kills dozens")])
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setenv("ZEABUR_GIT_COMMIT_SHA", "d" * 40)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    result = collector.run()

    batch_receipt, run_receipt = result["_supabase_receipts"]
    assert run_receipt["receipt"]["collector_version"] == "d" * 40
    assert batch_receipt["receipt"]["collector_version"] == "d" * 40
    assert run_receipt["receipt"]["collector_version_source"] == (
        "env:ZEABUR_GIT_COMMIT_SHA"
    )


# --- Deterministic geography: per-signal vetoes, ADM1 downgrade, gazetteer ---


@pytest.mark.parametrize(
    "title",
    [
        # French idiom that used to route as a coup d'etat.
        "Coup de chauffe à Cognac : trois jours de cirque, de danse et de vertige",
        "Gala des 41es prix Gémeaux: le prix Coup de cœur de l'année",
        # English idiom.
        "Dustin Martin: Albury scores major coup with rare guest speaker",
        # Metaphors, not events.
        "Cheating epidemic in higher ed",
        "Vanishing Trump signs in deep-red state could signal a political earthquake",
        # A burglary is not an armed conflict; an aircraft is not a storm.
        "Resident struck with crowbar in violent home invasion, police search",
        "RAF Typhoon fighter jet set to fly over Enniskillen",
    ],
)
def test_impact_vetoes_discard_idiomatic_signal_matches(title):
    assert title_impact_signals(title) == []


@pytest.mark.parametrize(
    ("title", "signal"),
    [
        # The veto is per signal, so an "airstrike" headline survives the
        # "what to know" explainer shape that a candidate-level filter killed.
        ("What to know about a reported US airstrike that hit a wedding in Iran",
         "armed_conflict"),
        ("Military coup attempt fails in Guinea-Bissau", "national_politics"),
        # No cyclone veto exists: a forward-only lookahead cannot separate
        # aftermath reporting from a team name without losing real events.
        ("Cyclone Narelle makes landfall in Queensland", "major_disaster"),
        ("Congo's Ebola outbreak shows no signs of slowing", "public_health"),
    ],
)
def test_impact_vetoes_keep_real_events_routable(title, signal):
    assert signal in title_impact_signals(title)


def test_vetoed_headline_never_reaches_stage1(monkeypatch, tmp_path):
    payload = _zip(
        [
            _row("cognac.example", "Coup de chauffe à Cognac : trois jours de cirque"),
            _row("wire.example", "US airstrike hit a wedding in Iran"),
        ]
    )
    collector = _configure_enabled_collector(monkeypatch, tmp_path, payload)
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    seen = []

    def _capture(candidates):
        seen.extend(
            document["title"]
            for candidate in candidates
            for document in candidate["representative_documents"]
        )
        return _fake_assessments(collector, candidates)

    monkeypatch.setattr(collector, "_request_stage1", _capture)
    collector.run()
    assert seen == ["US airstrike hit a wedding in Iran"]


def test_admin1_only_mention_is_downgraded_to_a_batch_country_anchor(
    monkeypatch, tmp_path
):
    anchor_row = _row("wire.example", "Major earthquake kills dozens in Kansas")
    anchor_row[9] = "1#United States#US##39.83#-98.58#US"
    admin1_row = _row("mdot.example", "MDOT offers tips for peak hurricane season")
    # Only a US state: real evidence of the country, but a state centroid.
    admin1_row[9] = "2#Mississippi, United States#US#US28#32.75#-89.53#MS"
    collector = _configure_enabled_collector(
        monkeypatch, tmp_path, _zip([anchor_row, admin1_row])
    )
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    records = {
        record["source_headline"]: record
        for record in collector.run()["_candidate_display_records"]
    }
    place = records["MDOT offers tips for peak hurricane season"]["places"][0]
    assert place["location_kind"] == "country_center"
    # The published point is the country point GDELT used elsewhere in the
    # batch, never the state centroid and never a synthesised coordinate.
    assert (place["name"], place["longitude"], place["latitude"]) == (
        "United States",
        -98.58,
        39.83,
    )
    assert place["country_code"] == "US"
    assert place["location_lineage"].startswith(
        "country_center:gdelt:metadata_fallback_loc_"
    )
    assert "降級為國家代表點" in place["evidence_basis"]
    assert "Mississippi, United States" in place["evidence_basis"]
    assert place["evidence_url"] in records[
        "MDOT offers tips for peak hurricane season"
    ]["source_urls"]


def test_admin1_mention_stays_unlocated_without_a_country_anchor(
    monkeypatch, tmp_path
):
    row = _row("mdot.example", "MDOT offers tips for peak hurricane season")
    row[9] = "2#Mississippi, United States#US#US28#32.75#-89.53#MS"
    collector = _configure_enabled_collector(monkeypatch, tmp_path, _zip([row]))
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    record = collector.run()["_candidate_display_records"][0]
    assert record["places"] == []  # No anchor means no country point to borrow.


def test_batch_country_anchor_is_lowest_evidence_id_per_country():
    batch = {
        "payload": {
            "candidates": [
                {
                    "location_evidence": [
                        {
                            "evidence_id": "loc_bbb",
                            "location_type": 1,
                            "name": "United States",
                            "country_code": "US",
                            "longitude": 1.0,
                            "latitude": 2.0,
                        },
                        # Not a country, and an out-of-range point: ignored.
                        {
                            "evidence_id": "loc_aaa",
                            "location_type": 4,
                            "name": "Newark",
                            "country_code": "US",
                            "longitude": 3.0,
                            "latitude": 4.0,
                        },
                        {
                            "evidence_id": "loc_000",
                            "location_type": 1,
                            "name": "Broken",
                            "country_code": "US",
                            "longitude": 999.0,
                            "latitude": 4.0,
                        },
                    ]
                },
                {
                    "location_evidence": [
                        {
                            "evidence_id": "loc_aab",
                            "location_type": 1,
                            "name": "United States",
                            "country_code": "US",
                            "longitude": 5.0,
                            "latitude": 6.0,
                        }
                    ]
                },
            ]
        }
    }
    anchors = batch_country_anchors(batch)
    # Cross-candidate scan, deterministic winner: 'loc_aab' < 'loc_bbb'.
    assert anchors["US"]["evidence_id"] == "loc_aab"
    assert (anchors["US"]["longitude"], anchors["US"]["latitude"]) == (5.0, 6.0)


def _gazetteer(*locations):
    return gazetteer_entries_from_locations(list(locations))


def _gkg_location(location_type, name, country_code, longitude, latitude):
    return {
        "location_type": location_type,
        "name": name,
        "country_code": country_code,
        "adm1_code": None,
        "longitude": longitude,
        "latitude": latitude,
        "feature_id": None,
    }


def test_gazetteer_matches_headline_place_and_quotes_it():
    gazetteer = _gazetteer(
        _gkg_location(1, "Egypt", "EG", 30.0, 27.0),
        _gkg_location(4, "Osoyoos, British Columbia, Canada", "CA", -119.47, 49.03),
    )
    assert gazetteer_lookup(
        "16 killed, 28 injured after bus overturns in Egypt's South Sinai", gazetteer
    ) == ("Egypt", gazetteer["egypt"])
    assert gazetteer_lookup(
        "Small wildfire sparked northwest of Osoyoos", gazetteer
    ) == ("Osoyoos", gazetteer["osoyoos"])


def test_gazetteer_ignores_publisher_suffix_and_mastheads():
    gazetteer = _gazetteer(
        _gkg_location(4, "Grande Prairie, Alberta, Canada", "CA", -118.8, 55.17),
    )
    # The town only appears in the masthead after the separator.
    assert (
        gazetteer_lookup(
            "Resident struck with crowbar in violent home invasion "
            "| My Grande Prairie Now",
            gazetteer,
        )
        is None
    )
    # A masthead word GDELT also geocodes never enters the gazetteer at all.
    assert "independent" not in _gazetteer(
        _gkg_location(4, "Independent, Missouri, United States", "US", -94.4, 39.1)
    )


def test_gazetteer_folds_diacritics_but_never_publishes_an_admin1_point():
    gazetteer = _gazetteer(_gkg_location(2, "Hawaii, United States", "US", -155.5, 19.6))
    headline = "President approves Major Disaster Declaration for Hawaiʻi"
    matched, entry = gazetteer_lookup(headline, gazetteer)
    assert matched == "Hawaii" and entry["location_type"] == 2
    # An ADM1 entry has a state centroid and no anchor on this path, so the
    # candidate stays unlocated rather than being drawn in the wrong place.
    assert (
        headline_gazetteer_places(
            [{"title": headline, "url": "https://one.example/story"}],
            ["https://one.example/story"],
            gazetteer,
        )
        == []
    )


def test_gazetteer_place_is_source_bound_with_url_fragment_lineage():
    gazetteer = _gazetteer(
        _gkg_location(4, "Osoyoos, British Columbia, Canada", "CA", -119.47, 49.03)
    )
    documents = [
        # Not in source_urls (oversized upstream): must be skipped, not used.
        {"title": "Small wildfire near Osoyoos", "url": "https://skip.example/x"},
        {
            "title": "Small wildfire sparked northwest of Osoyoos",
            "url": "https://one.example/story",
        },
    ]
    (place,) = headline_gazetteer_places(
        documents, ["https://one.example/story"], gazetteer
    )
    assert place["source_kind"] == "headline_gazetteer"
    assert place["location_kind"] == "city_center"
    assert place["evidence_url"] == "https://one.example/story"
    assert place["location_lineage"] == "city_center:https://one.example/story#Osoyoos"
    assert place["evidence_basis"] == (
        "依標題地名比對取得的代表位置，未確認為精確發生地：Osoyoos"
    )


def test_gazetteer_lineage_falls_back_when_url_holds_a_fragment():
    gazetteer = _gazetteer(
        _gkg_location(4, "Osoyoos, British Columbia, Canada", "CA", -119.47, 49.03)
    )
    url = "https://one.example/story#comments"
    (place,) = headline_gazetteer_places(
        [{"title": "Wildfire near Osoyoos", "url": url}], [url], gazetteer
    )
    # '#' is the platform's lineage separator, so the opaque form is used.
    assert place["location_lineage"].startswith("city_center:gdelt:headline_")
    assert "#" not in place["location_lineage"].split(":", 1)[1]


def test_gazetteer_persists_for_the_resume_path_and_expires(monkeypatch, tmp_path):
    row = _row("wire.example", "Major earthquake kills dozens near Osoyoos")
    row[9] = "4#Osoyoos, British Columbia, Canada#CA##49.03#-119.47#911"
    collector = _configure_enabled_collector(monkeypatch, tmp_path, _zip([row]))
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    result = collector.run()
    assert result["gazetteer_size"] >= 1
    stored = json.loads(
        (tmp_path / "global_events" / "gazetteer.json").read_text(encoding="utf-8")
    )
    assert stored["version"] == 1 and "osoyoos" in stored["entries"]

    # A resumed round never re-parses a GKG file, so the disk copy is what
    # makes headline matching work at all.
    import config

    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    reloaded = GlobalEventsCollector()._load_gazetteer()
    assert reloaded["osoyoos"]["name"] == "Osoyoos"

    batch = {
        "batch_id": "batch_" + "0" * 24,
        "payload": {
            "candidates": [
                {
                    "candidate_id": "cand_" + "0" * 24,
                    "observation_window": {
                        "first_slot": "20260901120000",
                        "last_slot": "20260901120000",
                    },
                    "representative_documents": [
                        {
                            "title": "Small wildfire sparked northwest of Osoyoos",
                            "url": "https://one.example/story",
                        }
                    ],
                    "location_evidence": [],
                }
            ]
        },
    }
    (record,) = candidate_display_records(
        batch, {"assessments": []}, "2026-09-05T00:00:00+00:00", None, reloaded
    )
    assert record["places"][0]["source_kind"] == "headline_gazetteer"

    # Names unseen for the retention window are forgotten.
    aged = {"stale": {**reloaded["osoyoos"], "last_seen": "2020-01-01"}}
    assert GlobalEventsCollector()._save_gazetteer(aged, {}) == {}


def test_local_cleanup_never_prunes_the_gazetteer(monkeypatch, tmp_path):
    import config

    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    collector = GlobalEventsCollector()
    collector.gazetteer_path.parent.mkdir(parents=True, exist_ok=True)
    collector.gazetteer_path.write_text('{"version": 1, "entries": {}}')
    ancient = datetime.now(timezone.utc).timestamp() - 90 * 86400
    os.utime(collector.gazetteer_path, (ancient, ancient))
    collector._cleanup_local_artifacts()
    # Operational state at the collector root, outside every retention sweep.
    assert collector.gazetteer_path.exists()


MIGRATION_399_PLACE_FIELDS = {
    "place_key",
    "name",
    "country_code",
    "country_code_scheme",
    "location_kind",
    "longitude",
    "latitude",
    "evidence_url",
    "location_lineage",
    "evidence_basis",
    "source_kind",
}


def _assert_migration_399_place_contract(places, source_urls):
    """Mirror the CHECK bodies in gis-platform migration 399.

    Any violation makes ``ingest_global_event_candidates`` RAISE and reject the
    whole batch, so every produced place is verified field by field here.
    """
    keys = set()
    for place in places:
        assert set(place) == MIGRATION_399_PLACE_FIELDS
        assert place["place_key"].strip() and len(place["place_key"]) <= 200
        assert place["name"].strip() and len(place["name"]) <= 300
        assert place["location_kind"] in {
            "event_point",
            "city_center",
            "country_center",
            "unknown",
        }
        assert place["country_code_scheme"] in {"fips10", "iso2"}
        assert place["source_kind"] in {
            "gdelt_metadata_mention",
            "reported",
            "geocoded",
            "headline_gazetteer",
        }
        assert len(place["country_code"] or "") <= 10
        assert len(place["evidence_basis"] or "") <= 500
        assert len(place["location_lineage"] or "") <= 2500
        # Representative sources can never assert an exact event point.
        assert not (
            place["source_kind"] in {"gdelt_metadata_mention", "headline_gazetteer"}
            and place["location_kind"] == "event_point"
        )
        assert -180 <= place["longitude"] <= 180
        assert -90 <= place["latitude"] <= 90
        assert place["evidence_url"] in source_urls
        assert (place["evidence_basis"] or "").strip()
        assert re.fullmatch(
            place["location_kind"]
            + r":(gdelt:[a-zA-Z0-9_-]+|https?://[^\s#]+#.+)",
            place["location_lineage"],
        )
        assert place["place_key"] not in keys
        keys.add(place["place_key"])


def test_every_produced_place_satisfies_migration_399(monkeypatch, tmp_path):
    anchor_row = _row("wire.example", "Major earthquake kills dozens in Kansas")
    anchor_row[9] = (
        "1#United States#US##39.83#-98.58#US;"
        "4#Osoyoos, British Columbia, Canada#CA##49.03#-119.47#911"
    )
    admin1_row = _row("mdot.example", "MDOT offers tips for peak hurricane season")
    admin1_row[9] = "2#Mississippi, United States#US#US28#32.75#-89.53#MS"
    gazetteer_row = _row("bc.example", "Small wildfire northwest of Osoyoos now held")
    gazetteer_row[9] = ""
    collector = _configure_enabled_collector(
        monkeypatch, tmp_path, _zip([anchor_row, admin1_row, gazetteer_row])
    )
    monkeypatch.setattr(collector, "_upload_handoff", lambda *args: True)
    monkeypatch.setattr(
        collector,
        "_request_stage1",
        lambda candidates: _fake_assessments(collector, candidates),
    )
    records = collector.run()["_candidate_display_records"]
    produced = {
        place["source_kind"] for record in records for place in record["places"]
    }
    kinds = {
        place["location_kind"] for record in records for place in record["places"]
    }
    assert produced == {"gdelt_metadata_mention", "headline_gazetteer"}
    assert kinds == {"country_center", "city_center"}
    for record in records:
        _assert_migration_399_place_contract(record["places"], record["source_urls"])
