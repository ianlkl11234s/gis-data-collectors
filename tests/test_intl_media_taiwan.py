"""Unit tests for the GDELT international-media metadata collector."""

from __future__ import annotations

import io
import json
import zipfile
from contextlib import contextmanager

import pytest

import config
from collectors.base import BaseCollector
from collectors.intl_media_taiwan import (
    GKGArtifact,
    IntlMediaTaiwanCollector,
    candidate_rules,
    canonical_url,
    iter_logical_rows,
    parse_gkg_locations,
    parse_gkg_tone,
    validate_stage1,
)
from storage.supabase_writer import SupabaseWriter


def make_row(
    *,
    record_id: str = "20260830120000-1",
    url: str = "https://example.com/story?utm_source=x&id=1",
    title: str = "Taiwan security update",
    locations: str = "",
    quotations: str = "COPYRIGHTED QUOTATION MUST NOT SURVIVE",
) -> list[str]:
    row = [""] * 27
    row[0] = record_id
    row[1] = "20260830115900"
    row[3] = "example.com"
    row[4] = url
    row[7] = "TAX_FNCACT_TAIWAN"
    row[9] = locations
    row[11] = "person"
    row[13] = "organization"
    row[15] = "-1.2,2.3"
    row[22] = quotations
    row[26] = f"<PAGE_TITLE>{title}</PAGE_TITLE>"
    return row


def zip_row(row: list[str], continuation: str | None = None) -> bytes:
    text = "\t".join(row)
    if continuation:
        text += "\n" + continuation
    payload = io.BytesIO()
    with zipfile.ZipFile(payload, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("gkg.tsv", text + "\n")
    return payload.getvalue()


def artifact(stream: str = "standard", slot: str = "20260830120000") -> GKGArtifact:
    return GKGArtifact(stream, slot, 0, "", f"https://example/{slot}.gkg.csv.zip")


def bare_collector() -> IntlMediaTaiwanCollector:
    collector = IntlMediaTaiwanCollector.__new__(IntlMediaTaiwanCollector)
    collector.supabase_writer = None
    collector._domain_registry = None
    collector._pending_checkpoints = {}
    return collector


class TestParserAndCandidateRules:
    def test_translation_xml_continuation_is_one_logical_row(self):
        row = make_row(title="split")
        row[26] = "<PAGE_TITLE>Taiwan"
        lines = ["\t".join(row) + "\n", "security update</PAGE_TITLE>\n"]
        parsed = list(iter_logical_rows(lines))
        assert len(parsed) == 1
        assert parsed[0][26].endswith("security update</PAGE_TITLE>")

    def test_candidate_by_tw_location(self):
        row = make_row(title="Unrelated words", locations="4#Taipei#TW#TW03#25#121#1")
        row[7] = row[11] = row[13] = ""
        assert candidate_rules(row, "Unrelated words") == ["location_country_tw"]

    def test_candidate_by_multilingual_registry(self):
        row = make_row(title="臺灣晶片產業")
        assert "taiwan_nonlocation_registry_v2" in candidate_rules(row, "臺灣晶片產業")

    def test_parser_never_retains_quotations_or_full_xml(self):
        collector = bare_collector()
        row = make_row()
        payload = zip_row(row)
        parsed, latest = collector._parse_artifact(artifact(), payload)
        assert latest == "2026-08-30T11:59:00+00:00"
        assert len(parsed) == 1
        encoded = json.dumps(parsed, ensure_ascii=False)
        assert "COPYRIGHTED QUOTATION" not in encoded
        assert "V2.1QUOTATIONS" not in encoded
        assert set(parsed[0]) >= {"url_norm", "report_key", "gkg_themes", "candidate_rules"}
        assert parsed[0]["source_location_level"] is None
        assert parsed[0]["source_latitude"] is None

    def test_missing_title_is_retained_with_quality_flag(self):
        collector = bare_collector()
        row = make_row(title="", locations="4#Taipei#TW#TW03#25#121#1")
        parsed, _latest = collector._parse_artifact(artifact(), zip_row(row))
        assert len(parsed) == 1
        assert parsed[0]["title_original"] is None
        assert parsed[0]["quality_flags"] == ["missing_title"]

    def test_canonical_url_dedups_tracking_variants(self):
        a = canonical_url("http://WWW.Example.com/a/?utm_source=x&id=1#top")
        b = canonical_url("https://example.com/a?id=1&fbclid=abc")
        assert a == b == "https://example.com/a?id=1"

    def test_gkg_metadata_is_normalized_not_raw(self):
        locations = parse_gkg_locations("4#Taipei#TW#TW03#25.04#121.53#-2637882")
        assert locations == [{
            "location_type": 4,
            "name": "Taipei",
            "country_code": "TW",
            "adm1_code": "TW03",
            "latitude": 25.04,
            "longitude": 121.53,
            "feature_id": "-2637882",
        }]
        tone = parse_gkg_tone("-1.2,2.3,3.5,5.8,10,1.5,321")
        assert tone["tone"] == -1.2
        assert tone["word_count"] == 321

    def test_missing_tone_is_stored_as_json_object(self):
        collector = bare_collector()
        row = make_row()
        row[15] = ""
        parsed, _latest = collector._parse_artifact(artifact(), zip_row(row))
        assert parsed[0]["gkg_tone"] == {}


class TestStage1Validation:
    @staticmethod
    def valid_payload():
        return {
            "items": [
                {
                    "id": 0,
                    "source_kind": "foreign_editorial_media",
                    "taiwan_relevance": 2,
                    "importance": 1,
                    "topics": ["cross_strait"],
                },
                {
                    "id": 1,
                    "source_kind": "unclear",
                    "taiwan_relevance": 0,
                    "importance": 0,
                    "topics": [],
                },
            ]
        }

    def test_accepts_exact_ids_schema_and_enums(self):
        assert len(validate_stage1(self.valid_payload(), {0, 1})) == 2

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda p: p["items"].append(dict(p["items"][0])),
            lambda p: p["items"][0].update({"extra": True}),
            lambda p: p["items"][0].update({"topics": ["not_an_enum"]}),
            lambda p: p["items"][0].update({"importance": 4}),
            lambda p: p.update({"explanation": "drift"}),
        ],
    )
    def test_rejects_id_schema_and_enum_drift(self, mutate):
        payload = self.valid_payload()
        mutate(payload)
        with pytest.raises(ValueError):
            validate_stage1(payload, {0, 1})

    def test_retry_falls_back_and_preserves_registry_source_kind(self, monkeypatch):
        collector = bare_collector()
        calls = []

        def request(model, batch):
            calls.append(model)
            if len(calls) == 1:
                return {"choices": [{"message": {"content": '{"items":[]}'}}]}, 0.1
            payload = {
                "items": [{
                    "id": 0,
                    "source_kind": "unclear",
                    "taiwan_relevance": 3,
                    "importance": 2,
                    "topics": ["security_defense"],
                }]
            }
            return {
                "choices": [{"message": {"content": json.dumps(payload)}}],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5, "cost": 0.001},
            }, 0.2

        monkeypatch.setattr(collector, "_request_with_deadline", request)
        monkeypatch.setattr("collectors.intl_media_taiwan.time.sleep", lambda _seconds: None)
        row = {
            "source_domain": "theguardian.com",
            "source_country": "UK",
            "source_kind": "foreign_editorial_media",
            "title_original": "Taiwan report",
            "gkg_themes": [],
            "gkg_locations": "",
            "gkg_persons": [],
            "gkg_organizations": [],
        }
        output, usage = collector._classify_batch([row])
        assert calls == ["openai/gpt-oss-20b", "qwen/qwen3.7-flash"]
        assert output[0]["source_kind"] == "foreign_editorial_media"
        assert output[0]["source_kind_method"] == "domain_registry"
        assert output[0]["llm_model"] == "qwen/qwen3.7-flash"
        assert usage["attempts"] == 2


class TestDomainRegistry:
    def test_registry_has_frozen_stream_top100_union(self):
        collector = bare_collector()
        registry = collector._load_domain_registry()
        assert len(registry) == 199
        assert registry["theguardian.com"] == {
            "source_kind": "foreign_editorial_media",
            "source_country": "UK",
            "source_city": "London",
            "source_location_label": "London, United Kingdom",
            "source_latitude": 51.5074,
            "source_longitude": -0.1278,
            "source_location_level": "city",
            "source_location_method": "outlet_registry",
            "source_location_confidence": "verified",
        }
        assert registry["n.yam.com"]["source_kind"] == "aggregator_press_release_or_ugc"

    def test_country_fallback_has_no_fabricated_coordinates(self):
        registry = bare_collector()._load_domain_registry()
        source = registry["dailymail.com"]
        assert source["source_country"] == "UK"
        assert source["source_city"] is None
        assert source["source_location_label"] == "United Kingdom"
        assert source["source_location_level"] == "country"
        assert source["source_location_method"] == "country_registry"
        assert source["source_location_confidence"] == "verified"
        assert source["source_latitude"] is None
        assert source["source_longitude"] is None

    def test_source_location_and_mentioned_locations_are_independent(self):
        collector = bare_collector()
        row = make_row(
            url="https://theguardian.com/world/taiwan",
            locations="4#Taipei#TW#TW03#25.04#121.53#-2637882",
        )
        row[3] = "theguardian.com"
        parsed, _latest = collector._parse_artifact(artifact(), zip_row(row))
        excluded, llm_input = collector._partition_by_domain_registry(parsed)
        assert excluded == []
        report = llm_input[0]
        assert report["source_location_label"] == "London, United Kingdom"
        assert (report["source_latitude"], report["source_longitude"]) == (51.5074, -0.1278)
        assert report["gkg_locations"][0]["name"] == "Taipei"
        assert (
            report["gkg_locations"][0]["latitude"],
            report["gkg_locations"][0]["longitude"],
        ) == (25.04, 121.53)

    @pytest.mark.parametrize(
        "mapping, message",
        [
            (
                {
                    "level": "city",
                    "city": "Nowhere",
                    "label": "Nowhere, Test",
                    "latitude": 20,
                    "longitude": None,
                    "method": "outlet_registry",
                    "confidence": "verified",
                },
                "pair",
            ),
            (
                {
                    "level": "city",
                    "city": "Nowhere",
                    "label": "Nowhere, Test",
                    "latitude": 91,
                    "longitude": 0,
                    "method": "outlet_registry",
                    "confidence": "verified",
                },
                "range",
            ),
        ],
    )
    def test_source_coordinates_must_be_paired_and_in_range(self, mapping, message):
        with pytest.raises(ValueError, match=message):
            bare_collector()._source_location_metadata("US", "United States", mapping)

    def test_reviewed_government_capital_method_is_supported_without_changing_source_kind(self):
        location = bare_collector()._source_location_metadata(
            "US",
            "United States",
            {
                "level": "city",
                "city": "Washington, D.C.",
                "label": "Washington, D.C., United States",
                "latitude": 38.9072,
                "longitude": -77.0369,
                "method": "government_capital",
                "confidence": "fallback",
            },
        )
        assert location["source_location_method"] == "government_capital"
        assert location["source_location_level"] == "city"
        assert location["source_location_confidence"] == "fallback"

    def test_local_and_aggregator_are_excluded_from_stage1_and_main_rows(self):
        collector = bare_collector()
        rows = [
            {"source_domain": "n.yam.com"},
            {"source_domain": "taipeitimes.com"},
            {"source_domain": "theguardian.com"},
            {"source_domain": "unknown-example.tw"},
        ]
        excluded, llm_input = collector._partition_by_domain_registry(rows)
        assert [row["source_domain"] for row in excluded] == ["n.yam.com", "taipeitimes.com"]
        assert excluded[0]["source_kind"] == "aggregator_press_release_or_ugc"
        assert excluded[1]["source_kind"] == "taiwan_editorial_media"
        assert llm_input[0]["source_kind"] == "foreign_editorial_media"
        assert llm_input[0]["source_country"] == "UK"
        assert llm_input[0]["source_city"] == "London"
        # No suffix heuristic: unregistered .tw remains unknown and goes to Stage1.
        assert llm_input[1]["source_domain"] == "unknown-example.tw"
        assert llm_input[1]["source_country"] is None
        assert llm_input[1]["source_city"] is None
        assert llm_input[1]["source_location_label"] is None
        assert llm_input[1]["source_location_method"] is None
        assert llm_input[1]["source_latitude"] is None
        assert llm_input[1]["source_longitude"] is None


class TestCheckpoint:
    def test_selects_only_contiguous_slots_after_checkpoint(self, monkeypatch):
        monkeypatch.setattr(config, "INTL_MEDIA_TAIWAN_MAX_FILES_PER_STREAM", 4)
        collector = bare_collector()
        artifacts = [
            artifact(slot="20260830121500"),
            artifact(slot="20260830123000"),
            artifact(slot="20260830124500"),
        ]
        selected = collector._select_pending(artifacts, "20260830120000")
        assert [item.slot for item in selected] == [
            "20260830121500", "20260830123000", "20260830124500"
        ]

    def test_gap_does_not_skip_to_later_slot(self, monkeypatch):
        monkeypatch.setattr(config, "INTL_MEDIA_TAIWAN_MAX_FILES_PER_STREAM", 4)
        collector = bare_collector()
        artifacts = [artifact(slot="20260830121500"), artifact(slot="20260830124500")]
        with pytest.raises(RuntimeError, match="index gap at 20260830123000") as error:
            collector._select_pending(artifacts, "20260830120000")
        assert [item.slot for item in error.value.completed] == ["20260830121500"]

    def test_local_checkpoint_advances_only_after_successful_base_run(self, monkeypatch):
        collector = bare_collector()
        collector._pending_checkpoints = {"standard": "20260830121500"}
        saved = []
        monkeypatch.setattr(collector, "_save_checkpoints", lambda value: saved.append(value.copy()))

        monkeypatch.setattr(BaseCollector, "run", lambda _self: {"error": "db failed"})
        collector.run()
        assert saved == []

        collector._pending_checkpoints = {"standard": "20260830121500"}
        monkeypatch.setattr(BaseCollector, "run", lambda _self: {"new_rows": 1})
        collector.run()
        assert saved == [{"standard": "20260830121500"}]

    def test_llm_failure_keeps_both_stream_checkpoints_unadvanced(self, monkeypatch):
        collector = bare_collector()
        monkeypatch.setattr(collector, "_load_checkpoints", lambda: {})
        monkeypatch.setattr(
            collector,
            "_fetch_index",
            lambda stream: [artifact(stream=stream, slot="20260830120000")],
        )
        monkeypatch.setattr(collector, "_download_artifact", lambda _artifact: b"unused")
        monkeypatch.setattr(
            collector,
            "_parse_artifact",
            lambda item, _payload: ([{
                "source_stream": item.stream,
                "source_domain": f"unknown-{item.stream}.example",
                "url_norm": f"https://{item.stream}.example/report",
                "title_original": "Taiwan report",
            }], "2026-08-30T11:59:00+00:00"),
        )
        monkeypatch.setattr(collector, "_existing_url_norms", lambda _urls: set())
        monkeypatch.setattr(
            collector,
            "_partition_by_domain_registry",
            lambda rows: ([], rows),
        )
        monkeypatch.setattr(
            collector,
            "_annotate",
            lambda _rows: (_ for _ in ()).throw(RuntimeError("model unavailable")),
        )

        result = collector.collect()
        assert result["data"] == []
        assert "openrouter_stage1" in result["_collector_error"]
        assert collector._pending_checkpoints == {}
        assert all(state["success"] is False for state in result["source_states"])
        assert all(state["checkpoint_slot_ts"] is None for state in result["source_states"])


def test_transform_includes_reports_and_two_source_states():
    writer = SupabaseWriter.__new__(SupabaseWriter)
    records = writer._transform_intl_media_taiwan(
        {
            "data": [{"source_id": "gdelt_gkg_standard", "url_norm": "https://x/"}],
            "source_states": [
                {"source_id": "gdelt_gkg_standard", "source_stream": "standard"},
                {"source_id": "gdelt_gkg_translation", "source_stream": "translation"},
            ],
        },
        None,
    )
    assert [row["_type"] for row in records] == ["report", "source_state", "source_state"]


def test_writer_atomically_writes_reports_state_and_health(monkeypatch):
    writer = SupabaseWriter.__new__(SupabaseWriter)
    executed = []
    transaction_count = 0

    class Cursor:
        def execute(self, sql, params=None):
            executed.append((sql, params))

    @contextmanager
    def txn(_conn):
        nonlocal transaction_count
        transaction_count += 1
        yield Cursor()

    writer._txn = txn
    inserted_values = []

    def capture_values(_cur, sql, values, **_kwargs):
        inserted_values.extend(values)
        executed.append((sql, None))
        return [("standard",)]

    monkeypatch.setattr("storage.supabase_writer.execute_values", capture_values)
    records = [
        {
            "_type": "report",
            "source_id": "gdelt_gkg_standard",
            "source_stream": "standard",
            "source_country": "UK",
            "source_city": "London",
            "source_location_label": "London, United Kingdom",
            "source_latitude": 51.5074,
            "source_longitude": -0.1278,
            "source_location_level": "city",
            "source_location_method": "outlet_registry",
            "source_location_confidence": "verified",
            "url_norm": "https://example.com/report",
        },
        {
            "_type": "source_state",
            "source_id": "gdelt_gkg_standard",
            "source_stream": "standard",
            "success": True,
            "feed_url": "gdelt://gkg/standard",
        },
        {
            "_type": "source_state",
            "source_id": "gdelt_gkg_translation",
            "source_stream": "translation",
            "success": True,
            "feed_url": "gdelt://gkg/translation",
        },
    ]
    writer._write_multi_table(object(), "intl_media_taiwan", records)
    sql = "\n".join(statement for statement, _params in executed)
    assert transaction_count == 1
    assert "INSERT INTO live.intl_media_taiwan " in sql
    assert "INSERT INTO live.intl_media_taiwan_source_state " in sql
    assert "INSERT INTO live.source_health " in sql
    assert "interval_minutes" in sql and "latest_item_ts" in sql
    assert all(
        column in sql
        for column in (
            "source_city",
            "source_location_label",
            "source_latitude",
            "source_longitude",
            "source_location_level",
            "source_location_method",
            "source_location_confidence",
        )
    )
    assert inserted_values[0][4:11] == (
        "London",
        "London, United Kingdom",
        51.5074,
        -0.1278,
        "city",
        "outlet_registry",
        "verified",
    )
