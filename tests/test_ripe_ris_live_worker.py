"""RIPE RIS Live bounded stream, gap and private archive contract tests."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml
from botocore.exceptions import ClientError

import config
from collectors.registry import get_entry_by_name
from storage.supabase_tables import TABLE_MAP
from workers.ripe_ris_live import (
    RipeRisLiveWorker,
    RipeRisSpoolManager,
    WindowState,
    _ris_roster,
    _subscription_specs,
)


UTC = timezone.utc
FIXTURES = Path(__file__).parent / "fixtures"


def _messages():
    return json.loads((FIXTURES / "ripe_ris_live_messages.json").read_text(encoding="utf-8"))


def _roster_file(path: Path) -> Path:
    payload = {
        "schema_version": "ripe_internet_health_roster.v1",
        "version": 2,
        "review_status": "approved",
        "internal_only": True,
        "country_code": "TW",
        "ripe_atlas": {"measurements": []},
        "ripe_ris_live": {
            "prefixes": ["203.0.113.0/24"],
            "origin_asns": [64510, 64511],
            "more_specific": True,
            "less_specific": False,
        },
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


class _FakeBody:
    def __init__(self, value): self.value = value
    def read(self): return self.value


class _FakeS3Client:
    def __init__(self, wrong_hash=False):
        self.objects = {}
        self.wrong_hash = wrong_hash

    def head_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        value, metadata = self.objects[Key]
        if self.wrong_hash and not Key.endswith("manifest.json"):
            metadata = {**metadata, "sha256": "wrong"}
        return {"ContentLength": len(value), "Metadata": metadata}

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.objects[key] = (Path(path).read_bytes(), ExtraArgs.get("Metadata", {}))

    def put_object(self, *, Bucket, Key, Body, Metadata, **_kwargs):
        self.objects[Key] = (Body, Metadata)

    def get_object(self, *, Bucket, Key):
        return {"Body": _FakeBody(self.objects[Key][0])}


class _FakeS3:
    bucket = "test"
    ClientError = ClientError
    def __init__(self, wrong_hash=False): self.s3 = _FakeS3Client(wrong_hash)


class _Writer:
    def __init__(self): self.calls = []
    def write(self, name, result, timestamp):
        self.calls.append((name, result, timestamp))
        return True


def test_ris_registry_defaults_and_persistent_worker_contract():
    entry = get_entry_by_name("ripe_ris_live")
    assert entry is not None and entry.persistent is True
    assert config.RIPE_RIS_LIVE_ENABLED is False
    assert config.RIPE_RIS_REPLICA_COUNT == 0
    assert TABLE_MAP["ripe_ris_live"]["is_multi_table"] is True


def test_ris_roster_is_bounded_and_prefix_subscription_precedes_asn():
    roster = _ris_roster({
        "ripe_ris_live": {
            "prefixes": ["203.0.113.0/24"],
            "origin_asns": [64510],
            "more_specific": True,
            "less_specific": False,
        }
    })
    specs = _subscription_specs(roster)
    assert specs == [{
        "type": "UPDATE", "prefix": "203.0.113.0/24",
        "moreSpecific": True, "lessSpecific": False,
    }]
    with pytest.raises(ValueError, match="bounded"):
        _ris_roster({"ripe_ris_live": {"prefixes": [f"10.{i // 256}.{i % 256}.0/24" for i in range(257)]}})


def test_replica_count_is_a_hard_enable_gate(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "RIPE_RIS_REPLICA_COUNT", 0)
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    worker = RipeRisLiveWorker(writer=_Writer(), s3_storage=_FakeS3(), connect_factory=lambda *_a, **_k: None)
    with pytest.raises(RuntimeError, match="REPLICA_COUNT=1"):
        worker.preflight()


def test_complete_window_writes_unknown_metrics_without_false_normal(monkeypatch):
    writer = _Writer()
    now = datetime(2026, 8, 31, 0, 5, tzinfo=UTC)
    worker = RipeRisLiveWorker(writer=writer, s3_storage=_FakeS3(), connect_factory=lambda *_a, **_k: None, now_fn=lambda: now)
    worker._roster = {
        "prefixes": ["203.0.113.0/24"], "origin_asns": [64510, 64511],
        "more_specific": True, "less_specific": False,
    }
    worker._subscription_hash = "reviewed-hash"
    window = WindowState(start=now - timedelta(minutes=5), end=now, gap=False)
    worker._window = window
    for envelope in _messages()[1:3]:
        worker._aggregate(envelope, now.isoformat())
    worker._flush_window(window)
    _, result, _ = writer.calls[0]
    run = result["data"][0]
    observations = result["data"][1:]
    by_signal = {row["signal"]: row for row in observations}
    assert run["status"] == "succeeded"
    assert by_signal["withdrawn_prefix_ratio_ipv4"]["value"] == 1.0
    assert by_signal["origin_change_count_ipv4"]["value"] == 1.0
    assert by_signal["prefix_visibility_ratio_ipv4"]["value"] is None
    assert by_signal["prefix_visibility_ratio_ipv4"]["quality_flags"]["state_uninitialized"] is True
    assert {row["reported_status"] for row in observations} == {"unknown"}
    assert all(row["metadata"]["public_visibility"] == "internal_only" for row in observations)


def test_any_stream_gap_nulls_entire_window():
    writer = _Writer()
    now = datetime(2026, 8, 31, 0, 5, tzinfo=UTC)
    worker = RipeRisLiveWorker(writer=writer, s3_storage=_FakeS3(), connect_factory=lambda *_a, **_k: None, now_fn=lambda: now)
    worker._roster = {
        "prefixes": ["203.0.113.0/24"], "origin_asns": [],
        "more_specific": True, "less_specific": False,
    }
    worker._subscription_hash = "reviewed-hash"
    window = WindowState(
        start=now - timedelta(minutes=5), end=now, gap=True,
        gap_reasons=["pong_timeout"], messages=10,
    )
    worker._flush_window(window)
    run = writer.calls[0][1]["data"][0]
    observations = writer.calls[0][1]["data"][1:]
    assert run["status"] == "partial"
    assert run["error_code"] == "stream_gap"
    assert all(row["value"] is None for row in observations)
    assert all(row["quality_flags"]["stream_gap"] is True for row in observations)
    assert {row["reported_status"] for row in observations} == {"unknown"}
    assert {row["source"] for row in observations} == {"ripe_ris_live"}
    assert {row["evidence_family"] for row in observations} == {"ripe_ris"}


def test_complete_empty_window_is_unknown_not_normal():
    writer = _Writer()
    now = datetime(2026, 8, 31, 0, 5, tzinfo=UTC)
    worker = RipeRisLiveWorker(writer=writer, s3_storage=_FakeS3(), connect_factory=lambda *_a, **_k: None, now_fn=lambda: now)
    worker._roster = {
        "prefixes": ["203.0.113.0/24"], "origin_asns": [],
        "more_specific": True, "less_specific": False,
    }
    worker._subscription_hash = "reviewed-hash"
    worker._flush_window(WindowState(start=now - timedelta(minutes=5), end=now, gap=False))
    run = writer.calls[0][1]["data"][0]
    observations = writer.calls[0][1]["data"][1:]
    assert run["status"] == "succeeded"
    assert run["error_code"] == "empty"
    assert {row["reported_status"] for row in observations} == {"unknown"}
    assert all(row["signal"] != "provider_status" for row in observations)
    assert all(row["source_updated_at"] is None for row in observations)


def test_private_spool_deletes_only_after_manifest_readback(tmp_path):
    spool = RipeRisSpoolManager(tmp_path, _FakeS3(), subscription_hash="reviewed-hash")
    path = tmp_path / "ripe_ris_live_spool" / "sample.jsonl.gz"
    path.write_bytes(b"test-payload")
    assert spool._upload(path, count=1)
    assert not path.exists()
    bad = tmp_path / "ripe_ris_live_spool" / "bad.jsonl.gz"
    bad.write_bytes(b"test-payload")
    assert not RipeRisSpoolManager(tmp_path, _FakeS3(wrong_hash=True), subscription_hash="reviewed-hash")._upload(bad, count=1)
    assert bad.exists()


def test_websocket_requires_ack_and_disables_binary_raw(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    fixture = _messages()

    class FakeWebSocket:
        def __init__(self): self.sent = []; self.items = iter(fixture[:2])
        def send(self, value): self.sent.append(json.loads(value))
        def recv(self, timeout=None):
            try: return json.dumps(next(self.items))
            except StopIteration: raise ConnectionError("bounded test stop")
        def close(self): pass

    socket = FakeWebSocket()
    worker = RipeRisLiveWorker(
        writer=_Writer(), s3_storage=_FakeS3(),
        connect_factory=lambda *_a, **_k: socket,
    )
    worker._roster = {
        "prefixes": ["203.0.113.0/24"], "origin_asns": [64510, 64511],
        "more_specific": True, "less_specific": False,
    }
    worker._subscriptions = _subscription_specs(worker._roster)
    worker._subscription_hash = "reviewed-hash"
    worker.spool = RipeRisSpoolManager(tmp_path, _FakeS3(), subscription_hash="reviewed-hash")
    with pytest.raises(ConnectionError, match="bounded test stop"):
        worker._connect_and_consume()
    subscribe = socket.sent[0]
    assert subscribe["type"] == "ris_subscribe"
    assert subscribe["data"]["socketOptions"] == {"includeRaw": False, "acknowledge": True}
    assert worker._acked is True
    assert worker.stats.messages_received == 1
    worker.spool.close()


def test_process_local_lock_blocks_second_worker(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "RIPE_RIS_REPLICA_COUNT", 1)
    monkeypatch.setattr(config, "RIPE_INTERNET_HEALTH_ROSTER_PATH", str(_roster_file(tmp_path / "roster.yaml")))
    first = RipeRisLiveWorker(writer=_Writer(), s3_storage=_FakeS3(), connect_factory=lambda *_a, **_k: None)
    second = RipeRisLiveWorker(writer=_Writer(), s3_storage=_FakeS3(), connect_factory=lambda *_a, **_k: None)
    first.prepare()
    with pytest.raises(RuntimeError, match="process-local lock"):
        second.prepare()
    first.stop()
    first.run()  # releases lock without opening a socket
    second.prepare()
    second.stop()
    second.run()


def test_inventory_keeps_ripe_internal_disabled_and_documents_archive_contract():
    repo = Path(__file__).resolve().parent.parent
    cross = yaml.safe_load((repo / "config" / "cross_layer_map.yaml").read_text(encoding="utf-8"))
    backup = yaml.safe_load((repo / "config" / "backup_manifest.yaml").read_text(encoding="utf-8"))
    zeabur = json.loads((repo / "zeabur.json").read_text(encoding="utf-8"))["env"]
    for name in ("ripe_atlas_internet_health", "ripe_ris_live"):
        assert cross[name]["enabled"] is False
        assert cross[name]["deployment"] == "disabled"
        assert cross[name]["critical"] is False
    assert cross["ripe_atlas_internet_health"]["s3_prefixes"][0]["expected_daily"] is False
    assert cross["ripe_ris_live"]["s3_prefixes"][0]["prefix"] == "ripe_ris_live/raw/v1/"
    assert zeabur["RIPE_ATLAS_INTERNET_HEALTH_ENABLED"]["default"] == "false"
    assert zeabur["RIPE_RIS_LIVE_ENABLED"]["default"] == "false"
    assert zeabur["RIPE_RIS_REPLICA_COUNT"]["default"] == "0"
    assert "live.internet_health_source_runs" in backup["archive_py_covered"]
