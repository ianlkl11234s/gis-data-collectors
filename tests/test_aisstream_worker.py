"""AISStream worker 的本地 contract / resilience smoke tests。"""

import gzip
import hashlib
import json
from botocore.exceptions import ClientError

from workers.aisstream import (
    AISStreamWorker,
    SpoolManager,
    _coordinates,
    _mmsi,
    _message_body,
    _observed_at,
    _subscription_boxes,
)


def test_default_subscription_has_five_named_boxes():
    boxes, names = _subscription_boxes()
    assert len(boxes) == 5
    assert set(names) == {
        "taiwan_north_east",
        "yonaguni_ishigaki",
        "miyako_okinawa",
        "amami",
        "kyushu_southwest",
    }


def test_position_event_normalization_helpers_accept_nested_message():
    event = {
        "MessageType": "PositionReport",
        "MetaData": {"MMSI": 416000001},
        "Message": {"PositionReport": {"UserID": 416000001, "Latitude": 24.5, "Longitude": 121.5}},
    }
    body = _message_body(event)
    assert _mmsi(event, body) == "416000001"
    assert _coordinates(event, body) == (24.5, 121.5)


def test_ais_timestamp_second_is_not_treated_as_unix_epoch():
    event = {
        "MessageType": "PositionReport",
        "MetaData": {"MMSI": 416000001, "time_utc": "2026-08-24 12:34:56.123 +0000 UTC"},
        "Message": {"PositionReport": {"UserID": 416000001, "Timestamp": 5}},
    }
    assert _observed_at(event, "2026-08-24T12:35:00+00:00").startswith("2026-08-24T12:34:56")
    no_metadata = {"MessageType": "PositionReport", "Message": {"PositionReport": {"Timestamp": 5}}}
    assert _observed_at(no_metadata, "2026-08-24T12:35:00+00:00") == "2026-08-24T12:35:00+00:00"


class _FakeBody:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


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
        self.objects[key] = (open(path, "rb").read(), ExtraArgs.get("Metadata", {}))

    def put_object(self, *, Bucket, Key, Body, Metadata, **_kwargs):
        self.objects[Key] = (Body, Metadata)

    def get_object(self, *, Bucket, Key):
        return {"Body": _FakeBody(self.objects[Key][0])}


class _FakeS3:
    bucket = "test"
    ClientError = ClientError

    def __init__(self, wrong_hash=False):
        self.s3 = _FakeS3Client(wrong_hash=wrong_hash)


def test_spool_upload_requires_object_and_manifest_hash_verification(tmp_path):
    spool = SpoolManager(tmp_path, _FakeS3())
    path = tmp_path / "aisstream_spool" / "sample.jsonl.gz"
    path.write_bytes(b"test-payload")
    assert spool._upload(path, count=1)
    assert not path.exists()
    bad_path = tmp_path / "aisstream_spool" / "bad.jsonl.gz"
    bad_path.write_bytes(b"test-payload")
    assert not SpoolManager(tmp_path, _FakeS3(wrong_hash=True))._upload(bad_path, count=1)
    assert bad_path.exists()


def test_spool_keeps_local_retry_copy_until_db_manifest_commits(tmp_path):
    def fail_ledger(_manifest):
        raise RuntimeError("db ledger unavailable")

    spool = SpoolManager(tmp_path, _FakeS3(), on_archive=fail_ledger)
    path = tmp_path / "aisstream_spool" / "ledger-fail.jsonl.gz"
    path.write_bytes(b"test-payload")
    assert not spool._upload(path, count=1)
    assert path.exists()


def test_subscription_health_waits_for_ack_or_first_valid_ais_event():
    from workers.aisstream import _is_subscription_confirmation
    assert _is_subscription_confirmation({"MessageType": "SubscriptionConfirmation"})
    assert not _is_subscription_confirmation({"MessageType": "PositionReport"})


def test_position_geom_and_static_update_do_not_touch_position_freshness(monkeypatch, tmp_path):
    from workers import aisstream as module

    execute_calls = []
    monkeypatch.setattr(module, "Json", lambda value: value, raising=False)
    monkeypatch.setattr("psycopg2.extras.execute_values", lambda cur, sql, values, **kwargs: execute_calls.append((sql, values, kwargs)), raising=True)

    class Cursor:
        def __init__(self): self.sql = []
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def execute(self, sql, params=None): self.sql.append(sql)

    class Conn:
        def __init__(self): self.cur = Cursor(); self.autocommit = True
        def cursor(self): return self.cur
        def commit(self): pass
        def rollback(self): pass

    conn = Conn()
    class Writer:
        class Context:
            def __enter__(self_inner): return conn
            def __exit__(self_inner, *_args): return False
        def with_conn(self, **_kwargs): return self.Context()

    worker = AISStreamWorker.__new__(AISStreamWorker)
    worker._writer = Writer()
    worker.run_id = "run"
    worker._static_cache = {}
    worker.stats = type("Stats", (), {"db_written": 0})()
    position = {"MessageType": "PositionReport", "MetaData": {"MMSI": 416000001, "time_utc": "2026-08-24T12:00:00Z"}, "Message": {"PositionReport": {"UserID": 416000001, "Latitude": 24.5, "Longitude": 121.5, "Timestamp": 5}}}
    static = {"MessageType": "ShipStaticData", "MetaData": {"MMSI": 416000001, "time_utc": "2026-08-24T12:00:01Z"}, "Message": {"ShipStaticData": {"UserID": 416000001, "Name": "TEST SHIP", "Type": 70}}}
    worker._write_db([(position, "2026-08-24T12:00:02+00:00"), (static, "2026-08-24T12:00:03+00:00")])
    assert execute_calls and "ST_SetSRID(ST_MakePoint" in execute_calls[0][2]["template"]
    assert len(execute_calls[0][1][0]) == 25
    static_sql = next(sql for sql in conn.cur.sql if sql.startswith("UPDATE live.aisstream_vessel_current"))
    assert "received_at=%s" not in static_sql and "payload_sha256=%s" not in static_sql


def test_spool_rotation_is_local_durable_without_s3(tmp_path):
    spool = SpoolManager(tmp_path, None)
    event = {"MessageType": "PositionReport", "Message": {"PositionReport": {"UserID": 1}}}
    spool.append(event, "2026-08-24T00:00:00+00:00")
    path = spool.rotate()
    assert path is not None
    assert path.exists()
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        record = json.loads(fh.readline())
    assert record["schema_version"] == "aisstream.raw.v1"
    assert record["event"] == event


def test_worker_preflight_requires_db_and_permanent_s3(monkeypatch, tmp_path):
    monkeypatch.setattr("config.LOCAL_DATA_DIR", tmp_path)
    monkeypatch.setattr("config.S3_BUCKET", None)
    monkeypatch.setattr("config.SUPABASE_ENABLED", False)
    worker = AISStreamWorker(api_key="test-only")
    assert worker._writer is None
    assert worker._s3 is None
    assert worker.queue.maxsize > 0
    try:
        worker.preflight()
    except RuntimeError as exc:
        assert "Supabase writer" in str(exc)
    else:
        raise AssertionError("AISStream must fail closed without DB ledger")

    worker._writer = object()
    try:
        worker.preflight()
    except RuntimeError as exc:
        assert "S3 cold archive" in str(exc)
    else:
        raise AssertionError("AISStream must fail closed without permanent S3 archive")


def test_prepare_runs_startup_gates_synchronously_once():
    worker = AISStreamWorker.__new__(AISStreamWorker)
    worker._prepared = False
    calls = []
    worker.preflight = lambda: calls.append("preflight")
    worker._write_run_start = lambda: calls.append("run_start")
    worker.spool = type("Spool", (), {"retry_pending": lambda self: calls.append("retry_pending")})()
    worker.prepare()
    worker.prepare()
    assert calls == ["preflight", "run_start", "retry_pending"]
