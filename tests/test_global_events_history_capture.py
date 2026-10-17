import hashlib
import io
import json
import zipfile
from datetime import datetime, timezone

import pytest
import requests

from scripts.global_events_history_capture import ShadowCapture, requested_slots


def _gkg_zip(title="Major earthquake kills dozens"):
    row = [
        "20260901120000-id",
        "20260901120000",
        "1",
        "news.example",
        "https://news.example/story?utm_source=fixture",
    ] + [""] * 22
    row[26] = f"<PAGE_TITLE>{title}</PAGE_TITLE>"
    raw = ("\t".join(row) + "\n").encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("20260901120000.gkg.csv", raw)
    return output.getvalue()


class _Response:
    def __init__(self, *, text=None, content=None, status=200):
        self.text = text
        self.content = content or b""
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(response=self)


class _Session:
    def __init__(self, responses):
        self.responses = responses
        self.headers = {}

    def get(self, url, timeout):
        return self.responses[url]


def test_capture_writes_replayable_metadata_checkpoint_and_skips_completed(tmp_path, monkeypatch):
    payload = _gkg_zip()
    url = "https://example.test/20260901120000.gkg.csv.zip"
    index_url = "https://example.test/index"
    monkeypatch.setattr(
        "scripts.global_events_history_capture.index_urls", lambda: {"standard": index_url}
    )
    index = f"{len(payload)} {hashlib.md5(payload).hexdigest()} {url}\n"
    capture = ShadowCapture(
        tmp_path,
        _Session({index_url: _Response(text=index), url: _Response(content=payload)}),
    )

    report = capture.run([datetime(2026, 9, 1, 12, tzinfo=timezone.utc)], ["standard"])

    assert report["status"] == "succeeded"
    assert report["totals"]["input_count"] == 1
    assert report["totals"]["unique_count"] == 1
    assert report["totals"]["estimated_model_input_tokens"] > 0
    assert len(report["source_manifest_sha256"]) == 64
    slot = json.loads((tmp_path / "slots/standard/20260901120000.json").read_text())
    assert slot["records"][0]["title"] == "Major earthquake kills dozens"
    assert "body" not in slot["records"][0]
    assert slot["download_sha256"] == hashlib.sha256(payload).hexdigest()
    checkpoint = json.loads((tmp_path / "checkpoint.json").read_text())
    assert checkpoint["slots"]["standard/20260901120000"]["status"] == "succeeded"

    replay = capture.run([datetime(2026, 9, 1, 12, tzinfo=timezone.utc)], ["standard"])
    assert replay["slots"][0]["status"] == "skipped_checkpoint"


def test_missing_index_slot_is_failed_not_empty(tmp_path, monkeypatch):
    index_url = "https://example.test/index"
    monkeypatch.setattr(
        "scripts.global_events_history_capture.index_urls", lambda: {"standard": index_url}
    )
    capture = ShadowCapture(tmp_path, _Session({index_url: _Response(text="")}))

    report = capture.run([datetime(2026, 9, 1, 12, tzinfo=timezone.utc)], ["standard"])

    assert report["status"] == "failed"
    assert report["slots"] == [
        {
            "stream": "standard",
            "slot": "20260901120000",
            "status": "missing_from_index",
            "input_count": 0,
            "unique_count": 0,
            "late_count": 0,
            "downloaded_bytes": 0,
            "metadata_bytes": 0,
            "wall_seconds": 0.0,
            "estimated_model_input_tokens": 0,
            "token_estimate_method": "UTF-8 metadata bytes / 4, rounded up",
            "error": "requested slot absent from fetched master index",
        }
    ]


def test_run_once_uses_last_completed_15_minute_slot():
    args = type("Args", (), {"mode": "run-once", "start": None, "end": None, "hours": 24})()
    assert requested_slots(args, datetime(2026, 9, 1, 12, 23, tzinfo=timezone.utc)) == [
        datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    ]
