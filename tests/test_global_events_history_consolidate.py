import json
import hashlib

from scripts.global_events_history_consolidate import run


def _slot(root, stream, slot, records):
    path = root / "slots" / stream / f"{slot}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"artifact": {"stream": stream, "slot": slot}, "records": records}), encoding="utf-8")


def _record(url, title, slot, record_id):
    return {
        "title": title,
        "url_norm": url,
        "source_domain": "news.example",
        "published_ts": "2026-09-01T12:00:00+00:00",
        "gkg_slot": slot,
        "gkg_themes": ["DISASTER"],
        "gkg_persons": ["Person"],
        "gkg_organizations": ["Org"],
        "gkg_locations": [{"name": "City"}],
        "gkg_record_id": record_id,
    }


def test_cross_stream_duplicate_keeps_variants_and_provenance(tmp_path):
    capture, output = tmp_path / "capture", tmp_path / "out"
    _slot(capture, "standard", "20260901120000", [_record("https://news.example/a", "Initial title", "20260901120000", "a")])
    _slot(capture, "translation", "20260901121500", [_record("https://news.example/a", "Updated title", "20260901121500", "b")])

    summary = run(capture, output)

    assert summary["status"] == "succeeded"
    assert summary["raw_parsed_count"] == 2
    assert summary["cross_slot_unique_count"] == 1
    assert summary["duplicate_count"] == 1
    row = json.loads((output / "events.ndjson").read_text())
    assert row["title"] == "Initial title"
    assert row["source_streams"] == ["standard", "translation"]
    assert row["variant_count"] == 2
    assert [item["title"] for item in row["title_variants"]] == ["Initial title", "Updated title"]


def test_same_url_same_title_still_preserves_cross_slot_provenance(tmp_path):
    capture, output = tmp_path / "capture", tmp_path / "out"
    _slot(capture, "standard", "20260901120000", [_record("https://news.example/a", "Same", "20260901120000", "a")])
    _slot(capture, "standard", "20260901121500", [_record("https://news.example/a", "Same", "20260901121500", "b")])

    summary = run(capture, output)

    row = json.loads((output / "events.ndjson").read_text())
    assert summary["duplicate_count"] == 1
    assert row["first_seen_slot"] == "20260901120000"
    assert row["last_seen_slot"] == "20260901121500"
    assert row["title_variants"][0]["gkg_record_ids"] == ["a", "b"]


def test_bad_slot_file_fails_without_success_ndjson(tmp_path):
    capture, output = tmp_path / "capture", tmp_path / "out"
    path = capture / "slots" / "standard" / "20260901120000.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    summary = run(capture, output)

    assert summary["status"] == "failed"
    assert summary["errors"]
    assert not (output / "events.ndjson").exists()
    assert json.loads((output / "summary.json").read_text())["status"] == "failed"


def test_many_small_slots_stream_to_sorted_ndjson_and_remove_temporary_db(tmp_path):
    capture, output = tmp_path / "large-capture", tmp_path / "out"
    for index in range(120):
        slot = f"202609{2 + index // 4:02d}00{index % 4 * 15:02d}00"
        _slot(capture, "standard", slot, [_record(f"https://news.example/{119-index:03d}", f"Title {index}", slot, str(index))])
    summary = run(capture, output)
    lines = (output / "events.ndjson").read_bytes()

    assert summary["cross_slot_unique_count"] == 120
    assert summary["events_sha256"] == hashlib.sha256(lines).hexdigest()
    assert [json.loads(line)["url_norm"] for line in lines.splitlines()] == sorted(json.loads(line)["url_norm"] for line in lines.splitlines())
    assert not (output / ".consolidate.sqlite3").exists()
