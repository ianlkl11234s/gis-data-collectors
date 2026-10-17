#!/usr/bin/env python3
"""Consolidate local Global Events shadow slots with bounded memory and no network."""
from __future__ import annotations
import argparse, hashlib, json, os, sqlite3, sys, tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc
CONSOLIDATION_VERSION = "global-events-history-consolidate/v2"
REQUIRED = {"title", "url_norm", "source_domain", "published_ts", "gkg_slot"}

def atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump(value, file, ensure_ascii=False, indent=2, sort_keys=True); file.write("\n"); file.flush(); os.fsync(file.fileno())
        os.replace(temp, path)
    finally: temp.unlink(missing_ok=True)

def packed(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def create_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.executescript("""PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL;
    CREATE TABLE events(url TEXT PRIMARY KEY, canonical TEXT NOT NULL, first_slot TEXT NOT NULL, last_slot TEXT NOT NULL, streams TEXT NOT NULL, occurrences INTEGER NOT NULL);
    CREATE TABLE variants(url TEXT NOT NULL,title TEXT NOT NULL,first_slot TEXT NOT NULL,last_slot TEXT NOT NULL,streams TEXT NOT NULL,ids TEXT NOT NULL,PRIMARY KEY(url,title));
    CREATE TABLE counters(kind TEXT NOT NULL,key TEXT NOT NULL,count INTEGER NOT NULL,PRIMARY KEY(kind,key));""")
    return con

def increment(con: sqlite3.Connection, kind: str, key: str) -> None:
    con.execute("INSERT INTO counters VALUES(?,?,1) ON CONFLICT(kind,key) DO UPDATE SET count=count+1", (kind, key))

def validate(path: Path, doc: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    artifact, records = doc.get("artifact"), doc.get("records")
    if not isinstance(artifact, dict) or not isinstance(records, list): raise ValueError("slot requires artifact object and records array")
    stream, slot = artifact.get("stream"), artifact.get("slot")
    if not isinstance(stream, str) or not isinstance(slot, str): raise ValueError("slot artifact requires stream and slot")
    if path.parent.name != stream or path.stem != slot: raise ValueError("slot path does not match artifact stream/slot")
    for index, record in enumerate(records):
        if not isinstance(record, dict) or not REQUIRED.issubset(record) or not all(isinstance(record[field], str) and record[field] for field in REQUIRED): raise ValueError(f"record {index} lacks required metadata")
    return stream, records

def canonical(record: dict[str, Any]) -> dict[str, Any]:
    result = {key: record.get(key, []) for key in ("gkg_themes", "gkg_persons", "gkg_organizations", "gkg_locations")}
    result.update({"title": record["title"], "url_norm": record["url_norm"], "source_domain": record["source_domain"], "published_ts": record["published_ts"], "gkg_slot": record["gkg_slot"], "canonical_provenance": {"source_stream": record["source_stream"], "gkg_record_id": record.get("gkg_record_id"), "selection": "earliest gkg_slot, then stream/title/gkg_record_id lexical order"}})
    return result

def order(record: dict[str, Any]) -> tuple[str, str, str, str]:
    return record["gkg_slot"], record["source_stream"], record["title"], str(record.get("gkg_record_id") or "")

def upsert(con: sqlite3.Connection, record: dict[str, Any]) -> None:
    url, stream, title = record["url_norm"], record["source_stream"], record["title"]
    old = con.execute("SELECT * FROM events WHERE url=?", (url,)).fetchone()
    if old is None:
        con.execute("INSERT INTO events VALUES(?,?,?,?,?,?)", (url, packed(canonical(record)), record["gkg_slot"], record["gkg_slot"], packed([stream]), 1))
    else:
        old_canonical = json.loads(old[1]); prior = (old_canonical["gkg_slot"], old_canonical["canonical_provenance"]["source_stream"], old_canonical["title"], str(old_canonical["canonical_provenance"].get("gkg_record_id") or ""))
        selected = canonical(record) if order(record) < prior else old_canonical
        con.execute("UPDATE events SET canonical=?,first_slot=?,last_slot=?,streams=?,occurrences=? WHERE url=?", (packed(selected), min(old[2], record["gkg_slot"]), max(old[3], record["gkg_slot"]), packed(sorted(set(json.loads(old[4])) | {stream})), old[5] + 1, url))
    variant = con.execute("SELECT * FROM variants WHERE url=? AND title=?", (url, title)).fetchone(); record_id = record.get("gkg_record_id")
    if variant is None:
        con.execute("INSERT INTO variants VALUES(?,?,?,?,?,?)", (url, title, record["gkg_slot"], record["gkg_slot"], packed([stream]), packed([record_id] if record_id else [])))
    else:
        ids = json.loads(variant[5]);
        if record_id and record_id not in ids: ids.append(record_id)
        con.execute("UPDATE variants SET first_slot=?,last_slot=?,streams=?,ids=? WHERE url=? AND title=?", (min(variant[2], record["gkg_slot"]), max(variant[3], record["gkg_slot"]), packed(sorted(set(json.loads(variant[4])) | {stream})), packed(ids), url, title))

def ingest(input_dir: Path, con: sqlite3.Connection) -> tuple[list[dict[str, str]], int, int]:
    errors: list[dict[str, str]] = []; input_bytes = raw_count = 0
    for path in sorted((input_dir / "slots").glob("*/*.json")):
        try:
            input_bytes += path.stat().st_size
            with path.open(encoding="utf-8") as file: stream, records = validate(path, json.load(file))
            for original in records:
                record = {**original, "source_stream": stream}; upsert(con, record); increment(con, "domain", record["source_domain"]); increment(con, "stream", stream); increment(con, "slot", record["gkg_slot"]); raw_count += 1
            con.commit()
        except (OSError, json.JSONDecodeError, ValueError, sqlite3.Error) as exc:
            con.rollback(); errors.append({"path": str(path), "error": f"{type(exc).__name__}: {exc}"})
    return errors, input_bytes, raw_count

def stream_ndjson(con: sqlite3.Connection, path: Path) -> tuple[int, str, int]:
    path.parent.mkdir(parents=True, exist_ok=True); fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent); temp = Path(name); digest = hashlib.sha256(); count = 0
    try:
        with os.fdopen(fd, "wb") as file:
            for event in con.execute("SELECT * FROM events ORDER BY url"):
                variants = [{"title": item[1], "first_seen_slot": item[2], "last_seen_slot": item[3], "source_streams": json.loads(item[4]), "gkg_record_ids": json.loads(item[5])} for item in con.execute("SELECT * FROM variants WHERE url=? ORDER BY title", (event[0],))]
                row = json.loads(event[1]) | {"first_seen_slot": event[2], "last_seen_slot": event[3], "source_streams": json.loads(event[4]), "occurrence_count": event[5], "variant_count": len(variants), "title_variants": variants}
                encoded = (packed(row) + "\n").encode(); file.write(encoded); digest.update(encoded); count += 1
            file.flush(); os.fsync(file.fileno())
        size = temp.stat().st_size; os.replace(temp, path); return size, digest.hexdigest(), count
    finally: temp.unlink(missing_ok=True)

def counters(con: sqlite3.Connection, kind: str) -> dict[str, int]:
    return {key: value for key, value in con.execute("SELECT key,count FROM counters WHERE kind=? ORDER BY key", (kind,))}

def run(input_dir: Path, output_dir: Path) -> dict[str, Any]:
    db = output_dir / ".consolidate.sqlite3"; db.unlink(missing_ok=True); con = create_db(db)
    try:
        errors, input_bytes, raw_count = ingest(input_dir, con)
        if errors:
            report = {"consolidation_version": CONSOLIDATION_VERSION, "created_at": datetime.now(UTC).isoformat(), "status": "failed", "errors": errors, "raw_parsed_count": raw_count, "input_bytes": input_bytes, "output_bytes": 0, "temporary_db": "removed after run"}; atomic_json(output_dir / "summary.json", report); return report
        output_bytes, digest, unique_count = stream_ndjson(con, output_dir / "events.ndjson"); slots = counters(con, "slot")
        report = {"consolidation_version": CONSOLIDATION_VERSION, "created_at": datetime.now(UTC).isoformat(), "status": "succeeded", "raw_parsed_count": raw_count, "cross_slot_unique_count": unique_count, "duplicate_count": raw_count - unique_count, "domain_counts": counters(con, "domain"), "stream_counts": counters(con, "stream"), "slot_counts": slots, "time_coverage": {"first_slot": min(slots) if slots else None, "last_slot": max(slots) if slots else None, "slot_count": len(slots)}, "input_bytes": input_bytes, "output_bytes": output_bytes, "events_sha256": digest, "temporary_db": "removed after run"}; atomic_json(output_dir / "summary.json", report); return report
    finally:
        con.close(); db.unlink(missing_ok=True)

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--input-dir", type=Path, required=True); parser.add_argument("--output-dir", type=Path, required=True); args = parser.parse_args(argv)
    report = run(args.input_dir, args.output_dir)
    console = {key: value for key, value in report.items() if key not in {"domain_counts", "slot_counts"}}
    console["domain_count"] = len(report.get("domain_counts", {}))
    print(json.dumps(console, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if report["status"] == "succeeded" else 1
if __name__ == "__main__": raise SystemExit(main())
