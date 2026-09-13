from __future__ import annotations
import json
from pathlib import Path
import pytest
from tasks.jp_medical_artifacts import build_plan, cleanup_eligibility, current_for, exact_upload_plan, prune_verified_raw, public_key_for, sha256_bytes

def _write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True); path.write_text(value, encoding="utf-8")

def test_public_denylist_and_traversal():
    for path in (Path("_private/a.geojson"), Path("../a.geojson"), Path("raw/a.geojson")):
        with pytest.raises(ValueError): public_key_for("jp_medical_navii", path, "x" * 64)

def test_current_bundle_preserves_index_to_shard_resolution(tmp_path):
    root=tmp_path/"analytics"; processed=root/"data/processed/world/jp_medical_navii"
    _write(processed/"current.json",json.dumps({"release_path":"releases/r1"}))
    _write(processed/"releases/r1/public-index.json",json.dumps({"shards":{"hospital":{"01":{"path":"shards/hospital/01.geojson"}}}}))
    _write(processed/"releases/r1/shards/hospital/01.geojson",'{"type":"FeatureCollection","features":[]}')
    plan=build_plan("jp_medical_navii",root); current=current_for(plan)
    index=current["files"]["public-index.json"]["key"]
    shard=json.loads((plan.release_root/"public-index.json").read_text())["shards"]["hospital"]["01"]["path"]
    assert shard in current["files"]
    assert current["files"][shard]["key"].startswith(index.rsplit("/",1)[0])
    assert (plan.release_root/shard).is_file()

def test_nested_service_detail_path_must_be_included(tmp_path):
    root=tmp_path/"analytics"; processed=root/"data/processed/world/jp_medical_navii"; release=processed/"releases/r1"
    _write(processed/"current.json",json.dumps({"release_path":"releases/r1"}))
    _write(release/"public-index.json",json.dumps({"service_details":{"clinic":{"aa":{"path":"details/aa.json"}}}}))
    with pytest.raises(ValueError,match="dependency"):
        build_plan("jp_medical_navii",root)

def test_h17_jsonl_is_only_frontend_shard_and_keeps_content_type(tmp_path):
    root=tmp_path/"analytics"; reports=root/"data/processed/world/jp_medical_reports"; release=reports/"h17_frontend/releases/r1"
    _write(reports/"h17_frontend/current.json",json.dumps({"release_path":"h17_frontend/releases/r1"}))
    _write(release/"index.json",json.dumps({"shards":[{"path":"shards/01.jsonl"}],"qa":{"nonspatial_path":"qa/nonspatial.jsonl"}}))
    _write(release/"shards/01.jsonl",'{"geometry":null,"value":1}\n');_write(release/"qa/nonspatial.jsonl",'{"geometry":null,"value":2}\n')
    plan=build_plan("jp_medical_reports",root); current=current_for(plan)
    assert "shards/01.jsonl" in current["files"]
    assert "qa/nonspatial.jsonl" in current["files"]
    assert current["content_types"]["shards/01.jsonl"] == "application/x-ndjson"

def test_pruning_is_unconditionally_deferred(tmp_path):
    protected=tmp_path/"old";protected.mkdir()
    result=prune_verified_raw([protected],current_dependencies={protected},now_epoch=9e9)
    assert result["status"] == "retained" and protected.exists()

def test_exact_payload_plan_rejects_unlisted_hash_mismatch(tmp_path):
    root=tmp_path/"analytics"; processed=root/"data/processed/world/jp_medical_navii"; raw=root/"data/raw/world/jp_medical_navii"
    _write(processed/"current.json",json.dumps({"release_path":"releases/r1"}));_write(processed/"releases/r1/a.geojson","{}")
    _write(raw/"objects/a.zip","raw")
    plan=build_plan("jp_medical_navii",root); manifest=tmp_path/"payload.json"
    manifest.write_text(json.dumps({"dataset":"jp_medical_navii","raw":[{"path":"objects/a.zip","sha256":sha256_bytes(b"raw"),"bytes":3}]}),encoding="utf-8")
    assert exact_upload_plan(plan,manifest)["raw"][0]["bucket_key"].endswith("/a.zip")
    manifest.write_text(json.dumps({"dataset":"jp_medical_navii","raw":[{"path":"objects/a.zip","sha256":"bad","bytes":3}]}),encoding="utf-8")
    with pytest.raises(ValueError,match="hash/bytes mismatch"): exact_upload_plan(plan,manifest)

def test_cleanup_needs_exact_receipt_and_protects_dependency(tmp_path):
    old=tmp_path/"old.zip";old.write_bytes(b"old"); now=old.stat().st_mtime+9*86400
    no_receipt=cleanup_eligibility([old],{},set(),now)[0]
    assert not no_receipt["eligible"] and "missing_verified_receipt" in no_receipt["reasons"]
    receipt={str(old):{"archive_verified":True,"sha256":sha256_bytes(b"old"),"bytes":3}}
    protected=cleanup_eligibility([old],receipt,{old},now)[0]
    assert not protected["eligible"] and "current_dependency" in protected["reasons"]
