"""Read-only Japan medical raw/archive and static bundle planning contract."""
from __future__ import annotations
import argparse, hashlib, json, sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

PUBLIC_DATASETS=frozenset({"jp_medical_navii","jp_medical_idwr","jp_medical_reports","jp_medical_frontend"})
PUBLIC_EXTENSIONS=frozenset({".geojson",".json",".pmtiles"})
PRIVATE_PARTS=frozenset({"raw","private","unreviewed","raw_only","xlsx_raw_only"})
IMMUTABLE_CACHE="public,max-age=31536000,immutable"; CURRENT_CACHE="public,max-age=60"
def sha256_bytes(value:bytes)->str:return hashlib.sha256(value).hexdigest()
def _canonical(value:object)->bytes:return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":")).encode()
def _safe_relative(path:Path)->Path:
    if path.is_absolute() or ".." in path.parts: raise ValueError(f"relative artifact path is unsafe: {path}")
    if any(part.lower().lstrip("_") in PRIVATE_PARTS for part in path.parts): raise ValueError(f"private or unreviewed artifact denied: {path}")
    return path
def _private_path(path:Path)->bool:
    return any(part.lower().lstrip("_") in PRIVATE_PARTS for part in path.parts)
def public_key_for(dataset:str,relative:Path,bundle_hash:str)->str:
    if dataset not in PUBLIC_DATASETS: raise ValueError(f"dataset is not public allowlisted: {dataset}")
    relative=_safe_relative(relative)
    is_catalog_jsonl=(dataset in {"jp_medical_reports","jp_medical_frontend"} and relative.suffix.lower()==".jsonl")
    if relative.suffix.lower() not in PUBLIC_EXTENSIONS and not is_catalog_jsonl: raise ValueError(f"public artifact extension denied: {relative.suffix or '(none)'}")
    return f"deploy-assets/medical/jp/{dataset}/releases/{bundle_hash}/{relative.as_posix()}"
@dataclass(frozen=True)
class BundlePlan:
    dataset:str; analytics_root:Path; processed_root:Path; raw_root:Path; release_root:Path; bundle_hash:str
    artifacts:tuple[tuple[Path,Path],...]; raw_files:tuple[Path,...]
def _read_json(path:Path)->dict[str,Any]:
    value=json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value,dict): raise ValueError(f"expected JSON object: {path}")
    return value
def _release_root(dataset:str,processed:Path)->Path:
    if dataset=="jp_medical_frontend":
        current=_read_json(processed/"current.json"); candidate=current.get("catalog")
        if not isinstance(candidate,str): raise ValueError("frontend catalog path unavailable")
        catalog=(processed/_safe_relative(Path(candidate))).resolve()
        if processed.resolve() not in catalog.parents or not catalog.is_file(): raise ValueError("frontend catalog escapes processed root or does not exist")
        return catalog.parent
    if dataset=="jp_medical_reports":
        pointer=_read_json(processed/"h17_frontend/current.json")
        candidate=pointer.get("release_path")
        if not isinstance(candidate,str):raise ValueError("H17 release path unavailable")
        root=(processed/_safe_relative(Path(candidate))).resolve()
        if processed.resolve() not in root.parents or not root.is_dir():raise ValueError("H17 release escapes processed root")
        return root
    current=_read_json(processed/"current.json")
    if dataset=="jp_medical_navii": candidate=current.get("release_path")
    elif dataset=="jp_medical_idwr": candidate=(current.get("releases") or {}).get(current.get("latest"),{}).get("path")
    else: raise ValueError(f"unknown dataset: {dataset}")
    if not isinstance(candidate,str): raise ValueError(f"current pointer lacks usable release path for {dataset}")
    root=(processed/_safe_relative(Path(candidate))).resolve()
    if processed.resolve() not in root.parents or not root.is_dir(): raise ValueError("current release escapes processed root or does not exist")
    return root
def _validate_index_dependencies(release:Path, included:set[Path])->None:
    """Validate every nested `path` in current/public-index before publication."""
    for index in (release/"current.json",release/"public-index.json",release/"index.json"):
        if not index.is_file(): continue
        def walk(value:Any)->None:
            if isinstance(value,dict):
                for key,item in value.items():
                    if (key=="path" or key.endswith("_path") and key not in {"index_path","release_path"}) and isinstance(item,str):
                        rel=_safe_relative(Path(item)); target=(release/rel).resolve()
                        if release.resolve() not in target.parents or not target.is_file() or rel not in included:
                            raise ValueError(f"index dependency is missing or excluded: {item}")
                    walk(item)
            elif isinstance(value,list):
                for item in value: walk(item)
        walk(_read_json(index))
def build_plan(dataset:str,analytics_root:Path)->BundlePlan:
    if dataset not in PUBLIC_DATASETS: raise ValueError(f"dataset is not allowlisted: {dataset}")
    root=analytics_root.resolve(); processed=root/"data/processed/world"/dataset; raw=root/"data/raw/world"/dataset; release=_release_root(dataset,processed); artifacts=[]
    if dataset=="jp_medical_frontend":
        catalog=release/"catalog.json"
        if not catalog.is_file(): raise ValueError("frontend release must include catalog.json")
        entries=_read_json(catalog).get("files")
        if not isinstance(entries,dict): raise ValueError("frontend catalog requires files object")
        artifacts.append((catalog,Path("catalog.json")))
        for path,expected in sorted(entries.items()):
            if not isinstance(path,str) or not isinstance(expected,dict): raise ValueError("invalid frontend catalog file entry")
            relative=_safe_relative(Path(path)); source=(release/relative).resolve()
            if release.resolve() not in source.parents or not source.is_file(): raise ValueError(f"catalog file unavailable: {path}")
            if relative.suffix.lower() not in PUBLIC_EXTENSIONS and relative.suffix.lower()!=".jsonl": raise ValueError(f"catalog file extension denied: {path}")
            body=source.read_bytes()
            if expected.get("sha256")!=sha256_bytes(body) or expected.get("bytes")!=len(body): raise ValueError(f"catalog file hash/bytes mismatch: {path}")
            if relative == Path("catalog.json"): continue
            artifacts.append((source,relative))
    else:
        for source in sorted(release.rglob("*")):
            if source.is_file():
                relative=source.relative_to(release)
                if _private_path(relative): continue
                relative=_safe_relative(relative)
                is_h17_shard=(dataset=="jp_medical_reports" and relative.parts[:1] in {("shards",),("qa",)} and relative.suffix.lower()==".jsonl")
                if relative.suffix.lower() in PUBLIC_EXTENSIONS or is_h17_shard: artifacts.append((source,relative))
    if not artifacts: raise ValueError("current release has no public artifacts")
    _validate_index_dependencies(release,{rel for _,rel in artifacts})
    inventory=[{"path":rel.as_posix(),"sha256":sha256_bytes(src.read_bytes()),"bytes":src.stat().st_size} for src,rel in artifacts]
    return BundlePlan(dataset,root,processed,raw,release,sha256_bytes(_canonical({"dataset":dataset,"files":inventory})),tuple(artifacts),tuple(sorted(p for p in raw.rglob("*") if p.is_file())))
def current_for(plan:BundlePlan)->dict[str,Any]:
    files={rel.as_posix():{"key":public_key_for(plan.dataset,rel,plan.bundle_hash),"sha256":sha256_bytes(src.read_bytes()),"bytes":src.stat().st_size} for src,rel in plan.artifacts}
    current={"dataset_id":plan.dataset,"bundle_sha256":plan.bundle_hash,"release_id":plan.bundle_hash,"files":files,
             "content_types": {path: ("application/x-ndjson" if path.endswith(".jsonl") else "application/vnd.pmtiles" if path.endswith(".pmtiles") else "application/json") for path in files}}
    if plan.dataset=="jp_medical_frontend":
        current.update({"version":plan.bundle_hash,"catalog":f"releases/{plan.bundle_hash}/catalog.json"})
    return current
def exact_upload_plan(plan:BundlePlan,payload_manifest:Path)->dict[str,Any]:
    """Validate an explicitly named raw payload list; never scan/upload a root."""
    manifest=_read_json(payload_manifest)
    if manifest.get("dataset")!=plan.dataset: raise ValueError("payload manifest dataset mismatch")
    entries=manifest.get("raw")
    if not isinstance(entries,list) or (not entries and plan.dataset!="jp_medical_frontend"): raise ValueError("payload manifest requires non-empty raw list")
    raw=[]
    for entry in entries:
        if not isinstance(entry,dict) or not isinstance(entry.get("path"),str): raise ValueError("invalid raw payload entry")
        relative=_safe_relative(Path(entry["path"])); source=(plan.raw_root/relative).resolve()
        if plan.raw_root.resolve() not in source.parents or not source.is_file(): raise ValueError(f"raw payload unavailable: {relative}")
        body=source.read_bytes(); digest=sha256_bytes(body)
        if entry.get("sha256")!=digest or entry.get("bytes")!=len(body): raise ValueError(f"raw payload hash/bytes mismatch: {relative}")
        raw.append({"path":relative.as_posix(),"sha256":digest,"bytes":len(body),"bucket_key":f"jp_medical/raw/sha256/{digest}/{source.name}"})
    return {"schema_version":1,"dataset":plan.dataset,"write":False,"raw":raw,"current":current_for(plan),"prune":prune_verified_raw()}
def cleanup_eligibility(raw_files:list[Path],receipts:dict[str,dict[str,Any]],current_dependencies:set[Path],now_epoch:float,retention_days:int=7)->list[dict[str,Any]]:
    """Dry-run only: exact file is eligible only after verified matching receipt."""
    protected={path.resolve() for path in current_dependencies}; cutoff=now_epoch-retention_days*86400; result=[]
    for source in raw_files:
        resolved=source.resolve(); receipt=receipts.get(str(source),{}); body=source.read_bytes(); digest=sha256_bytes(body)
        reasons=[]
        if any(resolved == dependency or dependency in resolved.parents for dependency in protected): reasons.append("current_dependency")
        if source.stat().st_mtime>=cutoff: reasons.append("within_retention")
        if receipt.get("archive_verified") is not True: reasons.append("missing_verified_receipt")
        if receipt.get("sha256")!=digest or receipt.get("bytes")!=len(body): reasons.append("receipt_hash_or_bytes_mismatch")
        result.append({"path":str(source),"eligible":not reasons,"reasons":reasons})
    return result
def prune_verified_raw(*_args:object,**_kwargs:object)->dict[str,str]:
    return {"status":"retained","reason":"safe pruning deferred: per-object archive and nested-current dependency proof is not implemented"}
def execute_plan(plan:BundlePlan)->dict[str,Any]:
    return {"dataset":plan.dataset,"write":False,"raw":[str(p) for p in plan.raw_files],"bundle":[{"path":r.as_posix(),"key":public_key_for(plan.dataset,r,plan.bundle_hash)} for _,r in plan.artifacts],"current":current_for(plan),"prune":prune_verified_raw()}
def main()->int:
    p=argparse.ArgumentParser(description="Japan medical artifact read-only plan");p.add_argument("--plan",action="store_true",required=True);p.add_argument("--dataset",choices=sorted(PUBLIC_DATASETS),required=True);p.add_argument("--analytics-root",type=Path,required=True);p.add_argument("--payload-manifest",type=Path);p.add_argument("--output",type=Path);a=p.parse_args()
    plan=build_plan(a.dataset,a.analytics_root); value=exact_upload_plan(plan,a.payload_manifest) if a.payload_manifest else execute_plan(plan)
    encoded=json.dumps(value,ensure_ascii=False,indent=2)
    if a.output: a.output.write_text(encoded+"\n",encoding="utf-8")
    else: print(encoded)
    return 0
if __name__=="__main__":raise SystemExit(main())
