"""Convert one v4 gzip GeoJSON track frame into a viewport/Range-friendly PMTiles asset."""
from __future__ import annotations
import argparse, gzip, hashlib, json, subprocess
import shutil, tempfile
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Callable
REPO_ROOT=Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path: sys.path.insert(0,str(REPO_ROOT))
from scripts.gfw_hourly_browser_assets import _empty_mbtiles, _run, require_gfw_asset_toolchain

SPATIAL_FRAME_ZOOM = 6

def _identity(feature: dict[str, Any]) -> str:
    properties=feature.get("properties") or {}; vessel_id=properties.get("vessel_id"); track_id=properties.get("track_id")
    if not vessel_id or not track_id: raise ValueError("track frame feature lacks vessel_id or track_id")
    return f"{vessel_id}:{track_id}"

def _decoded_identities(mbtiles: Path) -> Counter[str]:
    result=subprocess.run(["tippecanoe-decode",str(mbtiles)],check=False,capture_output=True,text=True)
    if result.returncode: raise RuntimeError(f"tippecanoe-decode failed: {result.stderr[-1000:]}")
    decoded=json.loads(result.stdout); identities: list[str]=[]
    for tile in decoded.get("features") or []:
        for layer in tile.get("features") or []:
            for feature in layer.get("features") or []: identities.append(_identity(feature))
    return Counter(identities)

def _spatial_pmtiles(*, named_inputs: list[tuple[str, Path]], output: Path, expected_identities: Counter[str]) -> None:
    """Encode exactly one complete, non-duplicated z6 viewport shard per frame."""
    tippecanoe,pmtiles=require_gfw_asset_toolchain(); output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="gfw-spatial-tippecanoe-",dir=output.parent) as temporary:
        temporary_path=Path(temporary); mbtiles=temporary_path/"asset.mbtiles"
        if expected_identities:
            command=[str(tippecanoe),"--force",f"--output={mbtiles}","--quiet",f"--minimum-zoom={SPATIAL_FRAME_ZOOM}",f"--maximum-zoom={SPATIAL_FRAME_ZOOM}","--drop-rate=1","--no-feature-limit","--no-tile-size-limit","--no-line-simplification","--no-clipping","--no-duplication"]
            for layer,source in named_inputs: command.append(f"--named-layer={layer}:{source}")
            _run(command)
            decoded=_decoded_identities(mbtiles)
            if decoded != expected_identities: raise ValueError(f"spatial frame identity mismatch: source={sum(expected_identities.values())} decoded={sum(decoded.values())}")
        else:
            _empty_mbtiles(mbtiles,layers=[layer for layer,_ in named_inputs],minimum_zoom=SPATIAL_FRAME_ZOOM,maximum_zoom=SPATIAL_FRAME_ZOOM)
        temporary_output=temporary_path/"asset.pmtiles"; _run([str(pmtiles),"convert",str(mbtiles),str(temporary_output)]); _run([str(pmtiles),"verify",str(temporary_output)])
        shown=subprocess.run([str(pmtiles),"show",str(temporary_output)],check=False,capture_output=True,text=True)
        if shown.returncode or "dropped_by_rate" in shown.stdout or f"min zoom: {SPATIAL_FRAME_ZOOM}" not in shown.stdout or f"max zoom: {SPATIAL_FRAME_ZOOM}" not in shown.stdout: raise RuntimeError("spatial PMTiles no-drop/fixed-zoom verification failed")
        temporary_output.replace(output)

def build_spatial_frame(*, source: Path, output: Path, observed_at: str, bucket: str, release_root: Path, pmtiles_builder: Callable[..., None] = _spatial_pmtiles) -> dict[str, Any]:
    value=json.loads(gzip.decompress(source.read_bytes()))
    features=value.get("features") or []
    ndjson=output.with_suffix(".ndjson")
    ndjson.parent.mkdir(parents=True, exist_ok=True)
    ndjson.write_text("".join(json.dumps(x,separators=(",",":"))+"\n" for x in features),encoding="utf-8")
    identities=Counter(_identity(feature) for feature in features)
    if any(count != 1 for count in identities.values()): raise ValueError("track frame has duplicate vessel_id+track_id identity")
    if pmtiles_builder is _spatial_pmtiles: pmtiles_builder(named_inputs=[("gfw_v4_track_frame",ndjson)],output=output,expected_identities=identities)
    else: pmtiles_builder(named_inputs=[("gfw_v4_track_frame",ndjson)],output=output,minimum_zoom=SPATIAL_FRAME_ZOOM,maximum_zoom=SPATIAL_FRAME_ZOOM)
    ndjson.unlink()
    payload=output.read_bytes(); sha=hashlib.sha256(payload).hexdigest()
    return {"path":output.relative_to(release_root).as_posix(),"type":"track_frame_pmtiles","bytes":len(payload),"content_length":len(payload),"sha256":sha,"etag":f'"{sha}"',"content_type":"application/octet-stream","content_encoding":"identity","cache_control":"public,max-age=604800,s-maxage=604800,immutable","semantic_counts":{"observed_at":observed_at,"bucket":bucket,"feature_count":len(features)},"spatial_contract":{"fixed_zoom":SPATIAL_FRAME_ZOOM,"source_feature_count":len(features),"decoded_feature_count":len(features),"identity_duplicate_count":0,"identity_missing_count":0}}

if __name__ == "__main__":
    p=argparse.ArgumentParser(); p.add_argument("--source",type=Path); p.add_argument("--output",type=Path); p.add_argument("--observed-at"); p.add_argument("--bucket"); p.add_argument("--release-root",type=Path); p.add_argument("--candidate-root",type=Path); p.add_argument("--output-root",type=Path); a=p.parse_args()
    if a.candidate_root:
      if a.output_root.exists(): raise FileExistsError(a.output_root)
      src=a.candidate_root; root=json.loads((src/'manifest.json').read_text()); rp=root['release_manifest']['path']; manifest=json.loads((src/rp).read_text()); stage=Path(tempfile.mkdtemp(prefix='.gfw-v4-spatial-',dir=a.output_root.parent)); shutil.copytree(src,stage,dirs_exist_ok=True)
      try:
       for asset in manifest['artifacts']:
        if asset['type']!='track_frame_hour': continue
        old_path=asset['path']; release_prefix=rp.rsplit('/',1)[0]+'/'
        old=stage/old_path; c=asset['semantic_counts']; new=old.with_suffix('').with_suffix('.pmtiles')
        meta=build_spatial_frame(source=old,output=new,observed_at=c['observed_at'],bucket=c['bucket'],release_root=stage); old.unlink(); asset.update(meta); matched=False
        for frame in manifest['tracks']['bucket_data'][c['bucket']]['frames']:
         if frame['path']==old_path.removeprefix(release_prefix):
          frame.update({'path':meta['path'].removeprefix(release_prefix),'format':'pmtiles','content_type':meta['content_type'],'content_encoding':'identity','cache_control':meta['cache_control'],'bytes':meta['bytes'],'sha256':meta['sha256'],'etag':meta['etag'],'content_length':meta['content_length'],'semantic_counts':meta['semantic_counts'],'spatial_contract':meta['spatial_contract']}); matched=True
        if not matched: raise ValueError(f"nested track frame not found: {old_path}")
       rpath=stage/rp; raw=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode(); rpath.write_bytes(raw); h=hashlib.sha256(raw).hexdigest(); root['release_manifest']={'path':rp,'bytes':len(raw),'sha256':h}; (stage/'manifest.json').write_text(json.dumps(root,sort_keys=True,separators=(',',':'))); stage.replace(a.output_root); print(json.dumps({'output_root':str(a.output_root),'artifacts':len(manifest['artifacts'])}))
      except Exception: raise
    else:
      print(json.dumps(build_spatial_frame(source=a.source,output=a.output,observed_at=a.observed_at,bucket=a.bucket,release_root=a.release_root,pmtiles_builder=_spatial_pmtiles),sort_keys=True))
