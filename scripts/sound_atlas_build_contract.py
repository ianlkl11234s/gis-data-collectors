#!/usr/bin/env python3
"""從前端 repo 產生聲景管線的「靜態契約包」（開發機手動跑；收集主機拿不到前端檔案）。

產出（--out，預設 sound_atlas/contract/，進 git，小檔）：
  grid_spec.json          前端 src/sound/grid/gridSpec.json 逐位元組複製（前端驗網格 sha256）
  rain_classes.json       前端 src/sound/grid/rainClasses.json 逐位元組複製
  cwa_radar_palette.json  前端 scripts/preprocess/sound/cwa-radar-palette.json 逐位元組複製
  grid_mask_spans.json    前端 src/sound/grid/gridMask.generated.ts 的列區段（[[c0,c1,county],…] 每列一組）
  freeway_vd_ids.json     前端 public/sound-atlas/grid/road-network-v1.json 國道路段用到的 VD（排序）
  river_stations.json     前端 public/geo/water_monitor_stations.geojson 的 river_level 站 {id: [lon, lat, name]}
  manifest.json           以上各檔 sha256＋來源路徑與前端 commit

另產（--static-out，不進 git；正式環境放 R2 sound-atlas/v1/static/，見 DESIGN §5）：
  natural-static-v1.json  natural 每日檔的 coast＋river_network 兩塊（靜態幾何，約 1.1 MB），
                          從一份既有 natural-<D>.json 原樣取出（--natural-sample）。

用法：
  python3 scripts/sound_atlas_build_contract.py --frontend <mini-taiwan-pulse worktree> \\
      --natural-sample <worktree>/public/sound-atlas/grid/natural-2026-11-09.json --static-out <dir>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

COPIES = {
    "grid_spec.json": "src/sound/grid/gridSpec.json",
    "rain_classes.json": "src/sound/grid/rainClasses.json",
    "cwa_radar_palette.json": "scripts/preprocess/sound/cwa-radar-palette.json",
}


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def find_public(frontend: Path, rel: str) -> Path:
    """public 大檔不在 worktree 時退回主 checkout（同 build-sound-scenes.py 的規則）"""
    p = frontend / rel
    if p.exists():
        return p
    if frontend.parent.name == ".worktrees":
        q = frontend.parent.parent / rel
        if q.exists():
            return q
    raise SystemExit(f"找不到 {rel}")


def mask_spans(ts_text: str) -> list:
    body = ts_text[ts_text.index("GRID_MASK_ROW_SPANS"):]
    body = body[body.index("= [") + 2:]
    return json.loads(re.sub(r",\s*\]", "]", body[: body.rindex("];") + 1]))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--frontend", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=ROOT / "sound_atlas" / "contract")
    ap.add_argument("--natural-sample", type=Path)
    ap.add_argument("--static-out", type=Path)
    a = ap.parse_args()
    fe = a.frontend.resolve()
    a.out.mkdir(parents=True, exist_ok=True)
    manifest: dict = {"frontend_commit": subprocess.run(["git", "-C", str(fe), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
                      "files": {}}
    for name, rel in COPIES.items():
        b = (fe / rel).read_bytes()
        (a.out / name).write_bytes(b)
        manifest["files"][name] = {"sha256": sha(b), "source": rel, "rule": "byte copy"}

    spans = mask_spans((fe / "src/sound/grid/gridMask.generated.ts").read_text(encoding="utf-8"))
    derived = {
        "grid_mask_spans.json": (spans, "src/sound/grid/gridMask.generated.ts", "GRID_MASK_ROW_SPANS"),
    }
    net_path = find_public(fe, "public/sound-atlas/grid/road-network-v1.json")
    net = json.loads(net_path.read_bytes())
    derived["freeway_vd_ids.json"] = (sorted({p["vd"] for p in net["freeway"] if p["vd"]}), "public/sound-atlas/grid/road-network-v1.json",
                                      "sorted({p.vd for p in freeway if p.vd})（build-sound-road-grid.py vd 的 used）")
    st_path = find_public(fe, "public/geo/water_monitor_stations.geojson")
    st = json.loads(st_path.read_bytes())
    stations = {ft["id"]: [ft["geometry"]["coordinates"][0], ft["geometry"]["coordinates"][1], ft["properties"].get("name")]
                for ft in st["features"] if ft["properties"].get("station_type") == "river_level"}
    derived["river_stations.json"] = (stations, "public/geo/water_monitor_stations.geojson", "station_type == river_level → {id: [lon, lat, name]}")
    for name, (obj, src, rule) in derived.items():
        b = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        (a.out / name).write_bytes(b)
        src_path = fe / src if (fe / src).exists() else (net_path if "road-network" in src else st_path)
        manifest["files"][name] = {"sha256": sha(b), "source": src, "source_sha256": sha(src_path.read_bytes()), "rule": rule}

    if a.natural_sample:
        doc = json.loads(a.natural_sample.read_bytes())
        nat = doc["natural"]
        static = {"schema": "pulse-sound-natural-static-v1", "coast": nat.get("coast"), "river_network": nat.get("river_network"),
                  "extracted_from": a.natural_sample.name}
        b = json.dumps(static, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        manifest["natural_static"] = {"file": "natural-static-v1.json", "sha256": sha(b), "bytes": len(b),
                                      "note": "不進 git；正式環境放 R2 sound-atlas/v1/static/natural-static-v1-<sha8>.json"}
        if a.static_out:
            a.static_out.mkdir(parents=True, exist_ok=True)
            (a.static_out / "natural-static-v1.json").write_bytes(b)
    (a.out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    for k, v in manifest["files"].items():
        print(f"{k:24s} {v['sha256'][:12]}")
    if "natural_static" in manifest:
        print("natural-static-v1.json", manifest["natural_static"]["sha256"][:12], manifest["natural_static"]["bytes"], "B")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
