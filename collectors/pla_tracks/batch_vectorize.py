#!/usr/bin/env python3
"""
批次向量化共機航跡圖 → 單一 GeoJSON（PT-0 Phase 2/3）

利用「同版型底圖像素級一致」這個特性：先對每張圖各自嘗試配準，取**中位數**
作為該批的共用配準，再對全部圖（含個別配準失敗者）抽走廊。

實測依據：2026-07-27/28/29/31 各自算出的配準式幾乎相同
（lon = 0.01227·x + 115.478 / lat = -0.011111·y + 31.011），
而 2026-07-30 因走廊密集遮住左側經度線，自身配準失敗 —— 正是共用配準要救的情況。

用法：
    python3 batch_vectorize.py <img_dir> -o out.geojson
    python3 batch_vectorize.py <img_dir> -o out.geojson --debug
"""
import os
import sys
import json
import glob
import argparse

import numpy as np

from scipy import ndimage
import shapely
from shapely import affinity
from shapely.geometry import MultiPoint, mapping

from .vectorize_pla_chart import (
    load_rgb, gray_line_mask, red_mask, find_grid, fit_linear,
    DEFAULT_LONS, DEFAULT_LATS,
)


def solve_georef(path, lons, lats):
    """單張圖的配準；失敗回 None。"""
    try:
        rgb = load_rgb(path)
        xs, ys = find_grid(gray_line_mask(rgb), len(lons), len(lats))
        lat_desc = list(reversed(lats))
        ax, bx = fit_linear([p for p, _ in xs], [lons[i] for _, i in xs])
        ay, by = fit_linear([p for p, _ in ys], [lat_desc[i] for _, i in ys])
        return (ax, bx, ay, by, rgb.shape[1], rgb.shape[0])
    except Exception:
        return None


def extract(path, georef, report_date, min_area_px=300):
    """抽走廊。

    ⚠ 品質指標 `edge_dev`：紅色像素到最小外接矩形邊界距離的 90 百分位，
    除以矩形短邊長度。官方走廊畫的是**空心線框**，單一走廊的像素應全部
    貼在四條邊上 → edge_dev ≈ 0；多條走廊交叉相連被歸為同一連通元件時，
    外接矩形會把它們整片包住（實測 2026-07-30 產生橫跨 117.8–122.9°E、
    涵蓋台灣本島的假走廊），中央斜跨的線段離邊界很遠 → edge_dev 明顯偏大。

    兩個先前試過而捨棄的指標：
      · 填充率（紅色像素/矩形面積）—— 空心線框天生只有 0.03–0.23，無鑑別力
      · 空心度（內縮 70% 區域外的像素比例）—— 大矩形的線剛好都在外圍，
        黏連案例反而得到 0.94 的高分
    """
    ax, bx, ay, by = georef
    rgb = load_rgb(path)
    m = ndimage.binary_closing(red_mask(rgb), structure=np.ones((3, 3)), iterations=2)
    labels, n = ndimage.label(m, structure=np.ones((3, 3)))
    feats = []
    for i in range(1, n + 1):
        pts = np.argwhere(labels == i)
        if len(pts) < min_area_px:
            continue
        rect_px = MultiPoint([(float(x), float(y)) for y, x in pts]).minimum_rotated_rectangle
        if rect_px.geom_type != "Polygon":
            continue
        px = pts[:, 1].astype(float)
        py = pts[:, 0].astype(float)
        # 紅色像素到矩形邊界的距離（除以短邊長度正規化）。單一空心走廊的
        # 線框應全部貼邊 → 接近 0；黏連時中央會有斜跨的線 → 明顯偏大。
        d = shapely.distance(rect_px.exterior, shapely.points(px, py))
        xy = np.array(rect_px.exterior.coords[:4])
        side = min(np.hypot(*(xy[1] - xy[0])), np.hypot(*(xy[2] - xy[1]))) or 1.0
        edge_dev = float(np.percentile(d, 90) / side)
        rect = MultiPoint([(ax * float(x) + bx, ay * float(y) + by)
                           for y, x in pts]).minimum_rotated_rectangle
        if rect.geom_type != "Polygon":
            continue
        feats.append({
            "type": "Feature",
            "geometry": mapping(rect),
            "properties": {
                "report_date": report_date,
                "corridor_no": len(feats) + 1,
                "pixel_area": int(len(pts)),
                "edge_dev": round(edge_dev, 3),
                "reliable": bool(edge_dev <= 0.12),
                "note": "依國防部示意圖描繪之活動走廊，非精確航跡",
            },
        })
    return feats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("img_dir")
    ap.add_argument("-o", "--out", required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    imgs = sorted(glob.glob(os.path.join(args.img_dir, "*.jpg")))
    if not imgs:
        sys.exit("找不到圖片")

    solved, sizes = [], []
    for p in imgs:
        g = solve_georef(p, DEFAULT_LONS, DEFAULT_LATS)
        if g:
            solved.append(g[:4])
            sizes.append(g[4:])
        if args.debug:
            print(f"[georef] {os.path.basename(p)}: "
                  + ("ok" if g else "FAILED — 將套用共用配準"))
    if not solved:
        sys.exit("全部圖片配準失敗，無法建立共用配準")

    if len({s for s in sizes}) > 1:
        print(f"⚠ 圖片尺寸不一致 {set(sizes)} — 共用配準可能不適用，請分版型處理")

    arr = np.array(solved)
    georef = tuple(float(np.median(arr[:, i])) for i in range(4))
    spread = arr.max(axis=0) - arr.min(axis=0)
    print(f"共用配準（{len(solved)}/{len(imgs)} 張成功後取中位數）：")
    print(f"  lon = {georef[0]:.6f}·x + {georef[1]:.4f}")
    print(f"  lat = {georef[2]:.6f}·y + {georef[3]:.4f}")
    print(f"  離散度 {spread[0]:.2e} {spread[1]:.4f} {spread[2]:.2e} {spread[3]:.4f}")

    features = []
    for p in imgs:
        d = os.path.splitext(os.path.basename(p))[0]
        fs = extract(p, georef, d)
        features.extend(fs)
        print(f"  {d}: {len(fs)} 個走廊")

    fc = {"type": "FeatureCollection", "features": features,
          "properties": {"georef": {"lon": georef[:2], "lat": georef[2:]},
                         "images": len(imgs), "solved": len(solved)}}
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=1)
    print(f"✅ 共 {len(features)} 個走廊 → {args.out}")


if __name__ == "__main__":
    main()
