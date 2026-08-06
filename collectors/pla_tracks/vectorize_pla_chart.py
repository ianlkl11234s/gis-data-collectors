#!/usr/bin/env python3
"""
共機航跡示意圖 → GeoJSON 活動走廊（PT-0 計畫 Phase 1+2 POC）

國防部「中共解放軍進入臺海周邊空域活動示意圖」的兩個關鍵特性讓向量化可行：
  1. 圖上**自帶經緯度網格**（117–123°E × 21–29°N，每度一條線並標數字）
     → 用格線位置直接解像素↔經緯度的線性轉換，不需目視標控制點。
  2. 航跡是**紅色矩形活動走廊**（非細箭頭線）→ 顏色分割 + 最小旋轉矩形即可。

不依賴 OpenCV（PEP 668 環境裝不了）：PIL 讀圖 + numpy 分割 +
scipy.ndimage 連通元件 + shapely minimum_rotated_rectangle。

用法：
    python3 vectorize_pla_chart.py <chart.jpg> --date 2024-11-08 -o out.geojson
    python3 vectorize_pla_chart.py <chart.jpg> --debug    # 印診斷資訊
"""
import os
import sys
import json
import argparse

import numpy as np
from PIL import Image
from scipy import ndimage
from shapely.geometry import MultiPoint, mapping

# 圖上網格的經緯度範圍（實測 2024-11 版型；跨版型時由 --lon/--lat 覆寫）
DEFAULT_LONS = list(range(117, 124))      # 117°E … 123°E
DEFAULT_LATS = list(range(21, 30))        # 21°N … 29°N


def load_rgb(path: str) -> np.ndarray:
    return np.asarray(Image.open(path).convert("RGB"), dtype=np.int16)


def red_mask(rgb: np.ndarray) -> np.ndarray:
    """紅色走廊遮罩。底圖為灰階線條 + 黑字，紅色通道明顯高於藍綠即為走廊。"""
    r, g, b = rgb[:, :, 0], rgb[:, :, 1], rgb[:, :, 2]
    return (r > 110) & (r - g > 45) & (r - b > 45)


def gray_line_mask(rgb: np.ndarray) -> np.ndarray:
    """底圖線條（灰/黑、低飽和）遮罩 — 用來找網格線。"""
    r, g, b = rgb[:, :, 0], rgb[:, :, 1], rgb[:, :, 2]
    mx = np.maximum(np.maximum(r, g), b)
    mn = np.minimum(np.minimum(r, g), b)
    return (mx < 200) & ((mx - mn) < 30)


def _candidate_lines(mask: np.ndarray, axis: int, min_frac: float,
                     roi: tuple[int, int] | None = None,
                     max_thick: int = 6) -> list[int]:
    """投影法取候選線（含雜訊，如標題筆畫與左上角表格框線）。

    roi 限制投影只在繪圖區內累加（axis=0 → y 範圍；axis=1 → x 範圍），
    搭配較高的 min_frac 可濾掉「只橫跨局部」的表格線與標題文字。
    """
    m = mask
    if roi is not None:
        lo, hi = roi
        m = mask[lo:hi, :] if axis == 0 else mask[:, lo:hi]
    proj = m.sum(axis=0) if axis == 0 else m.sum(axis=1)
    length = m.shape[0] if axis == 0 else m.shape[1]
    cand = np.where(proj > length * min_frac)[0]
    if len(cand) == 0:
        return []
    groups, cur = [], [cand[0]]
    for x in cand[1:]:
        if x - cur[-1] <= 3:
            cur.append(x)
        else:
            groups.append(cur)
            cur = [x]
    groups.append(cur)
    # 網格線細（1–3 px）；過厚的群組是文字行或圖例色塊，濾掉
    return [int(round(float(np.average(g, weights=proj[g]))))
            for g in groups if len(g) <= max_thick]


def pick_equispaced(cand: list[int], expect: int, tol: float = 8.0,
                    expected_d: float | None = None,
                    d_tol: float = 0.12) -> list[tuple[int, int]]:
    """從候選線中挑出**等距**的那組 = 真正的經緯度網格。

    圖上除網格外還有左上角統計表格的框線，會混進候選（實測 2024-11 版
    多出 1 條垂直、4 條水平），若直接依序對應經緯度會整組錯位。
    網格線必然等距（等經緯度間隔），故以 RANSAC 式窮舉：任取兩條線並
    假設相隔 k 格 → 推出間距 d → 數 inlier，取 inlier 最多者。

    回傳 [(pixel, grid_index)]，grid_index = 該線是網格的第幾格（0-based）。
    帶格號才能在「中間某條線沒偵測到」時仍對應到正確的經緯度值。
    """
    if len(cand) < 2:
        return [(c, i) for i, c in enumerate(cand)]
    best: tuple[int, list[tuple[int, int]]] = (0, [])
    for i in range(len(cand)):
        for j in range(i + 1, len(cand)):
            for k in range(1, expect):
                d = (cand[j] - cand[i]) / k
                if d < 8:                       # 間距過小 = 不可能是網格
                    continue
                # 已知預期間距時（緯度線常被地圖內容遮擋、候選過少易誤判 k），
                # 只接受接近預期值的假設
                if expected_d is not None and abs(d - expected_d) / expected_d > d_tol:
                    continue
                for shift in range(expect):     # cand[i] 可能是第 shift 格
                    origin = cand[i] - shift * d
                    hit: list[tuple[int, int]] = []
                    for m in range(expect):
                        target = origin + m * d
                        near = [c for c in cand if abs(c - target) <= tol]
                        if near:
                            hit.append((min(near, key=lambda c: abs(c - target)), m))
                    # 同一像素被配到多格 → 該假設不成立
                    if len({p for p, _ in hit}) != len(hit):
                        continue
                    if len(hit) > best[0]:
                        best = (len(hit), sorted(hit))
    return best[1]


def find_grid(mask: np.ndarray, n_lon: int, n_lat: int, mid_lat: float = 25.0,
              debug: bool = False) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """兩階段偵測經緯網格，回傳 (垂直線, 水平線)，元素為 (像素, 網格索引)。

    1. 先抓垂直線（不受標題／表格橫筆畫干擾）→ 得繪圖區 x 範圍與每度像素數。
    2. 水平線只在繪圖區內累加（排除標題文字與統計表格橫線 —— 實測不排除會
       多抓一條而使整組緯度**偏移一格**），並以物理先驗約束間距：
       等距圓柱投影下 `y每度 ≈ x每度 / cos(lat)`（實測 81.2/89.6 = 0.906，
       與 cos(25°)=0.906 完全吻合）。緯線常被台灣島與 ADIZ 線遮擋而候選稀少，
       沒有這個先驗時 RANSAC 會把「相隔 8 格」誤判成「相隔 1 格」。
    """
    xs = pick_equispaced(_candidate_lines(mask, 0, 0.35), n_lon)
    if len(xs) < 2:
        raise RuntimeError("垂直網格線偵測失敗")
    x_lo, x_hi = min(p for p, _ in xs), max(p for p, _ in xs)
    dx = (x_hi - x_lo) / (max(i for _, i in xs) - min(i for _, i in xs))
    dy_expect = dx / np.cos(np.radians(mid_lat))

    # 緯線常被台灣島／ADIZ 線遮擋，候選稀疏且混有標題筆畫與表格橫線。
    # 最穩健的錨點是**圖框上下邊**（= 最高與最低緯線，實線橫跨全寬）：
    # 在候選中找跨度最接近 (n_lat-1)*dy_expect 的一對即可定出整個緯度尺度。
    cand_y = _candidate_lines(mask, 1, 0.55, roi=(x_lo, x_hi))
    span_expect = (n_lat - 1) * dy_expect
    best, ys = None, []
    for i in range(len(cand_y)):
        for j in range(i + 1, len(cand_y)):
            err = abs((cand_y[j] - cand_y[i]) - span_expect)
            if best is None or err < best:
                best, ys = err, [(cand_y[i], 0), (cand_y[j], n_lat - 1)]
    if debug:
        print(f"[debug] plot area x=[{x_lo},{x_hi}] dx={dx:.2f}px/deg "
              f"dy_expect={dy_expect:.2f} span_expect={span_expect:.1f} "
              f"span_err={best:.1f}\n[debug] cand_y={cand_y}")
    if best is not None and best > 0.06 * span_expect:
        raise RuntimeError(f"緯度框線跨度不合理（誤差 {best:.1f}px）")
    if len(ys) < 2:
        raise RuntimeError("水平網格線偵測失敗")
    return xs, ys


def fit_linear(pixels: list[int], values: list[float]) -> tuple[float, float]:
    """最小平方擬合 value = a*pixel + b。"""
    a, b = np.polyfit(np.array(pixels, dtype=float), np.array(values, dtype=float), 1)
    return float(a), float(b)


def vectorize(path: str, report_date: str, lons=None, lats=None,
              min_area_px: int = 400, debug: bool = False) -> dict:
    lons = lons or DEFAULT_LONS
    lats = lats or DEFAULT_LATS
    rgb = load_rgb(path)
    h, w = rgb.shape[:2]

    # ── 1. 配準：偵測網格線 → 像素↔經緯度線性轉換 ──────────────
    gmask = gray_line_mask(rgb)
    xs, ys = find_grid(gmask, len(lons), len(lats), debug=debug)
    if debug:
        print(f"[debug] size={w}x{h} vlines={len(xs)} hlines={len(ys)}")
        print(f"[debug] xs={xs}\n[debug] ys={ys}")
    if len(xs) < 2 or len(ys) < 2:
        raise RuntimeError(f"網格線偵測失敗（v={len(xs)} h={len(ys)}）")

    # 依網格索引取值：x 由左至右 = 經度遞增；y 由上至下 = 緯度遞減
    lat_desc = list(reversed(lats))
    ax, bx = fit_linear([p for p, _ in xs], [lons[i] for _, i in xs])
    ay, by = fit_linear([p for p, _ in ys], [lat_desc[i] for _, i in ys])
    if debug:
        print(f"[debug] lon = {ax:.6f}*x + {bx:.4f} | lat = {ay:.6f}*y + {by:.4f}")

    def px2lonlat(x: float, y: float) -> tuple[float, float]:
        return (ax * x + bx, ay * y + by)

    # ── 2. 走廊：紅色分割 → 連通元件 → 最小旋轉矩形 ──────────────
    rmask = red_mask(rgb)
    rmask = ndimage.binary_closing(rmask, structure=np.ones((3, 3)), iterations=2)
    labels, n = ndimage.label(rmask, structure=np.ones((3, 3)))
    if debug:
        print(f"[debug] red_px={int(rmask.sum())} components={n}")

    features = []
    for i in range(1, n + 1):
        pts = np.argwhere(labels == i)          # (y, x)
        if len(pts) < min_area_px:
            continue
        ll = [px2lonlat(float(x), float(y)) for y, x in pts]
        rect = MultiPoint(ll).minimum_rotated_rectangle
        if rect.geom_type != "Polygon":
            continue
        features.append({
            "type": "Feature",
            "geometry": mapping(rect),
            "properties": {
                "report_date": report_date,
                "corridor_no": len(features) + 1,
                "pixel_area": int(len(pts)),
                "source_chart": os.path.basename(path),
                "note": "依國防部示意圖描繪之活動走廊，非精確航跡",
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "properties": {
            "report_date": report_date,
            "georef": {"lon": [ax, bx], "lat": [ay, by],
                       "vlines": len(xs), "hlines": len(ys)},
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("image")
    ap.add_argument("--date", required=True)
    ap.add_argument("-o", "--out")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    fc = vectorize(args.image, args.date, debug=args.debug)
    txt = json.dumps(fc, ensure_ascii=False, indent=1)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(txt)
        print(f"✅ {len(fc['features'])} 個走廊 → {args.out}")
        for ft in fc["features"]:
            c = ft["geometry"]["coordinates"][0]
            lons = [p[0] for p in c]
            lats = [p[1] for p in c]
            print(f"   #{ft['properties']['corridor_no']}: "
                  f"lon {min(lons):.2f}–{max(lons):.2f} lat {min(lats):.2f}–{max(lats):.2f} "
                  f"({ft['properties']['pixel_area']} px)")
    else:
        print(txt)


if __name__ == "__main__":
    main()
