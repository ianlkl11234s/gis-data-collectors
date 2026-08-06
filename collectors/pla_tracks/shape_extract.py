"""混合法抽取共機活動區多邊形（像素座標）。

流程：
  1. 紅色遮罩 → 去編號標記（①②③④，12–26px 圓環，兼從表格區數出當日項次數當 ground truth）
  2. 輕度膨脹分群（cluster）：每群 = 一個或多個交疊的形狀
  3. 每群自適應閉合（r 遞增直到線框封閉）→ 填洞 → 侵蝕回來 = 該群實心區 core
  4. 每群跑 probabilistic Hough → 平行線配對成矩形，**每個矩形的 4 條邊
     個別驗證紅色支持度**（假矩形＝不規則多邊形的兩條長邊被誤配，短邊無紅線 → 剔除）
  5. core 減去接受的矩形 → 大面積殘差 = 不規則多邊形（輪廓 + simplify）
     沒有任何矩形時整個 core 輪廓直接輸出（小矩形 Hough 抓不到也靠這條路救回）
"""
import numpy as np
from PIL import Image, ImageDraw
from scipy import ndimage
from skimage.transform import probabilistic_hough_line
from skimage.morphology import disk
from skimage import measure
from shapely.geometry import Polygon, MultiPoint
from shapely.ops import unary_union
from .vectorize_pla_chart import load_rgb, red_mask

# 影像目錄由呼叫端以 set_image_dir() 指定
S = ''


def set_image_dir(path):
    global S
    S = path.rstrip('/') + '/'


# ── 1. 遮罩清理 ─────────────────────────────────────────────

def remove_markers(mask):
    """去掉圓形編號標記與雜點。回傳 (clean, markers[(cx,cy,px)])。

    標記 = 12–26px 見方、填洞後佔 bbox ≥ 50%（圓環+數字）。
    細線碎片 bbox 可能也小，但填洞後仍是細線 → 佔比低 → 保留。
    """
    m = mask.copy()
    lab, n = ndimage.label(m, structure=np.ones((3, 3)))
    markers = []
    objs = ndimage.find_objects(lab)
    for i, sl in enumerate(objs, 1):
        if sl is None:
            continue
        idx = (lab[sl] == i)
        c = int(idx.sum())
        if c < 12:                                # 雜點
            m[sl][idx] = False
            continue
        h, w = idx.shape
        if 12 <= h <= 26 and 12 <= w <= 26:
            fill_ratio = ndimage.binary_fill_holes(idx).sum() / (h * w)
            if fill_ratio >= 0.5:
                m[sl][idx] = False
                ys, xs = np.nonzero(idx)
                markers.append((sl[1].start + xs.mean(), sl[0].start + ys.mean(), c))
    return m, markers


def is_glyph_cluster(cmask, bmax_max=70, hole_side=(5, 14), hole_round=0.6,
                     big_hole=300):
    """這個 cluster 是「圖徽」而非活動區嗎（空飄氣球的 圓+箭頭+虛線圈）？

    判準來自實測：閉合後找內孔，**編號圓圈內部**是 ~7×9px、圓度 0.75–0.83 的小孔。
      · 氣球圖徽（2026-02-27 bmax=50 / 01-04 bmax=59 / 01-11 bmax=61）：
        只有這種小圓孔、沒有大孔
      · 真走廊：要嘛有大孔（自身框內部，如 07-27 的 90×22），要嘛整個無孔
        （線框斷掉），且 bmax 通常遠大於 70
    編號標記黏在走廊上時兩種孔並存 → 有大孔就不算圖徽。
    """
    ys, xs = np.nonzero(cmask)
    if max(np.ptp(ys), np.ptp(xs)) + 1 > bmax_max:
        return False
    cl = ndimage.binary_closing(cmask, structure=disk(3))
    hl = ndimage.binary_fill_holes(cl) & ~cl
    lab, n = ndimage.label(hl)
    small = 0
    for j in range(1, n + 1):
        yy, xx = np.nonzero(lab == j)
        h, w = int(np.ptp(yy)) + 1, int(np.ptp(xx)) + 1
        area = len(yy)
        if area >= big_hole:
            return False
        if (hole_side[0] <= w <= hole_side[1] and hole_side[0] <= h <= hole_side[1]
                and area / (np.pi * w * h / 4) >= hole_round):
            small += 1
    return small >= 1


# ── 2. 幾何小工具 ────────────────────────────────────────────

def seg_angle(s):
    (x0, y0), (x1, y1) = s
    return np.degrees(np.arctan2(y1 - y0, x1 - x0)) % 180


def seg_len(s):
    (x0, y0), (x1, y1) = s
    return float(np.hypot(x1 - x0, y1 - y0))


def angdiff(a, b):
    d = abs(a - b) % 180
    return min(d, 180 - d)


def edge_supports(poly, dist, tol=3.5):
    """矩形 4 條邊的紅色支持度（每 2px 取樣，距紅像素 ≤ tol 算有支持）。"""
    xy = list(poly.exterior.coords)[:4] + [poly.exterior.coords[0]]
    sup = []
    H, W = dist.shape
    for k in range(4):
        p0, p1 = np.array(xy[k], float), np.array(xy[k + 1], float)
        L = np.hypot(*(p1 - p0))
        nstep = max(int(L / 2), 2)
        ts = np.linspace(0, 1, nstep)
        pts = p0[None, :] + ts[:, None] * (p1 - p0)[None, :]
        xs = np.clip(pts[:, 0].round().astype(int), 0, W - 1)
        ys = np.clip(pts[:, 1].round().astype(int), 0, H - 1)
        sup.append(float((dist[ys, xs] <= tol).mean()))
    return sup


# ── 3. 每群 Hough 矩形分解（沿用已驗證的配對邏輯） ───────────────

def hough_rects(cmask, min_len=45, gap=6, pair_tol_ang=8, pair_w=(8, 90)):
    try:
        segs = probabilistic_hough_line(cmask, threshold=8, line_length=min_len,
                                        line_gap=gap, rng=np.random.default_rng(42))
    except TypeError:      # 舊版 skimage 用 seed
        segs = probabilistic_hough_line(cmask, threshold=8, line_length=min_len,
                                        line_gap=gap, seed=42)
    segs = [s for s in segs if seg_len(s) >= min_len]

    groups = []
    for s in sorted(segs, key=seg_len, reverse=True):
        a = seg_angle(s)
        for g in groups:
            if angdiff(a, g['ang']) <= pair_tol_ang:
                g['segs'].append(s)
                break
        else:
            groups.append({'ang': a, 'segs': [s]})

    rects, angs = [], []
    for g in groups:
        a = np.radians(g['ang'])
        nvec = np.array([-np.sin(a), np.cos(a)])
        items = []
        for sg in g['segs']:
            mid = np.array([(sg[0][0] + sg[1][0]) / 2, (sg[0][1] + sg[1][1]) / 2])
            items.append({'seg': sg, 'off': float(np.dot(mid, nvec)), 'len': seg_len(sg)})
        items.sort(key=lambda t: t['off'])
        lanes = []
        for it in items:
            if lanes and abs(it['off'] - lanes[-1]['off']) <= 6:
                lanes[-1]['segs'].append(it['seg'])
                lanes[-1]['off'] = (lanes[-1]['off'] + it['off']) / 2
                lanes[-1]['len'] = max(lanes[-1]['len'], it['len'])
            else:
                lanes.append({'off': it['off'], 'segs': [it['seg']], 'len': it['len']})
        used = set()
        for i in range(len(lanes) - 1):
            if i in used:
                continue
            j = i + 1
            if j in used:
                continue
            w = abs(lanes[j]['off'] - lanes[i]['off'])
            if not (pair_w[0] <= w <= pair_w[1]):
                continue
            v = np.array([np.cos(a), np.sin(a)])

            def span(sgs):
                ps = [np.dot(np.array(p, float), v) for sg in sgs for p in sg]
                return min(ps), max(ps)
            si, sj = span(lanes[i]['segs']), span(lanes[j]['segs'])
            ov = min(si[1], sj[1]) - max(si[0], sj[0])
            if ov < 0.4 * min(lanes[i]['len'], lanes[j]['len']):
                continue
            pts = [tuple(map(float, p)) for sg in lanes[i]['segs'] + lanes[j]['segs'] for p in sg]
            rects.append(MultiPoint(pts).minimum_rotated_rectangle)
            angs.append(g['ang'])
            used.add(i)
            used.add(j)

    # 同一條邊被 Hough 切成多段 → 同角度且高度重疊的矩形合併去重
    merged = True
    while merged:
        merged = False
        for i in range(len(rects)):
            for j in range(i + 1, len(rects)):
                if angdiff(angs[i], angs[j]) > 10:
                    continue
                inter = rects[i].intersection(rects[j]).area
                small = min(rects[i].area, rects[j].area) or 1
                if inter / small > 0.35:
                    rects[i] = MultiPoint(
                        list(rects[i].exterior.coords) + list(rects[j].exterior.coords)
                    ).minimum_rotated_rectangle
                    rects.pop(j)
                    angs.pop(j)
                    merged = True
                    break
            if merged:
                break
    return rects


# ── 4. 每群自適應閉合 → core 實心區 ──────────────────────────

def adaptive_core(cmask):
    """對單一 cluster 的紅像素：膨脹 r 遞增直到線框封閉（填得出洞），
    再侵蝕回來。回傳 (core_bool, r_used)；封不起來回傳 (None, None)。"""
    outline_px = int(cmask.sum())
    ys, xs = np.nonzero(cmask)
    y0, y1, x0, x1 = ys.min(), ys.max(), xs.min(), xs.max()
    for r in (3, 5, 7, 9, 11, 13):
        pad = r + 2
        sub = np.zeros((y1 - y0 + 1 + 2 * pad, x1 - x0 + 1 + 2 * pad), bool)
        sub[pad + ys - y0, pad + xs - x0] = True
        d = ndimage.binary_dilation(sub, structure=disk(r))
        f = ndimage.binary_fill_holes(d)
        interior = int(f.sum() - d.sum())
        if interior >= max(200, int(0.6 * outline_px)):
            core_sub = ndimage.binary_erosion(f, structure=disk(r))
            core = np.zeros_like(cmask)
            sl = core_sub[pad:-pad, pad:-pad]
            core[y0:y1 + 1, x0:x1 + 1] = sl
            return core, r
    return None, None


def core_polygon(core, simplify_tol=2.5):
    cs = measure.find_contours(np.pad(core.astype(float), 1), 0.5)
    if not cs:
        return None
    c = max(cs, key=len)
    poly = Polygon([(float(x) - 1, float(y) - 1) for y, x in c]).buffer(0)
    if poly.geom_type == 'MultiPolygon':
        poly = max(poly.geoms, key=lambda p: p.area)
    return poly.simplify(simplify_tol)


# ── 5. 主流程 ────────────────────────────────────────────────

def snap_or_poly(p, snap_iou=0.96):
    """輪廓近似矩形（面積/最小旋轉矩形 ≥ snap_iou）→ 吸附成乾淨矩形。"""
    mrr = p.minimum_rotated_rectangle
    if mrr.area > 0 and p.area / mrr.area >= snap_iou:
        return mrr, 'rect'
    return p, 'poly'


def extract(date, cluster_r=5, edge_min=0.45, mean_min=0.72,
            rect_area_min=500, resid_area_min=2500, core_area_min=300,
            hough_min_len=45, n_balloon=0, verbose=True):
    """n_balloon：表格判定的空飄氣球項次數（table_items.expected_shapes 提供）。

    >0 時最多丟棄同樣數量的「圖徽」cluster —— 氣球在圖上畫的是虛線圈＋箭頭，
    不是活動區，抽成多邊形只會讓形狀數多出來。以表格數字設上限，
    確保沒有氣球的日子完全不受影響。
    """
    rgb = load_rgb(f'{S}{date}.jpg')
    mask, solid_markers = remove_markers(red_mask(rgb))
    n_table = sum(1 for cx, _, _ in solid_markers if cx < 60)   # 表格區完整圓環

    dist = ndimage.distance_transform_edt(~mask)             # 給邊支持度用

    dil = ndimage.binary_dilation(mask, structure=disk(cluster_r))
    lab, n = ndimage.label(dil, structure=np.ones((3, 3)))

    # 氣球圖徽：先全數找出，再依 bmax 由小到大丟棄 n_balloon 個
    drop = set()
    if n_balloon > 0:
        cand = []
        for i in range(1, n + 1):
            cm = mask & (lab == i)
            if cm.sum() < 25:
                continue
            ys, xs = np.nonzero(cm)
            if float(xs.mean()) < 60 or not is_glyph_cluster(cm):
                continue
            cand.append((max(np.ptp(ys), np.ptp(xs)), i))
        drop = {i for _, i in sorted(cand)[:n_balloon]}
        if verbose and drop:
            print(f'   [balloon] 丟棄 {len(drop)} 個氣球圖徽 cluster（表格氣球項次={n_balloon}）')

    shapes = []                                              # (poly, kind)
    used_mask = np.zeros_like(mask)                          # 有產出形狀的 cluster 紅像素
    for i in range(1, n + 1):
        if i in drop:
            continue
        n_before = len(shapes)
        cmask = mask & (lab == i)
        px = int(cmask.sum())
        if px < 25:
            continue
        ys, xs = np.nonzero(cmask)
        cx = float(xs.mean())
        bmax = max(np.ptp(ys) + 1, np.ptp(xs) + 1)
        if cx < 60:                                          # 表格區＝被打斷的編號環
            n_table += 1
            continue
        if bmax <= 26:                                       # 圖上編號環（斷裂殘體）
            continue
        if bmax <= 70:                                       # 小形狀：Hough 抓不到，直接框
            if px >= 80:
                pts = MultiPoint(list(zip(xs.astype(float), ys.astype(float))))
                shapes.append((pts.minimum_rotated_rectangle, 'rect', 0))
                used_mask |= cmask
            continue

        core, r_used = adaptive_core(cmask)
        cpoly = core_polygon(core) if core is not None else None

        # Hough 矩形 + 四邊支持度驗證
        kept = []
        for rp in hough_rects(cmask, min_len=hough_min_len):
            if rp.area < rect_area_min:
                continue
            sup = edge_supports(rp, dist)
            if min(sup) >= edge_min and float(np.mean(sup)) >= mean_min:
                kept.append(rp)
            elif verbose:
                print(f'   [drop] 假矩形 支持度={[f"{s:.2f}" for s in sup]} '
                      f'面積={rp.area:.0f}')

        if cpoly is None or cpoly.area < core_area_min:
            if kept:                                         # 封不起來但矩形可信 → 仍輸出
                shapes += [(rp, 'rect', -1) for rp in kept]
                used_mask |= cmask
            elif verbose:
                print(f'   [warn] cluster {i}: 線框封不起來（{px}px）且無可信矩形，捨棄')
            continue

        if len(kept) >= 2:
            # 交叉形狀 → 矩形分解 + 大面積殘差
            shapes += [(rp, 'rect', r_used) for rp in kept]
            resid = cpoly.difference(unary_union([rp.buffer(4) for rp in kept]))
            pieces = list(resid.geoms) if resid.geom_type == 'MultiPolygon' else [resid]
            shapes += [(p.simplify(2.5), 'poly', r_used)
                       for p in pieces if p.area >= resid_area_min]
        elif len(kept) == 1:
            resid = cpoly.difference(kept[0].buffer(4))
            pieces = [p for p in
                      (list(resid.geoms) if resid.geom_type == 'MultiPolygon' else [resid])
                      if p.area >= resid_area_min]
            if pieces:                                       # 矩形+其他 混合 cluster
                shapes.append((kept[0], 'rect', r_used))
                shapes += [(p.simplify(2.5), 'poly', r_used) for p in pieces]
            else:                                            # 單一形狀 → 忠實輪廓（可吸附）
                poly, kind = snap_or_poly(cpoly)
                shapes.append((poly, kind, r_used))
        else:
            poly, kind = snap_or_poly(cpoly)
            shapes.append((poly, kind, r_used))
        if len(shapes) > n_before:
            used_mask |= cmask

    # ── 量化驗證 ──
    # precision：抽出形狀邊界上，距紅像素 ≤4px 的取樣點比例（邊界是否貼著紅線）
    # recall：  已處理 cluster 的紅像素中，距任一抽出邊界 ≤4px 的比例（紅線是否都被交代）
    prec = rec = float('nan')
    if shapes:
        sup_all = []
        bnd = np.zeros(mask.shape, bool)
        for p, _, _ in shapes:
            xy = np.array(p.exterior.coords)
            for k in range(len(xy) - 1):
                L = max(int(np.hypot(*(xy[k + 1] - xy[k])) / 1.5), 2)
                ts = np.linspace(0, 1, L)
                pts = xy[k][None] + ts[:, None] * (xy[k + 1] - xy[k])[None]
                xs_ = np.clip(pts[:, 0].round().astype(int), 0, mask.shape[1] - 1)
                ys_ = np.clip(pts[:, 1].round().astype(int), 0, mask.shape[0] - 1)
                sup_all.append(dist[ys_, xs_] <= 4)
                bnd[ys_, xs_] = True
        prec = float(np.concatenate(sup_all).mean())
        if used_mask.any():
            dist_b = ndimage.distance_transform_edt(~bnd)
            rec = float((dist_b[used_mask] <= 4).mean())

    if verbose:
        print(f'{date}: 表格項次={n_table} → 抽出 {len(shapes)} 形狀 '
              f'(邊界precision={prec:.2f} 紅線recall={rec:.2f})')
        for k, (p, kind, r_used) in enumerate(shapes, 1):
            print(f'   #{k} {kind:4s} 面積={p.area:7.0f}px 頂點={len(p.exterior.coords)-1} '
                  f'(閉合r={r_used})')
    return shapes, n_table, prec, rec


# ── 6. 已知目標數引導的重試 ───────────────────────────────────
#
# 表格已經告訴我們當日該有幾個形狀。基準參數沒對上時，不必盲目調參：
# 沿一條由保守到積極的階梯重跑，用「命中期望數」當**選擇器**挑分割方案。
# 只有品質同時過關才採用，否則維持基準結果並標 needs_review。
#
# 兩支階梯：抽太少 → 切得更開；抽太多 → 併得更攏。

SPLIT_LADDER = [
    {'cluster_r': 4},
    {'cluster_r': 3},
    {'hough_min_len': 35},
    {'cluster_r': 3, 'hough_min_len': 35},
    {'cluster_r': 3, 'hough_min_len': 30, 'edge_min': 0.38, 'mean_min': 0.66},
    {'cluster_r': 2, 'hough_min_len': 30, 'edge_min': 0.38, 'mean_min': 0.66,
     'resid_area_min': 1200},
]
MERGE_LADDER = [
    {'cluster_r': 7},
    {'cluster_r': 9},
    {'hough_min_len': 60},
    {'cluster_r': 9, 'hough_min_len': 60},
    {'cluster_r': 11, 'hough_min_len': 70, 'resid_area_min': 5000},
]

# 品質下限：取自基準已通過日的分佈（precision p5=0.98、recall p5=0.58），
# 放寬一點當門檻 —— 命中數字但畫得不對的方案要擋掉
# （實測 2026-01-07 命中卻只有 precision 0.59、2026-02-25 recall 0.31）
PREC_MIN, REC_MIN = 0.90, 0.60


def extract_guided(date, n_expected, n_balloon=0, prec_min=PREC_MIN,
                   rec_min=REC_MIN, verbose=True, **base_kw):
    """回傳 (shapes, prec, rec, params)。params=None 代表用基準參數。"""
    shapes, n_marker, prec, rec = extract(date, n_balloon=n_balloon,
                                          verbose=verbose, **base_kw)
    if n_expected is None or len(shapes) == n_expected:
        return shapes, n_marker, prec, rec, None

    ladder = SPLIT_LADDER if len(shapes) < n_expected else MERGE_LADDER
    best = None
    for cfg in ladder:
        try:
            sh, _, p, r = extract(date, n_balloon=n_balloon, verbose=False,
                                  **{**base_kw, **cfg})
        except Exception:
            continue
        if len(sh) != n_expected or not (p >= prec_min and r >= rec_min):
            continue
        if best is None or (p, r) > (best[2], best[3]):
            best = (sh, cfg, p, r)
    if best is None:
        return shapes, n_marker, prec, rec, None
    sh, cfg, p, r = best
    if verbose:
        print(f'   [guided] {len(shapes)}→{len(sh)} 形狀命中期望 {n_expected}，'
              f'採用 {cfg}（precision={p:.2f} recall={r:.2f}）')
    return sh, n_marker, p, r, cfg


def draw(date, shapes):
    im = Image.open(f'{S}{date}.jpg').convert('RGB')
    dr = ImageDraw.Draw(im)
    for k, (p, kind, _) in enumerate(shapes, 1):
        color = (0, 90, 255) if kind == 'rect' else (170, 0, 200)
        dr.line([(x, y) for x, y in p.exterior.coords], fill=color, width=3)
        cx, cy = p.centroid.coords[0]
        dr.text((cx - 6, cy - 6), f'#{k}', fill=color)
    out = f'{S}hybrid_{date}.png'
    im.save(out)
    return out


