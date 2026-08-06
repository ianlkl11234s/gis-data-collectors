"""讀圖左上表格的「項次」並依類型分流。

守門用的 ground truth 原本是「表格項次數 == 抽出形狀數」，但**項次不全是封閉多邊形**：
例如 2026-01-08 項次④ 是空飄氣球（紅圈虛線代表氣球消失），畫的是虛線軌跡＋圓圈，
本來就抽不出多邊形。把這類項次從期望數扣掉，守門才對得起真實情況。

作法：表格是中英雙語，英文行用 tesseract（`-l eng`）就讀得動（中文無 chi_tra 會亂碼，
但不影響關鍵字判斷）。每個項次的紅色圓圈編號當列錨點，OCR 文字行歸給最近的錨點。

⚠ 需 taipei-gis-analytics/venv/bin/python（scikit-image）＋ 系統 tesseract。
"""
import os
import re
import subprocess
import sys  # 只有檔尾的 __main__ CLI 用；package 化後那段不可執行，保留以與研究版對齊

import numpy as np
from PIL import Image
from scipy import ndimage

from .vectorize_pla_chart import load_rgb, red_mask
from .shape_extract import remove_markers

OCR_SCALE = 3

# 非封閉多邊形的項次類型：圖上畫的是虛線軌跡／圓圈，不是活動區
# （英文行的關鍵字；中文行 OCR 會亂碼故不採用）
BALLOON_RE = re.compile(r'ballo{0,2}n', re.I)


def table_frame(g, thr=140, min_h=60):
    """表格外框：回傳 (xl, xsplit, ytop, ybot, xr)。

    以「項次」欄左右兩條豎線定位 —— 兩者同起同終且貫穿整個表格，
    比橫線可靠（表格頂邊常與地圖經線在同一列相接，會被誤判成延伸到 x=399）。

    門檻放寬的兩個實測理由：
      · 左界常落在 x=0，JPEG 邊緣使其灰階達 ~150（2026-01-25）→ thr 用 140 而非 110
      · 只有 1 個項次時表格僅約 90px 高（2026-01-26）→ min_h 用 60 而非 100
    """
    H, W = g.shape
    dark = g < thr
    cands = []
    for x in range(0, int(W * 0.15)):
        ys = np.nonzero(dark[:int(H * 0.8), x])[0]
        if len(ys) < min_h:
            continue
        span = ys[-1] - ys[0] + 1
        if len(ys) / span > 0.95:                 # 實心貫穿
            cands.append((x, int(ys[0]), int(ys[-1])))
    if len(cands) < 2:
        return None
    xl, ytop, ybot = cands[0]
    # 項次欄分隔線：與左界同起同終
    same = [c for c in cands[1:] if abs(c[1] - ytop) <= 3 and abs(c[2] - ybot) <= 3]
    if not same:
        return None
    xsplit = same[0][0]

    # 右界：取各條列分隔線自 xl 起的連續暗像素長度中位數
    ends = []
    for y in range(ytop, ybot + 1):
        run = dark[y, xl:min(W, xl + 500)]
        if run[:20].all():
            k = int(np.argmin(run)) if not run.all() else len(run)
            if k > (xsplit - xl) * 3:
                ends.append(xl + k)
    xr = int(np.median(ends)) if len(ends) >= 2 else min(W - 1, xsplit + 300)
    return xl, xsplit, ytop, ybot, xr


def item_anchors(rgb, x_table=60, ybounds=None):
    """表格區紅色編號的 y 中心（由上而下）。

    沿用 shape_extract.extract() 的計數口徑：完整圓環（remove_markers 抓到的）
    ＋ 被切斷而留在遮罩裡的殘體 cluster，兩者都算一個項次。

    ybounds=(ytop, ybot) 時只採計表格框內的標記 —— 地圖左緣若有紅色形狀落在
    x<60，原本會被誤計成項次。
    """
    def inside(y):
        return ybounds is None or ybounds[0] <= y <= ybounds[1]

    mask, solid = remove_markers(red_mask(rgb))
    ys = [cy for cx, cy, _ in solid if cx < x_table and inside(cy)]
    lab, n = ndimage.label(ndimage.binary_dilation(mask, np.ones((5, 5))),
                           structure=np.ones((3, 3)))
    for i in range(1, n + 1):
        cm = mask & (lab == i)
        if cm.sum() < 25:
            continue
        yy, xx = np.nonzero(cm)
        if float(xx.mean()) < x_table and inside(float(yy.mean())):
            ys.append(float(yy.mean()))
    ys.sort()
    # 同一個編號被拆成兩塊時會出現兩個很近的錨點 → 併掉
    merged = []
    for y in ys:
        if merged and y - merged[-1] < 12:
            merged[-1] = (merged[-1] + y) / 2
        else:
            merged.append(y)
    return merged


def ocr_lines(path, box, tmp_png):
    """回傳 [(y_中心, 文字), ...]（原圖座標）。"""
    x0, y0, x1, y1 = box
    im = Image.open(path).convert('L').crop((x0, y0, x1, y1))
    im = im.resize((im.width * OCR_SCALE, im.height * OCR_SCALE), Image.LANCZOS)
    im.save(tmp_png)
    out = subprocess.run(['tesseract', tmp_png, '-', '-l', 'eng', '--psm', '6', 'tsv'],
                         capture_output=True, text=True, errors='replace').stdout
    lines = {}
    for row in out.splitlines()[1:]:
        f = row.split('\t')
        if len(f) < 12 or not f[11].strip():
            continue
        key = (f[2], f[3], f[4])
        lines.setdefault(key, {'y': [], 'w': []})
        lines[key]['y'].append(y0 + (int(f[7]) + int(f[9]) / 2) / OCR_SCALE)
        lines[key]['w'].append(f[11])
    return sorted((float(np.mean(v['y'])), ' '.join(v['w'])) for v in lines.values())


def read_items(path, tmp_png=None):
    """回傳 (items, n_items)。items = [{'no','y','text','kind'}]，kind='balloon'|'area'。

    OCR 失敗（表格框找不到）時 items 為 None，呼叫端應退回原本的「全部項次都算」。
    """
    rgb = load_rgb(path)
    g = np.array(Image.open(path).convert('L'))
    fr = table_frame(g)
    if fr is None:
        return None, len(item_anchors(rgb))
    xl, xsplit, ytop, ybot, xr = fr
    anchors = item_anchors(rgb, ybounds=(ytop, ybot))
    if not anchors:
        return None, 0
    tmp_png = tmp_png or os.path.join(os.path.dirname(path), '_ocr_tmp.png')
    lines = ocr_lines(path, (xsplit + 2, ytop, xr, ybot), tmp_png)

    items = [{'no': i + 1, 'y': y, 'text': '', 'kind': 'area'}
             for i, y in enumerate(anchors)]
    for y, txt in lines:
        k = int(np.argmin([abs(y - a) for a in anchors]))
        items[k]['text'] += (' ' if items[k]['text'] else '') + txt
    for it in items:
        if BALLOON_RE.search(it['text']):
            it['kind'] = 'balloon'
    return items, len(anchors)


def expected_shapes(path, tmp_png=None):
    """守門用的期望形狀數 = 項次總數 − 非多邊形項次數。

    回傳 (expected, n_items, n_excluded, items)；OCR 不可用時 expected = n_items。
    """
    items, n = read_items(path, tmp_png)
    if items is None:
        return n, n, 0, None
    excl = sum(1 for it in items if it['kind'] != 'area')
    return n - excl, n, excl, items


if __name__ == '__main__':
    import glob
    for p in sorted(sum([glob.glob(a) if '*' in a else [a] for a in sys.argv[1:]], [])):
        exp, n, excl, items = expected_shapes(p)
        print(f'=== {os.path.basename(p)} 項次={n} 排除={excl} 期望={exp}')
        for it in (items or []):
            print(f"   ({it['no']}) {it['kind']:7s} y={it['y']:.0f} {it['text'][:90]}")
