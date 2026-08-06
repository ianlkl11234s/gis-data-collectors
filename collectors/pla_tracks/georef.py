"""固化配準常數 + 單日向量化 pipeline（每日自動化入口）。

## 為什麼要固化配準，而不是每天現算

`solve_georef()` 從圖上的經緯網格線反推「像素 → 經緯度」的線性式。研究期的
`build_geojson.py` 是**一次跑一整批圖、依尺寸分組取中位數**當共用配準 —— 那個
中位數不只是省事，是**正確性的保護**。

2026-05~07 共 75 張 720×1040 實測（2026-08-06）：

| 當張 solve_georef 結果 | 張數 | 佔比 |
|---|---|---|
| 配準失敗（回 None） | 8 | 11% |
| 成功且與眾數一致（圖角偏差 ≤ 0.004°） | 55 | 73% |
| **成功但錯誤**（圖角偏差 0.77°–3.01°） | 12 | **16%** |

那 16% 是網格線 off-by-N 造成的「假成功」—— 函式回傳了一個看似合理的解，
但整幅圖平移最多 3 度（台海約 300 km）。**單張跑而直接採用當張解，等於每 6 天
就有 1 天畫在錯的位置**，且 `needs_review` 抓不到（它只比對形狀數量，不驗位置）。

故本模組一律用固化值畫圖，當張解只拿來做一致性檢查與版型改版偵測。

## 固化值來源

眾數（75 張中 40 張逐位元相同；第 2、3 名共 15 張差 ≤ 0.004° 屬像素級抖動）。
緯度係數在所有成功樣本中完全一致，經度係數才是 off-by-N 的受害者。
"""
from __future__ import annotations

import os

from PIL import Image

from .batch_vectorize import solve_georef
from .vectorize_pla_chart import DEFAULT_LONS, DEFAULT_LATS
from . import shape_extract
from .shape_extract import extract_guided
from .table_items import expected_shapes

# 已知可處理的版型（寬, 高）。其餘版型一律不採用 —— 已知 794×1115（2026 年 3 張）
# 網格線太少，配準與表格框偵測都必失敗，硬跑只會產生錯誤形狀。
PLATE_SIZE = (720, 1040)

# 固化配準：lon = ax·x + bx，lat = ay·y + by
GEOREF = (0.0122698915, 115.4776685516, -0.0111111111, 31.0111111111)

# 當張解 vs 固化值的圖角最大偏差容忍（度）。
# 實測正常抖動 ≤ 0.004°、錯誤解 ≥ 0.77° —— 0.05° 落在兩者之間，兩側都有 15 倍餘裕。
GEOREF_TOL_DEG = 0.05


def georef_deviation(path: str) -> float | None:
    """當張 solve_georef 解與固化值的圖角最大偏差（度）。

    None = 當張配準失敗（11% 屬常態，不是錯誤）。
    回傳值 > GEOREF_TOL_DEG 代表當張解不可信 —— 但**只記錄不阻擋**，因為畫圖
    本來就用固化值。持續大量偏離才是「底圖改版」的訊號（見 collector 的告警）。
    """
    g = solve_georef(path, DEFAULT_LONS, DEFAULT_LATS)
    if not g:
        return None
    ax, bx, ay, by = g[:4]
    gax, gbx, gay, gby = GEOREF
    w, h = PLATE_SIZE
    dlon = max(abs((ax - gax) * x + (bx - gbx)) for x in (0, w))
    dlat = max(abs((ay - gay) * y + (by - gby)) for y in (0, h))
    return max(dlon, dlat)


def vectorize_day(img_dir: str, report_date: str) -> dict:
    """單日航跡圖 → 形狀清單（經緯度）+ 守門結果。

    Args:
        img_dir: 存放 `{report_date}.jpg` 的目錄
        report_date: YYYY-MM-DD

    Returns:
        {
          'report_date': str,
          'plate_ok': bool,            # 版型是否可處理；False 時 shapes 必為空
          'plate_size': (w, h),
          'georef_dev': float | None,  # 當張解與固化值偏差；None = 當張配準失敗
          'expected': int | None,      # 表格項次數（已扣掉空飄氣球）
          'balloon_items': int,
          'guided': bool,              # 是否走過「已知目標數」引導重試
          'ok': bool,                  # 守門：抽出形狀數 == 期望數
          'edge_precision': float,
          'red_recall': float,
          'shapes': [
            {'shape_no', 'ring': [(lon, lat), ...], 'shape_kind', 'vertices'}, ...
          ],
        }

    `ok=False` → 呼叫端應把該日形狀標 needs_review（不靜默採用，兩支 RPC 預設排除）。
    `plate_ok=False` → 完全不產形狀，呼叫端應告警（版型改了，程式要跟）。
    """
    path = os.path.join(img_dir, f"{report_date}.jpg")
    size = Image.open(path).size

    out = {
        'report_date': report_date,
        'plate_ok': size == PLATE_SIZE,
        'plate_size': size,
        'georef_dev': None,
        'expected': None,
        'balloon_items': 0,
        'guided': False,
        'ok': False,
        'edge_precision': 0.0,
        'red_recall': 0.0,
        'shapes': [],
    }
    if not out['plate_ok']:
        return out

    out['georef_dev'] = georef_deviation(path)

    # shape_extract 以模組級全域記影像目錄，且靠 date 組檔名找圖
    shape_extract.set_image_dir(img_dir)

    # 守門的期望數 = 表格項次 − 非多邊形項次（空飄氣球畫的是虛線軌跡，抽不出多邊形）
    n_exp, n_items, n_excl, _ = expected_shapes(path, tmp_png=os.path.join(img_dir, '_ocr_tmp.png'))
    shapes, n_marker, prec, rec, guided = extract_guided(
        report_date, n_exp if n_items else None, n_balloon=n_excl)
    n_table = n_exp if n_items else n_marker

    ax, bx, ay, by = GEOREF
    out.update({
        'expected': n_table,
        'balloon_items': n_excl,
        'guided': guided is not None,
        # 期望 0 抽出 0（整天只有氣球）也算通過 —— 這種日子沒有形狀但確實跑過了
        'ok': n_table is not None and len(shapes) == n_table,
        'edge_precision': prec,
        'red_recall': rec,
        'shapes': [
            {
                'shape_no': i,
                'ring': [(ax * x + bx, ay * y + by) for x, y in poly.exterior.coords],
                'shape_kind': kind,
                'vertices': len(poly.exterior.coords) - 1,
            }
            for i, (poly, kind, _close_r) in enumerate(shapes, 1)
        ],
    })
    return out
