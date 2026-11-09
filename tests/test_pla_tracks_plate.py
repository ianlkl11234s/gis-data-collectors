"""共機航跡圖版型分流：720×1040 原流程、1125×1625 縮圖後沿用、未知尺寸拒絕。

背景：2026-10-01 起國防部改出 1125×1625，舊程式只認 720×1040，
整整一個多月 plate_ok=False、0 形狀（ledger error 為 NULL 所以不會自動重跑）。
"""
import shutil
from pathlib import Path

import pytest
from PIL import Image

pytest.importorskip("scipy")
pytest.importorskip("skimage")

from collectors.pla_tracks import georef as G  # noqa: E402
from collectors.pla_tracks import shape_extract as se  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "pla_tracks" / "2026-11-05_1125x1625.jpg"
DATE = "2026-11-05"


def _blank(path: Path, size):
    Image.new("RGB", size, "white").save(path, quality=95)


def test_normalize_scales_known_plate_in_place(tmp_path):
    p = tmp_path / f"{DATE}.jpg"
    _blank(p, (1125, 1625))
    assert G.normalize_plate(str(p)) == (1125, 1625)
    assert Image.open(p).size == G.PLATE_SIZE
    # 第二次呼叫（已縮過）不再動，回傳工作尺寸
    assert G.normalize_plate(str(p)) == G.PLATE_SIZE


@pytest.mark.parametrize("size", [(720, 1040), (794, 1115), (1440, 2080)])
def test_normalize_leaves_other_sizes_alone(tmp_path, size):
    p = tmp_path / f"{DATE}.jpg"
    _blank(p, size)
    assert G.normalize_plate(str(p)) == size
    assert Image.open(p).size == size


def test_unknown_plate_rejected_without_shapes(tmp_path):
    """長寬比相同但沒登記的尺寸（2 倍 720）也不放行 —— 要先驗證版面再登記。"""
    _blank(tmp_path / f"{DATE}.jpg", (1440, 2080))
    out = G.vectorize_day(str(tmp_path), DATE)
    assert out["plate_ok"] is False
    assert out["plate_size"] == (1440, 2080)
    assert out["shapes"] == []


def test_every_plate_georef_covers_same_extent():
    """每個版型的固化配準在工作尺寸下都應把 117–123°E × 21–29°N 放在差不多的位置。"""
    for size, (ax, bx, ay, by) in G.PLATES.items():
        x117, x123 = (117 - bx) / ax, (123 - bx) / ax
        y29, y21 = (29 - by) / ay, (21 - by) / ay
        assert abs(x117 - 124) < 3 and abs(x123 - 613) < 3, size
        assert abs(y29 - 181) < 3 and abs(y21 - 901) < 3, size


def test_real_1125_chart_georef_and_shapes(tmp_path):
    """真圖（2026-11-05，JPEG q60 重存以縮小檔案）：縮圖後配準與固化值一致、抽出 2 個活動區。"""
    shutil.copy(FIXTURE, tmp_path / f"{DATE}.jpg")
    path = str(tmp_path / f"{DATE}.jpg")
    assert G.normalize_plate(path) == (1125, 1625)

    dev = G.georef_deviation(path, G.PLATES[(1125, 1625)])
    assert dev is not None and dev < 0.004
    # 用舊 720 配準會偏 0.017° —— 這就是為什麼要分版型固化
    assert G.georef_deviation(path, G.PLATES[(720, 1040)]) > 0.01

    se.set_image_dir(str(tmp_path))
    shapes, n_table, prec, rec = se.extract(DATE, verbose=False)
    assert n_table == 2
    assert len(shapes) == 2
    assert prec >= 0.98 and rec >= 0.9

    # 形狀落點：① 馬祖東南狹長框、② 西南角三角形（經緯度目視核對原圖）
    ax, bx, ay, by = G.PLATES[(1125, 1625)]
    cents = sorted((ax * p.centroid.x + bx, ay * p.centroid.y + by) for p, _, _ in shapes)
    (lon_sw, lat_sw), (lon_n, lat_n) = cents
    assert 117.8 < lon_sw < 119.5 and 21.0 < lat_sw < 22.6
    assert 119.6 < lon_n < 121.0 and 25.0 < lat_n < 26.3


def test_pending_query_gives_up_on_old_404(monkeypatch):
    """原圖 404 的日子（如 2026-10-14）過了 GIVE_UP_404_DAYS 天就不再進待處理清單。"""
    import collectors.pla_tracks_vectorize as m

    seen = {}

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql, params):
            seen["sql"], seen["params"] = sql, params

        def fetchall(self):
            return []

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            pass

    monkeypatch.setattr(m, "connect_supabase", lambda **kw: _Conn())
    col = m.PlaTracksVectorizeCollector.__new__(m.PlaTracksVectorizeCollector)
    assert col._find_pending() == []
    p = seen["params"]
    assert p["mark_404"] == "404 Client Error"
    assert p["give_up"] == m.PlaTracksVectorizeCollector.GIVE_UP_404_DAYS
    assert "strpos(r.error, %(mark_404)s)" in seen["sql"]
    # 下載失敗訊息確實帶這段字樣（requests HTTPError 的格式）
    import requests
    resp = requests.Response()
    resp.status_code, resp.url, resp.reason = 404, "https://example.invalid/x", "Not Found"
    with pytest.raises(requests.HTTPError) as e:
        resp.raise_for_status()
    assert m.ERROR_404_MARK in str(e.value)
