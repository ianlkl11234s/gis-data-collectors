"""台鐵時刻表 stations_raw 的行為測試（2026-08-23 加）。

背景：`convert_tra_train` 的 `stations` 欄位服務 mini-taipei 3D 前端，
必須是「畫得出來」的站——經過 `normalize_station_id`（新站碼→軌道舊站碼）
與 `track_station_set` 過濾（只留這條 O-D 軌道有 progress 的站）。

這對 3D 是對的，對統計是災難：

  - 台中線高架化後 TDX 換了新站碼，`STATION_ID_MAPPING` 把它們映射回舊站碼。
    潭子(3250)→栗林(3240)、新烏日(3340)/成功(3350)→烏日(3330)。
    再經去重（同一 station_id 只留一筆），同一班車停兩站會**互相吃掉**——
    不是加總，是聯集。栗林因此被算成「栗林 ∪ 潭子」，潭子本身歸零。
  - 嘉北(4070) 只出現在 271 條軌道中的 3 條，絕大多數經過它的車會把這站濾掉
    （2026-08-19 TDX 原始 79 筆停靠，轉換後只剩 2 筆）。

修法是**加一份沒被動過的原始停靠序列**，不動 `stations`。
本測試鎖住兩件事：加了 raw 之後 `stations` 逐位元不變、raw 真的完整。

fixture 是合成的（不打 S3），但站碼與映射關係取自真實情境。
"""
import pytest

from tasks.mini_taipei_publish import build_track_index, convert_tra_train

# 一條「臺北 → 臺中」的假軌道。刻意讓它：
#   - 含舊站碼 3240（栗林）與 3330（烏日）→ 新站碼會被映射進來
#   - **不含** 4070（嘉北）→ 模擬「軌道沒涵蓋這站」的過濾
OD_PROGRESS = {
    "OD-TP-TC": {"1000": 0.0, "3240": 0.5, "3330": 0.8, "3300": 1.0},
}


def _stop(station_id: str, hhmm: str) -> dict:
    return {
        "StationID": station_id,
        "StationName": {"Zh_tw": station_id},
        "ArrivalTime": hhmm,
        "DepartureTime": hhmm,
    }


# 同一班車依序停：臺北 → 潭子(3250) → 栗林(3240) → 嘉北(4070) → 新烏日(3340) → 烏日(3330) → 臺中
# 其中 3250→3240、3340→3330 會被 STATION_ID_MAPPING 撞在一起。
TRAIN = {
    "TrainInfo": {"TrainNo": "1234", "TrainTypeName": {"Zh_tw": "區間"}},
    "StopTimes": [
        _stop("1000", "08:00"),
        _stop("3250", "08:30"),
        _stop("3240", "08:35"),
        _stop("4070", "08:40"),
        _stop("3340", "08:50"),
        _stop("3330", "08:55"),
        _stop("3300", "09:00"),
    ],
}


@pytest.fixture
def track_index():
    return build_track_index(OD_PROGRESS)


def test_stations_unchanged_when_raw_enabled(track_index):
    """開了 include_raw_stops 之後，給 3D 前端的 stations 必須逐位元不變。"""
    off = convert_tra_train(TRAIN, track_index, OD_PROGRESS)
    on = convert_tra_train(TRAIN, track_index, OD_PROGRESS, include_raw_stops=True)

    assert off is not None and on is not None
    assert on["stations"] == off["stations"]
    assert {k: v for k, v in on.items() if k != "stations_raw"} == off


def test_stations_still_loses_stops(track_index):
    """反向鎖住既有行為：stations 確實會少站（這是 3D 需要的，不是 bug）。"""
    off = convert_tra_train(TRAIN, track_index, OD_PROGRESS)
    ids = [s["station_id"] for s in off["stations"]]

    assert "4070" not in ids, "嘉北不在軌道上，應該被濾掉"
    assert ids.count("3240") == 1, "潭子與栗林都映射成 3240，去重後只剩一筆"
    assert ids.count("3330") == 1, "新烏日與成功都映射成 3330，去重後只剩一筆"
    assert len(ids) == 4, "7 站進去只剩 4 站"


def test_raw_stops_keep_every_tdx_stop(track_index):
    """stations_raw 必須是 TDX 原樣：不映射、不過濾、不去重。"""
    on = convert_tra_train(TRAIN, track_index, OD_PROGRESS, include_raw_stops=True)
    raw_ids = [s["station_id"] for s in on["stations_raw"]]

    assert raw_ids == ["1000", "3250", "3240", "4070", "3340", "3330", "3300"]
    assert "3250" in raw_ids, "潭子要保留新站碼，不能被改寫成栗林"
    assert "4070" in raw_ids, "嘉北不該因為軌道沒涵蓋就消失"


def test_raw_stops_share_the_same_time_base(track_index):
    """raw 與 stations 的秒數基準必須一致（都相對於首站發車）。"""
    on = convert_tra_train(TRAIN, track_index, OD_PROGRESS, include_raw_stops=True)
    by_id = {s["station_id"]: s for s in on["stations_raw"]}

    assert by_id["1000"]["arrival"] == 0
    assert by_id["3300"]["arrival"] == 3600  # 08:00 → 09:00

    # 沒被映射撞號的站，兩邊秒數必須一致
    for st in on["stations"]:
        if st["station_id"] in ("3240", "3330"):
            continue  # 撞號站另外測，見下
        assert by_id[st["station_id"]]["arrival"] == st["arrival"]


def test_merged_ids_carry_the_wrong_stations_time(track_index):
    """鎖住污染的具體樣貌：stations 裡的「栗林」其實帶著潭子的時刻。

    這不是斷言正確行為，是把 bug 的形狀寫下來——因為它從輸出完全看不出來：
    3240 這筆長得像一筆正常的栗林停靠，時刻卻是潭子的 08:30。
    哪天 `stations` 的行為被修好了，這個測試會紅，那是好事，改掉它即可。
    """
    on = convert_tra_train(TRAIN, track_index, OD_PROGRESS, include_raw_stops=True)
    stations = {s["station_id"]: s for s in on["stations"]}
    raw = {s["station_id"]: s for s in on["stations_raw"]}

    # 潭子 08:30 = 1800 秒、栗林 08:35 = 2100 秒
    assert raw["3250"]["arrival"] == 1800
    assert raw["3240"]["arrival"] == 2100
    # 去重留第一筆 → 掛著栗林站碼、拿著潭子的時刻
    assert stations["3240"]["arrival"] == 1800

    # 新烏日 08:50 / 烏日 08:55 同理
    assert raw["3340"]["arrival"] == 3000
    assert raw["3330"]["arrival"] == 3300
    assert stations["3330"]["arrival"] == 3000


def test_raw_absent_by_default(track_index):
    """預設不輸出——S3 給 3D 前端的那條路徑不該多這個欄位。"""
    assert "stations_raw" not in convert_tra_train(TRAIN, track_index, OD_PROGRESS)
