"""pla_activity_daily 解析單元測試 — 2026-08 現行句型 + 舊句型 fallback。

背景：2026-08-01 稽核發現三個解析 bug（crossed 51/51 全 NULL、ADIZ 分區
只標最後一區、report_date 誤註為截止日），本測試鎖住修正後的行為。
"""
from collectors.pla_activity_daily import parse_pla_detail, parse_pla_page

_HEADER = "中共解放軍臺海周邊海、空域動態 一、日期： "

CURRENT_WITH_MEDIAN = (
    _HEADER
    + "中華民國115年7月30日（星期四）0600時至115年7月31日（星期五）0600時止。 "
    "二、活動動態： 迄0600時止，偵獲共機27架次（逾越中線進入北部、中部、西南及東部空域22架次）、"
    "共艦9艘及公務船2艘，持續在臺海周邊活動。"
)

CURRENT_NO_MEDIAN = (
    _HEADER
    + "中華民國115年7月31日（星期五）0600時至115年8月1日（星期六）0600時止。 "
    "二、活動動態： 迄0600時止，偵獲共機5架次（進入西南及東部空域5架次）、共艦10艘及公務船2艘，"
    "持續在臺海周邊活動。"
)

ZERO_AIRCRAFT = (
    _HEADER
    + "中華民國115年6月27日（星期六）0600時至115年6月28日（星期日）0600時止。 "
    "二、活動動態： 迄0600時止，偵獲共艦6艘，持續在臺海周邊活動。 "
    "三、上述期間未偵獲共機，故無提供航跡圖。"
)

LEGACY_FORMAT = (
    _HEADER
    + "中華民國113年5月10日（星期五）0600時至113年5月11日（星期六）0600時止。 "
    "二、活動動態： 迄0600時止，偵獲共機 12 架次、共艦 5 艘，持續在臺海周邊活動，"
    "其中共機 8 架次逾越海峽中線及進入我東部空域。"
)


def test_current_with_median():
    p = parse_pla_detail(CURRENT_WITH_MEDIAN)
    assert p is not None
    assert p["report_date"] == "2026-07-30"          # 起算日
    assert p["period_start"] == "2026-07-30"
    assert p["period_end"] == "2026-07-31"
    assert p["aircraft_sorties"] == 27
    assert p["crossed_median_line_cnt"] == 22        # 括號句尾數字
    assert p["plan_vessels"] == 9
    assert p["official_ships"] == 2
    # 頓號列舉四區全標（舊版只會標到緊貼「空域」的東部）
    assert p["adiz_north"] and p["adiz_central"]
    assert p["adiz_southwestern"] and p["adiz_eastern"]


def test_current_no_median_is_zero_crossed():
    p = parse_pla_detail(CURRENT_NO_MEDIAN)
    assert p is not None
    assert p["aircraft_sorties"] == 5
    assert p["crossed_median_line_cnt"] == 0         # 未提「逾越中線」= 0，非 NULL
    assert not p["adiz_north"] and not p["adiz_central"]
    assert p["adiz_southwestern"] and p["adiz_eastern"]
    assert p["period_end"] == "2026-08-01"


NO_CLAUSE = (  # 有共機但無括號子句 = 當日未逾越中線／未進入我空域（2026-07-25 實例）
    _HEADER
    + "中華民國115年7月25日（星期五）0600時至7月26日（星期六）0600時止。 "
    "二、活動動態： 迄0600時止，偵獲共機4架次、共艦7艘及公務船3艘，持續在臺海周邊活動。"
)


def test_no_clause_means_zero_crossed_not_null():
    p = parse_pla_detail(NO_CLAUSE)
    assert p is not None
    assert p["aircraft_sorties"] == 4
    assert p["crossed_median_line_cnt"] == 0     # 0 ≠ NULL（NULL 保留給「未知」）
    assert not any(p[k] for k in ("adiz_north", "adiz_central",
                                  "adiz_southwestern", "adiz_eastern"))
    assert p["period_end"] == "2026-07-26"       # 第二個日期不帶年份


def test_year_rollover_period_end():
    text = (
        _HEADER
        + "中華民國114年12月31日（星期三）0600時至1月1日（星期四）0600時止。 "
        "二、活動動態： 迄0600時止，偵獲共機2架次、共艦3艘，持續在臺海周邊活動。"
    )
    p = parse_pla_detail(text)
    assert p is not None
    assert p["report_date"] == "2025-12-31"
    assert p["period_end"] == "2026-01-01"       # 月份回捲 → 年 +1


def test_upstream_typo_missing_ji_char():
    """2025-01-09 國防部原文漏「機」字：「偵獲共4架次」。"""
    text = (
        _HEADER
        + "中華民國114年1月9日（星期四）0600時至1月10日（星期五）0600時止。 "
        "二、活動動態： 迄0600時止，偵獲共4架次（逾越中線進入北部空域2架次）、共艦7艘，"
        "持續在臺海周邊活動。"
    )
    p = parse_pla_detail(text)
    assert p is not None
    assert p["aircraft_sorties"] == 4
    assert p["crossed_median_line_cnt"] == 2
    assert p["adiz_north"]


def test_vessels_only_day_is_zero_sorties():
    """2026-02 起數見：整段只列共艦、完全未提共機 = 當日 0 架次（非未知）。"""
    text = (
        _HEADER
        + "中華民國115年2月7日（星期六）0600時至2月8日（星期日）0600時止。 "
        "二、活動動態： 迄0600時止，偵獲共艦7艘，持續在臺海周邊活動。"
        "國軍運用任務機、艦及岸置飛彈系統嚴密監控與應處。 "
        "三、中共空飄氣球活動： 中共空飄氣球計偵獲1顆。"
    )
    p = parse_pla_detail(text)
    assert p is not None
    assert p["aircraft_sorties"] == 0
    assert p["crossed_median_line_cnt"] == 0
    assert p["plan_vessels"] == 7


def test_zero_aircraft_day():
    p = parse_pla_detail(ZERO_AIRCRAFT)
    assert p is not None
    assert p["aircraft_sorties"] == 0
    assert p["crossed_median_line_cnt"] == 0
    assert p["plan_vessels"] == 6
    assert not any(p[k] for k in ("adiz_north", "adiz_central", "adiz_southwestern", "adiz_eastern"))


SINGLE_AIRCRAFT = (  # 單架寫「1架」不寫「1架次」（nid 87169 實例）
    _HEADER
    + "中華民國115年7月27日（星期一）0600時至115年7月28日（星期二）0600時止。 "
    "二、活動動態： 迄0600時止，偵獲共機1架（進入西南空域1架）及共艦7艘，持續在臺海周邊活動。"
)

MIXED_UNITS = (  # 同句混用「架次」與「架」（nid 87155 實例）
    _HEADER
    + "中華民國115年7月26日（星期日）0600時至115年7月27日（星期一）0600時止。 "
    "二、活動動態： 迄0600時止，偵獲共機3架次（逾越中線進入西南空域1架）、共艦7艘及公務船4艘。"
)


def test_single_aircraft_unit():
    p = parse_pla_detail(SINGLE_AIRCRAFT)
    assert p is not None
    assert p["report_date"] == "2026-07-27"
    assert p["aircraft_sorties"] == 1
    assert p["crossed_median_line_cnt"] == 0     # 括號無「逾越中線」
    assert p["plan_vessels"] == 7
    assert p["adiz_southwestern"] and not p["adiz_eastern"]


def test_mixed_units_in_one_sentence():
    p = parse_pla_detail(MIXED_UNITS)
    assert p is not None
    assert p["aircraft_sorties"] == 3
    assert p["crossed_median_line_cnt"] == 1     # 括號內「1架」
    assert p["official_ships"] == 4


def test_legacy_number_first_fallback():
    p = parse_pla_detail(LEGACY_FORMAT)
    assert p is not None
    assert p["aircraft_sorties"] == 12
    assert p["crossed_median_line_cnt"] == 8         # 舊句型數字在前
    assert p["adiz_eastern"]


def test_parse_page_maincontent_and_track_chart():
    html = (
        '<html><nav>新聞與公告 區域動態 導覽列雜訊</nav>'
        '<div class="maincontent"><p>' + CURRENT_NO_MEDIAN + '</p>'
        '<img src="https://www.mnd.gov.tw/NewUpload/202608/1150801_圖_272482.jpg" />'
        "</div><footer>頁尾</footer></html>"
    )
    p = parse_pla_page(html)
    assert p is not None
    assert p["track_chart_url"] == "https://www.mnd.gov.tw/NewUpload/202608/1150801_圖_272482.jpg"
    assert "導覽列雜訊" not in p["raw_text"]          # raw_text 只存內文
    assert "偵獲共機5架次" in p["raw_text"]
