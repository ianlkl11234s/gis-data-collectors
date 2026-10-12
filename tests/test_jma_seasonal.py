"""JMA 季節觀測（jma_seasonal）單元測試。

不打網路、不打 DB：HTML／XML／CSV 為 2026-10-12 實打頁面的裁剪樣本（保留原始標記結構），
另加跨年與缺測列以覆蓋邊界。
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from collectors.global_climate import jma_seasonal as js
from storage.supabase_tables import TABLE_MAP
from storage.supabase_writer import SupabaseWriter

COLLECTED = "2026-10-12T15:00:00+08:00"

# 累年 CSV（Shift_JIS 解碼後）：首兩列為表頭；札幌 2024 年欄 0＝缺測、那覇 2025 年欄 1225（隔年欄規則）
RUINENCHI_004 = (
    "4,さくらの開花,,,,,,,,,,,,,,\n"
    "番号,地点名,2023,rm,2024,rm,2025,rm,2026,rm,平年値,rm,最早値,rm,最早年,最晩値,rm,最晩年\n"
    "412,札幌　　,415,8,0,0,422,8,0,0,501,8,421,8,2008,514,8,1980\n"
    "936,那覇　　,116,6,123,7,1225,7,0,0,116,6,103,6,1990,208,6,1983\n"
    "945,南大東島,122,7,203,7,115,7,0,0,120,6,104,6,1990,203,6,1990\n"
)
RUINENCHI_016 = (
    "16,かえでの落葉,,,,,,,,,,,,,,\n"
    "番号,地点名,2023,rm,2024,rm,2025,rm,2026,rm,平年値,rm,最早値,rm,最早年,最晩値,rm,最晩年\n"
    "412,札幌　　,1120,8,1118,9,1125,8,0,0,1115,8,1030,8,1960,1210,8,1980\n"
    "827,鹿児島　,1229,6,105,6,0,0,0,0,1226,6,1201,6,1960,120,6,1980\n"
)
AMEDAS_TABLE = {
    "14163": {"type": "A", "kjName": "札幌", "lat": [43, 3.6], "lon": [141, 19.7]},
    "91197": {"type": "A", "kjName": "那覇", "lat": [26, 12.4], "lon": [127, 41.2]},
    "92011": {"type": "A", "kjName": "南大東", "lat": [25, 49.7], "lon": [131, 13.6]},
    "88317": {"type": "A", "kjName": "鹿児島", "lat": [31, 33.3], "lon": [130, 32.8]},
    "99999": {"type": "C", "kjName": "札幌", "lat": [40, 0.0], "lon": [140, 0.0]},  # 同名非官署
}

PHN_014 = """<h1>かえでの紅葉日  (2025年-2026年)</h1>
<TABLE class=data><TBODY>
<TR class=mtx><th rowspan=2 scope="col">地点名</th><th colspan=3 scope="col">2025年</th>
<th colspan=3 scope="col">2026年</th><th rowspan=2 scope="col">代替種目による観測</th></tr>
<TR class=mtx><th scope="col">観測日</th><th scope="col">平年差</th><th scope="col">昨年差</th>
<th scope="col">観測日</th><th scope="col">平年差</th><th scope="col">昨年差</th></tr>
<TR class=mtx><td>
札幌
</td><td align="center" valign="middle">
11月 6日
</td><td align="center" valign="middle">
+9
</td><td align="center" valign="middle">
-5
</td><td align="center" valign="middle">
10月 9日
</td><td align="center" valign="middle">
///
</td><td align="center" valign="middle">
-28
</td><td>やまもみじ</td></tr>
<TR class=mtx><td>
釧路
</td><td align="center" valign="middle">
-
</td><td align="center" valign="middle">
///
</td><td align="center" valign="middle">
///
</td><td align="center" valign="middle">
-
</td><td align="center" valign="middle">
///
</td><td align="center" valign="middle">
///
</td><td></td></tr>
<TR class=mtx><td>
鹿児島
</td><td align="center" valign="middle">
1月 5日
</td><td align="center" valign="middle">
+10
</td><td align="center" valign="middle">
0
</td><td align="center" valign="middle">
-
</td><td align="center" valign="middle">
///
</td><td align="center" valign="middle">
///
</td><td></td></tr>
</TBODY></TABLE>"""

SAKURA_KAIKA = """<h1>2026年のさくらの開花状況
<br>（本ページは12月から6月までの間、1日3回更新しています。<br></h1>
<table width='900' class='data2_s'>
<tr class='mtx'><th colspan='7' align='left' scope='col'>【沖縄県】</th></tr>
<tr class='mtx'><th width='100' scope='col'>地点名</th><th scope='col'>観測日</th><th scope='col'>平年差(日)</th>
<th scope='col'>平年日</th><th scope='col'>昨年差(日)</th><th scope='col'>昨年日</th><th scope='col'>種類</th></tr>
<tr class='mtx'>
<th scope='row'>那覇     </th>
<td align='right'>12月25日</td>
<td align='right'>-22</td>
<td align='right'> 1月16日</td>
<td align='right'>-11</td>
<td align='right'> 1月 5日</td>
<td>ひかんざくら</td>
</tr>
<tr class='mtx'>
<th scope='row'>札幌     </th>
<td align='right'> 4月22日</td>
<td align='right'>-9</td>
<td align='right'> 5月 1日</td>
<td align='right'>0</td>
<td align='right'> 4月22日</td>
<td></td>
</tr>
<tr class='mtx'>
<th scope='row'>南大東島 </th>
<td align='right'>-</td>
<td align='right'>///</td>
<td align='right'> 1月20日</td>
<td align='right'>///</td>
<td align='right'> 1月15日</td>
<td>ひかんざくら</td>
</tr>
</table>"""

VGSK55 = """<?xml version="1.0" encoding="UTF-8"?>
<Report xmlns="http://xml.kishou.go.jp/jmaxml1/" xmlns:jmx="http://xml.kishou.go.jp/jmaxml1/">
  <Control><Title>生物季節観測</Title><DateTime>2026-10-06T00:25:15Z</DateTime><Status>通常</Status></Control>
  <Head xmlns="http://xml.kishou.go.jp/jmaxml1/informationBasis1/">
    <Title>生物季節観測</Title><ReportDateTime>2026-10-06T09:23:00+09:00</ReportDateTime>
    <TargetDateTime>2026-10-06T00:00:00+09:00</TargetDateTime><InfoType>発表</InfoType>
    <InfoKind>生物季節観測報告気象報</InfoKind>
  </Head>
  <Body xmlns="http://xml.kishou.go.jp/jmaxml1/body/meteorology1/">
    <MeteorologicalInfos type="生物季節観測"><MeteorologicalInfo>
      <DateTime significant="yyyy-mm-dd">2026-10-06T00:00:00+09:00</DateTime>
      <Item>
        <Kind><Name>すすきの開花日</Name><Code>11</Code><ClassName>ススキ</ClassName><Condition>通常</Condition></Kind>
        <Station><Name>札幌管区気象台</Name><Code type="国際地点番号">47412</Code><Location>札幌市</Location><Status>構内</Status></Station>
      </Item>
    </MeteorologicalInfo></MeteorologicalInfos>
    <AdditionalInfo><ObservationAddition><DeviationFromNormal>7</DeviationFromNormal><DeviationFromLastYear /></ObservationAddition></AdditionalInfo>
  </Body>
</Report>"""

FEED = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry><title>生物季節観測</title>
    <link type="application/xml" href="https://www.data.jma.go.jp/developer/xml/data/20261006002515_0_VGSK55_016000.xml"/></entry>
  <entry><title>地方海上警報（Ｈ２８）</title>
    <link type="application/xml" href="https://www.data.jma.go.jp/developer/xml/data/20261006002515_0_VPCU51_010000.xml"/></entry>
</feed>"""

SNOWCAP = """<div id="main"><h1>初冠雪の記録等</h1>
<h2 id="latest_snw">今季の山頂における積雪の状況（2027寒候年：2026年８月～2027年７月）</h2>
<p class="mtx">2026年10月12日8時 時点の山頂における積雪の状況を表示しています。<br/>※　毎時25分頃更新します。</p>
<table class="data">
<tr class="mtx"><th>山岳名</th><th>初冠雪日</th><th> 最新の積雪状況 </th></tr>
<tr class="mtx"><td style="white-space:nowrap">利尻山（リシリザン）</td><td style="text-align: center;">--</td><td class="none" style="text-align: center;">積雪なし</td></tr>
<tr class="mtx"><td style="white-space:nowrap">旭岳（アサヒダケ）</td><td style="text-align: center;">2026年10月02日</td><td class="none" style="text-align: center;">積雪なし</td></tr>
</table>
<h2 id="past_obs">過去の初冠雪日の記録（2026寒候年まで）</h2>
<table class="data"><tr><th>山岳名（カナ）</th><th>観測地点名</th></tr>
<tr><td><a href="./records/100.html">利尻山(リシリザン)</a></td><td>稚内</td></tr>
<tr><td><a href="./records/104.html">旭岳(アサヒダケ)</a></td><td>旭川</td></tr>
</table></div>"""

SNOW_RECORD = """<h1>過去の初冠雪日の記録（2026寒候年まで）　旭岳</h1><table class="data2_s">
<tr><th>寒候年</th><th>初冠雪年月日</th></tr>
<tr><td>2020</td><td>2019/09/21</td></tr>
<tr><td>2021</td><td>2020/09/26</td></tr>
<tr><td>2022</td><td>--</td></tr>
<tr><td>2026</td><td>2025/09/27</td></tr>
</table>"""

BAIU = """<h1>令和8年の梅雨入りと梅雨明け（速報値）</h1>
<table class="onset"><caption><div class="flex-box"><div class="title">令和8年の梅雨入り</div>
<div class="update">更新日：令和8年8月4日 </div></div></caption>
<tbody class="onset"><tr><th scope="row">沖縄</th>
<td class="this-year event-body">5月4日頃</td><td class="middle-body">早い</td><td class="middle-body">6日早い</td>
<td class="class-body">1日早い</td><td>5月10日頃</td><td class="event-body">5月5日頃</td><td class="class-body">早い</td></tr>
<tr><th scope="row">東海</th>
<td class="this-year event-body">－</td><td class="middle-body">－</td><td class="middle-body">－</td>
<td class="class-body">同じ</td><td>6月6日頃</td><td class="event-body">5月17日頃</td><td class="class-body">かなり早い</td></tr>
</tbody></table>
<table class="over"><caption><div class="title">令和8年の梅雨明け</div></caption>
<tbody class="over"><tr><th scope="row">沖縄</th>
<td class="this-year event-body">6月29日頃</td><td class="middle-body">遅い</td><td class="middle-body">8日遅い</td>
<td class="class-body">22日遅い</td><td>6月21日頃</td><td class="event-body">6月7日頃</td><td class="class-body">かなり早い</td></tr>
</tbody></table>"""


@pytest.fixture
def stations():
    return js.build_station_index(RUINENCHI_004 + "\n".join(RUINENCHI_016.splitlines()[2:]), AMEDAS_TABLE)


# ───────────────────────── 日期解碼／站點 ─────────────────────────

def test_decode_mmdd_cross_year_and_missing():
    assert js.decode_mmdd("1225", 2025, "spring") == date(2024, 12, 25)  # 冬春種目：記在隔年欄
    assert js.decode_mmdd("105", 2024, "autumn") == date(2025, 1, 5)     # 秋種目：記在前一年欄
    assert js.decode_mmdd("422", 2025, "spring") == date(2025, 4, 22)
    assert js.decode_mmdd("0", 2025, "spring") is None                   # 0＝缺測，不當 0
    assert js.decode_mmdd("", 2025, "spring") is None
    assert js.decode_mmdd("231", 2025, "spring") is None                 # 不存在的日期


def test_season_year_roundtrip():
    assert js.season_year_of(date(2024, 12, 25), "spring") == 2025
    assert js.season_year_of(date(2025, 1, 5), "autumn") == 2024
    assert js.season_year_of(date(2026, 10, 6), "calendar") == 2026


def test_station_index_prefers_official_and_alias(stations):
    assert stations["札幌"]["station_no"] == "47412"
    assert stations["札幌"]["lat"] == pytest.approx(43.06)               # type A，不是同名 C 站
    assert stations["南大東島"]["lat"] == pytest.approx(25.82833)        # 站表叫「南大東」
    assert stations["鹿児島"]["station_no"] == "47827"


def test_ruinenchi_csv_years_missing_alt_and_normal(stations):
    rows = js.parse_ruinenchi_csv(RUINENCHI_004, "004", stations, years=[2024, 2025],
                                  source_ref="u", source_time=None, collected_at=COLLECTED)
    got = {(r["station_name"], r["season_year"]): r for r in rows}
    assert ("札幌", 2024) not in got                                     # 0／rm 0 缺測不寫列
    assert got[("札幌", 2025)]["obs_date"] == "2025-04-22"
    assert got[("札幌", 2025)]["rm_code"] == 8 and got[("札幌", 2025)]["alt_species"] is None
    naha = got[("那覇", 2025)]
    assert naha["obs_date"] == "2024-12-25" and naha["rm_code"] == 7
    assert naha["alt_species"] == "代替種目" and naha["site_status"] == "付近"
    assert naha["normal_mmdd"] == 116 and naha["normal_date"] == "2025-01-16"
    assert naha["diff_normal_days"] is None                              # CSV 無差值，不自算
    autumn = js.parse_ruinenchi_csv(RUINENCHI_016, "016", stations, years=[2024, 2025],
                                    source_ref="u", source_time=None, collected_at=COLLECTED)
    kago = {(r["station_name"], r["season_year"]): r for r in autumn}
    assert kago[("鹿児島", 2024)]["obs_date"] == "2025-01-05"
    assert ("鹿児島", 2025) not in kago


# ───────────────────────── 當年 HTML ─────────────────────────

def test_phn_page_unobserved_alt_and_cross_year(stations):
    rows = js.parse_phn_page(PHN_014, "015", stations, source_ref="u", source_time=None,
                             collected_at=COLLECTED)
    got = {(r["station_name"], r["season_year"]): r for r in rows}
    assert set(got) == {("札幌", 2025), ("札幌", 2026), ("鹿児島", 2025)}  # 「-」不寫列
    s25 = got[("札幌", 2025)]
    assert s25["obs_date"] == "2025-11-06" and s25["diff_normal_days"] == 9
    assert s25["diff_last_year_days"] == -5 and s25["normal_date"] == "2025-10-28"
    assert s25["alt_species"] == "やまもみじ" and s25["source"] == "current_html"
    s26 = got[("札幌", 2026)]
    assert s26["diff_normal_days"] is None and s26["normal_date"] is None  # 「///」→ NULL
    assert got[("鹿児島", 2025)]["obs_date"] == "2026-01-05"              # 秋種目 1 月 → 隔年日曆日
    assert got[("鹿児島", 2025)]["alt_species"] is None
    assert all(r["phenomenon_code"] == "015" and r["phenomenon_name"] == "かえでの紅葉" for r in rows)


def test_sakura_page_species_and_december(stations):
    rows = js.parse_sakura_page(SAKURA_KAIKA, "004", stations, source_ref="u", source_time=None,
                                collected_at=COLLECTED)
    got = {r["station_name"]: r for r in rows}
    assert set(got) == {"那覇", "札幌"}                                    # 南大東島「-」不寫列
    assert got["那覇"]["obs_date"] == "2025-12-25" and got["那覇"]["season_year"] == 2026
    assert got["那覇"]["normal_date"] == "2026-01-16" and got["那覇"]["alt_species"] == "ひかんざくら"
    assert got["札幌"]["alt_species"] is None and got["札幌"]["diff_last_year_days"] == 0
    assert got["札幌"]["station_no"] == "47412"


# ───────────────────────── XML ─────────────────────────

def test_vgsk55_feed_and_report(stations):
    assert js.parse_feed_vgsk55(FEED) == [
        "https://www.data.jma.go.jp/developer/xml/data/20261006002515_0_VGSK55_016000.xml"]
    rows = js.parse_vgsk55(VGSK55, stations, source_ref="x.xml", collected_at=COLLECTED)
    assert len(rows) == 1
    r = rows[0]
    assert r["station_name"] == "札幌" and r["station_no"] == "47412"
    assert r["phenomenon_code"] == "011" and r["phenomenon_name"] == "すすきの開花"
    assert r["obs_date"] == "2026-10-06" and r["season_year"] == 2026
    assert r["diff_normal_days"] == 7 and r["diff_last_year_days"] is None
    assert r["info_type"] == "発表" and r["source"] == "xml" and r["site_status"] == "構内"
    assert r["source_time"] == "2026-10-06T09:23:00+09:00"


# ───────────────────────── 初冠雪／梅雨 ─────────────────────────

def test_snowcap_page_and_records():
    first, status, mountains = js.parse_snowcap_page(SNOWCAP, collected_at=COLLECTED)
    assert mountains["旭岳"] == {"code": "104", "observatory": "旭川", "kana": "アサヒダケ"}
    assert len(first) == 1                                               # 「--」不寫初冠雪列
    f = first[0]
    assert (f["mountain_code"], f["season_year"], f["first_snowcap_date"], f["judgement_method"]) == (
        "104", 2027, "2026-10-02", "analysis")
    assert f["source_time"] == "2026-10-12T08:00:00+09:00"
    assert {s["mountain_code"]: s["status_text"] for s in status} == {"100": "積雪なし", "104": "積雪なし"}
    assert status[0]["status_class"] == "none"
    rec = js.parse_snowcap_record(SNOW_RECORD, mountain_code="104", mountain_name="旭岳",
                                  observatory="旭川", years=range(2021, 2027), collected_at=COLLECTED)
    assert [(r["season_year"], r["first_snowcap_date"]) for r in rec] == [
        (2021, "2020-09-26"), (2026, "2025-09-27")]
    assert all(r["judgement_method"] == "visual" for r in rec)


def test_baiu_page():
    rows = js.parse_baiu_page(BAIU, collected_at=COLLECTED)
    got = {(r["region"], r["event"]): r for r in rows}
    oki = got[("沖縄", "onset")]
    assert oki["year"] == 2026 and oki["period_date"] == "2026-05-04"
    assert oki["diff_normal_days"] == -6 and oki["normal_date"] == "2026-05-10"
    assert oki["page_updated"] == "2026-08-04"
    tokai = got[("東海", "onset")]
    assert tokai["period_text"] == "－" and tokai["period_date"] is None   # 特定不能 → NULL
    assert tokai["diff_normal_days"] is None and tokai["diff_last_year_days"] == 0
    assert got[("沖縄", "end")]["diff_normal_days"] == 8
    assert js.reiwa_to_year("令和元年") == 2019


# ───────────────────────── 只送新項目 ─────────────────────────

def _fake_base_run(result_error=None):
    def run(self):
        out = self.collect()
        return {"error": result_error} if result_error else {k: v for k, v in out.items() if k != "data"}
    return run


def _collector(tmp_path, pages):
    c = js.JmaSeasonalCollector.__new__(js.JmaSeasonalCollector)
    c.name, c._session = "jma_seasonal", None
    c._stations = js.build_station_index(RUINENCHI_004, AMEDAS_TABLE)
    c._stations_loaded_at = datetime.now(timezone.utc)
    c._state, c._pending_state = None, None
    c.state_path = tmp_path / "state" / "jma_seasonal_state.json"
    calls = []

    def fake_get(url, *, encoding=None):
        calls.append(url)
        for key, text in pages.items():
            if url.endswith(key):
                return text, "2026-10-11T23:32:07+00:00"
        raise js.JmaFetchError(f"{url}: 404")

    c._get = fake_get
    c.calls = calls
    return c


def _pages(**over):
    empty_phn = PHN_014.split("<TR class=mtx><td>")[0] + "</TBODY></TABLE>"
    pages = {f"{p}.html": empty_phn for p in js.PHN_PAGES}
    pages["phn_014.html"] = PHN_014
    pages["sakura_kaika.html"] = SAKURA_KAIKA
    pages["sakura_mankai.html"] = SAKURA_KAIKA
    pages["other_l.xml"] = FEED
    pages["VGSK55_016000.xml"] = VGSK55
    pages["index.php"] = SNOWCAP
    pages["sokuhou_baiu.html"] = BAIU
    pages.update(over)
    return pages


def test_only_new_items_after_successful_run(monkeypatch, tmp_path):
    from collectors.base import BaseCollector
    monkeypatch.setattr(BaseCollector, "run", _fake_base_run())
    c = _collector(tmp_path, _pages())
    first = c.collect()
    assert first["new_counts"]["phenology_xml"] == 1 and first["new_counts"]["rainy"] == 3
    assert {r["_type"] for r in first["data"]} == {"phenology", "snowcap", "snow_status", "rainy"}
    assert set(first["raw_payload"]) == {"phenology_html", "phenology_xml", "snowcap", "rainy"}
    json.dumps(first)
    c.run()                                                              # 成功 → state 前進
    c.calls.clear()
    again = c.collect()
    assert again.get("no_change") is True and "data" not in again and "raw_payload" not in again
    assert not any(u.endswith("VGSK55_016000.xml") for u in c.calls)   # 看過的電文不再抓

    # 模擬重啟（從 state 檔讀）＋鹿兒島 2026 新觀測＋旭岳狀態改變
    newer = PHN_014.replace("""<td align="center" valign="middle">
-
</td><td align="center" valign="middle">
///
</td><td align="center" valign="middle">
///
</td><td></td></tr>
</TBODY>""", """<td align="center" valign="middle">
11月30日
</td><td align="center" valign="middle">
-3
</td><td align="center" valign="middle">
///
</td><td></td></tr>
</TBODY>""")
    snow = SNOWCAP.replace('2026年10月12日8時', '2026年10月12日14時').replace(
        '2026年10月02日</td><td class="none" style="text-align: center;">積雪なし',
        '2026年10月02日</td><td class="s1" style="text-align: center;">積雪あり')
    c2 = _collector(tmp_path, _pages(**{"phn_014.html": newer, "index.php": snow}))
    out = c2.collect()
    assert [(r["_type"], r.get("station_name") or r.get("mountain_code")) for r in out["data"]] == [
        ("phenology", "鹿児島"), ("snow_status", "104")]
    assert out["data"][0]["obs_date"] == "2026-11-30"
    assert set(out["raw_payload"]) == {"phenology_html", "snowcap"}


def test_state_not_advanced_when_db_write_fails(monkeypatch, tmp_path):
    from collectors.base import BaseCollector
    monkeypatch.setattr(BaseCollector, "run", _fake_base_run(result_error="required Supabase write failed"))
    c = _collector(tmp_path, _pages())
    c.run()
    assert not c.state_path.exists()
    assert c.collect()["new_counts"]["phenology_html"] > 0


def test_partial_source_failure_flags_error_but_keeps_rows(tmp_path):
    pages = _pages()
    pages.pop("sokuhou_baiu.html")
    c = _collector(tmp_path, pages)
    out = c.collect()
    assert "rainy" in out["source_failures"] and out["_collector_error"]
    assert out["new_counts"]["rainy"] == 0 and out["new_counts"]["phenology_html"] > 0


def test_stale_sources_by_source_time():
    now = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)
    stale = js.JmaSeasonalCollector._stale_sources(
        {"snowcap": "2026-10-12T08:00:00+09:00", "phn_014": "2026-10-11T23:32:07+00:00"}, now)
    assert stale == {"snowcap": 13.0}


# ───────────────────────── writer ─────────────────────────

def test_writer_routes_four_tables_do_nothing(monkeypatch, tmp_path):
    captured = []
    monkeypatch.setattr("storage.supabase_writer.execute_values",
                        lambda cur, sql, values, page_size=100: captured.append((sql, list(values))))
    w = SupabaseWriter.__new__(SupabaseWriter)
    w._pool = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = MagicMock()
    data = _collector(tmp_path, _pages()).collect()["data"]
    bad = dict(data[0], obs_date=None)                                   # 必填欄缺值 → 丟棄
    recs = w._transform_jma_seasonal({"data": data + [bad]}, datetime.now(timezone.utc))
    assert len(recs) == len(data)
    assert TABLE_MAP["jma_seasonal"]["is_multi_table"] is True
    w._write_multi_table(conn, "jma_seasonal", recs)
    tables = {sql.split()[2]: vals for sql, vals in captured}
    assert set(tables) == {"live.jma_phenology_observations", "live.jma_snowcap",
                           "live.jma_snowcap_status", "live.jma_rainy_season"}
    assert all(sql.rstrip().endswith("ON CONFLICT DO NOTHING") for sql, _ in captured)
