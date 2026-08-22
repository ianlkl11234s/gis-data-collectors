"""
NCDR 災害示警的編碼防護測試（2026-08-22 加）。

背景：這支 collector 曾經用 `r.text` 讓 requests 猜編碼，中文 UTF-8 位元組被猜成
西里爾單位元組碼頁。16,815 筆裡壞 95 筆（0.6%），**從 2022 年零星壞到 2026 年
都沒被發現** —— 因為壞的比例低，而且同一支 collector 的 JSON 路徑（feed_title /
author）是好的，畫面上只有部分欄位亂碼，看起來像個別資料品質問題。

兩層防護各測一次：
  1. 解析路徑：bytes 進 ET.fromstring，由 XML 宣告決定編碼（不看 HTTP header）
  2. 寫入守門：真的壞掉的字串必須被 _mojibake_fields 認出來

測資是**當時真的寫進正式庫的那串位元組**，不是自己編的。
"""
import xml.etree.ElementTree as ET

import pytest

from collectors.ncdr_alerts import (
    CAP_NS,
    MOJIBAKE_CHECK_FIELDS,
    _mojibake_fields,
)

# 2026-08-22 從正式庫撈出來的真實壞資料。
# 兩種花樣：utf8→ptcp154（75 筆）與 utf8→cp1251（16 筆）。
REAL_MOJIBAKE = {
    "重大火災事件通報": "йҮҚеӨ§зҒ«зҒҪдәӢд»¶йҖҡе ұ",
    "內政部消防署": "е…§ж”ҝйғЁж¶ҲйҳІзҪІ",
    "停水_ptcp154": "еҒңж°ҙ",
    "停水_cp1251": "еЃњж°ґ",
    "海洋污染事件": "жө·жҙӢжұЎжҹ“дәӢд»¶",
}


def _clean_parsed(**overrides):
    base = {f: "水利署淹水感測達門檻" for f in MOJIBAKE_CHECK_FIELDS}
    base.update(overrides)
    return base


class TestMojibakeGuard:
    def test_clean_record_passes(self):
        assert _mojibake_fields(_clean_parsed()) == []

    @pytest.mark.parametrize("label,garbled", sorted(REAL_MOJIBAKE.items()))
    def test_real_mojibake_is_rejected(self, label, garbled):
        """每一種真實壞法都要被認出來"""
        assert _mojibake_fields(_clean_parsed(headline=garbled)) == ["headline"]

    def test_reports_every_bad_field_not_just_first(self):
        parsed = _clean_parsed(
            headline=REAL_MOJIBAKE["重大火災事件通報"],
            sender_name=REAL_MOJIBAKE["內政部消防署"],
        )
        assert set(_mojibake_fields(parsed)) == {"headline", "sender_name"}

    def test_ignores_non_string_and_missing_fields(self):
        """geom 之類的非字串欄位、或缺欄位，不該讓守門炸掉"""
        assert _mojibake_fields({"headline": None, "description": 123}) == []
        assert _mojibake_fields({}) == []

    def test_legit_chinese_and_ascii_pass(self):
        """正常內容不可誤擋 —— 誤擋會讓真示警進不來，比亂碼更糟"""
        parsed = _clean_parsed(
            headline="陸上強風特報(黃色燈號)",
            area_desc="臺南市玉井區 / 臺南市南化區",
            sender_name="中央氣象署",
            event="Wind",
            description="Advisory / Minor / Observed",
            instruction="請注意強風",
        )
        assert _mojibake_fields(parsed) == []


class TestCapDecoding:
    """解析路徑本身：bytes 進去，由 XML 宣告決定編碼"""

    CAP_XML = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<alert xmlns="urn:oasis:names:tc:emergency:cap:1.2">'
        "<identifier>TEST-1</identifier>"
        "<info><headline>重大火災事件通報</headline>"
        "<senderName>內政部消防署</senderName></info>"
        "</alert>"
    ).encode("utf-8")

    def test_bytes_path_decodes_chinese_correctly(self):
        root = ET.fromstring(self.CAP_XML)
        headline = root.find(".//cap:headline", CAP_NS)
        assert headline is not None and headline.text == "重大火災事件通報"

    def test_str_path_with_wrong_charset_produces_the_bug(self):
        """
        回歸測試：證明「先用錯的編碼轉成 str 再解析」就是當初的 bug。

        這正是 `r.text` 在上游沒送 charset 時做的事 —— requests 退回 chardet 猜測，
        中文 UTF-8 位元組常被猜成西里爾碼頁。所以 _fetch_cap 必須回 bytes。
        """
        wrongly_decoded = self.CAP_XML.decode("ptcp154")
        # 用錯的編碼解出來的字串，中文已經變成西里爾字母
        assert "重大火災事件通報" not in wrongly_decoded
        assert _mojibake_fields({"headline": wrongly_decoded}) == ["headline"]
