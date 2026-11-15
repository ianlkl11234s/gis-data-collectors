"""0 筆心跳的尾巴（COL-ZERO-1）：「因例外而空」不可寫成功心跳，「正常抓到 0 筆」照寫。

走真的 collect() → BaseCollector.run() → SupabaseWriter.write()，只 mock 上游抓取與連線池，
用 _report_heartbeat 的呼叫紀錄判斷 metadata.collector_status 會收到什麼。
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from tests.test_supabase_writer import writer_with_mock_pool  # noqa: F401（pytest fixture）


def _prepare(collector_cls, writer):
    """繞過 BaseCollector.__init__（不建 storage／不連線），接上測試用 writer。"""
    c = collector_cls.__new__(collector_cls)
    c.storage = MagicMock()
    c.storage.save.return_value = '/tmp/fake.json'
    c.supabase_writer = writer
    c.last_run = None
    c.last_success_at = None
    c.run_count = 0
    c.error_count = 0
    c.consecutive_errors = 0
    return c


def _run(c, writer):
    with patch.object(writer, '_report_heartbeat') as hb, \
            patch('collectors.base.notify_error'), patch('collectors.base.notify_success'):
        stats = c.run()
    return stats, [call.args[:3] for call in hb.call_args_list]


# ── 代表 1：collect() 直接吞例外（wic_sewer；同型 wic_evacuate／wic_pumb／tpml_seat／uswg）──

def test_wic_sewer_fetch_exception_no_success_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.wic_sewer import WicSewerCollector
    c = _prepare(WicSewerCollector, writer)
    c._fetch = MagicMock(side_effect=requests.ConnectionError('reset'))
    stats, hbs = _run(c, writer)
    assert 'error' in stats and c.last_success_at is None
    assert not any(success for _, success, _ in hbs)


def test_wic_sewer_normal_zero_rows_success_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.wic_sewer import WicSewerCollector
    c = _prepare(WicSewerCollector, writer)
    c._fetch = MagicMock(return_value=[])
    stats, hbs = _run(c, writer)
    assert 'error' not in stats
    assert hbs == [(c.name, True, 0)]


# ── 代表 2：多來源迴圈，每個來源都拋例外（youbike；同型 bus／parking／road_congestion…）──

def _youbike(writer, fetch):
    from collectors.youbike import YouBikeCollector
    c = _prepare(YouBikeCollector, writer)
    c.cities = ['Taipei', 'NewTaipei']
    c._fetch_city = MagicMock(side_effect=fetch)
    return c


@patch('collectors.youbike.time.sleep')
def test_youbike_all_cities_fail_no_success_heartbeat(_sleep, writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    c = _youbike(writer, requests.Timeout('timed out'))
    stats, hbs = _run(c, writer)
    assert 'error' in stats
    assert not any(success for _, success, _ in hbs)


@patch('collectors.youbike.time.sleep')
def test_youbike_cities_ok_but_empty_success_heartbeat(_sleep, writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    c = _youbike(writer, lambda city: [])
    stats, hbs = _run(c, writer)
    assert 'error' not in stats
    assert hbs == [(c.name, True, 0)]


# ── 代表 3：列表抓取失敗被吞成空清單（pla_activity_daily）──

def test_pla_activity_list_failure_no_success_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.pla_activity_daily import PlaActivityDailyCollector
    c = _prepare(PlaActivityDailyCollector, writer)
    c._fetch_list_ids = MagicMock(side_effect=requests.ConnectionError('reset'))
    stats, hbs = _run(c, writer)
    assert 'error' in stats
    assert not any(success for _, success, _ in hbs)


def test_pla_activity_empty_list_success_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.pla_activity_daily import PlaActivityDailyCollector
    c = _prepare(PlaActivityDailyCollector, writer)
    c._fetch_list_ids = MagicMock(return_value=[])
    stats, hbs = _run(c, writer)
    assert 'error' not in stats
    assert hbs == [(c.name, True, 0)]


# ── 代表 4：helper 吞例外回 None（jtwc RSS）vs 上游正常但沒有颱風 ──

def test_jtwc_rss_failure_no_success_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.global_climate.jtwc import JtwcCollector
    c = _prepare(JtwcCollector, writer)
    c._fetch_text = MagicMock(return_value=None)
    stats, hbs = _run(c, writer)
    assert 'error' in stats
    assert not any(success for _, success, _ in hbs)


def test_jtwc_no_active_storm_success_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.global_climate.jtwc import JtwcCollector
    c = _prepare(JtwcCollector, writer)
    c._fetch_text = MagicMock(return_value='<rss><channel></channel></rss>')
    stats, hbs = _run(c, writer)
    assert 'error' not in stats
    assert hbs == [(c.name, True, 0)]


def test_collector_error_with_zero_rows_writes_one_failure_heartbeat(writer_with_mock_pool):  # noqa: F811
    """帶 _collector_error 且 0 筆：writer 記一次失敗心跳（原本整輪不動，#203 副作用），
    run() 不再重複補報。"""
    writer, _ = writer_with_mock_pool
    from collectors.wic_pumb import WicPumbCollector
    c = _prepare(WicPumbCollector, writer)
    c._fetch = MagicMock(side_effect=requests.ConnectionError('reset'))
    stats, hbs = _run(c, writer)
    assert 'error' in stats and c.last_success_at is None
    assert hbs == [(c.name, False, 0)]


def test_zero_rows_failure_heartbeat_error_redacted_and_truncated(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    with patch.object(writer, '_report_heartbeat') as hb:
        assert writer.write('wic_pumb', {'data': [], '_collector_error': _LEAKY + 'x' * 500}, None) is True
    (name, success, records, error), = [call.args for call in hb.call_args_list]
    assert (name, success, records) == ('wic_pumb', False, 0)
    assert 'SECRET123' not in error and len(error) <= 200


# ── 不經 writer 回報心跳的 opt-in collector（heartbeat_without_writer）──

def _jma_typhoon(writer, fetch_json):
    from collectors.global_climate.jma_typhoon import JmaTyphoonCollector
    c = _prepare(JmaTyphoonCollector, writer)
    c._fetch_json = fetch_json
    return c


def test_typhoon_no_active_storm_success_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    c = _jma_typhoon(writer, lambda url: [])
    stats, hbs = _run(c, writer)
    assert 'error' not in stats and stats['status'] == 'no_active_storm'
    assert hbs == [(c.name, True, 0)]
    c.storage.save.assert_not_called()


def test_typhoon_fetch_exception_failure_heartbeat_only(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.global_climate.jma_typhoon import JmaFetchError

    def fail(url):
        raise JmaFetchError('timeout')
    c = _jma_typhoon(writer, fail)
    stats, hbs = _run(c, writer)
    assert 'error' in stats and c.last_success_at is None
    assert hbs == [(c.name, False, 0)]


def test_typhoon_with_rows_heartbeat_from_writer_only(writer_with_mock_pool):  # noqa: F811
    """有 data 且有 transformer → 心跳由 writer 發，base 不重複。"""
    writer, _ = writer_with_mock_pool
    c = _jma_typhoon(writer, lambda url: [])
    c.collect = MagicMock(return_value={'status': 'ok', 'data': []})
    _, hbs = _run(c, writer)
    assert hbs == [(c.name, True, 0)]


def test_tra_static_success_heartbeat_without_db_write(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.tra_static import TRAStaticCollector
    c = _prepare(TRAStaticCollector, writer)
    c.collect = MagicMock(return_value={'fetch_time': 't', 'data': {'stations': {'count': 0, 'data': []}}})
    stats, hbs = _run(c, writer)
    assert 'error' not in stats
    assert hbs == [(c.name, True, 0)]


def test_tra_static_exception_failure_heartbeat(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.tra_static import TRAStaticCollector
    c = _prepare(TRAStaticCollector, writer)
    c.collect = MagicMock(side_effect=requests.ConnectionError(_LEAKY))
    stats, hbs = _run(c, writer)
    assert 'error' in stats
    assert hbs == [(c.name, False, 0)]


def test_opt_out_collector_without_data_gets_no_base_heartbeat(writer_with_mock_pool):  # noqa: F811
    """未 opt-in 的 collector 行為不變：不帶 data 的成功輪次不發心跳。"""
    writer, _ = writer_with_mock_pool
    from collectors.wic_pumb import WicPumbCollector
    c = _prepare(WicPumbCollector, writer)
    assert WicPumbCollector.heartbeat_without_writer is False
    c.collect = MagicMock(return_value={'status': 'idle'})
    _, hbs = _run(c, writer)
    assert hbs == []


# ── 存檔不可帶金鑰：collect() 的 result 會原樣存本機 JSON 並歸檔 S3（base.run → storage.save）──

_LEAKY = 'HTTPSConnectionPool: https://opendata.cwa.gov.tw/api?Authorization=SECRET123&format=JSON'


def _saved_text(c):
    import json
    return json.dumps([call.args for call in c.storage.save.call_args_list], ensure_ascii=False, default=str)


def test_wic_sewer_error_fields_redacted_in_saved_result(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.wic_sewer import WicSewerCollector
    c = _prepare(WicSewerCollector, writer)
    c._fetch = MagicMock(side_effect=requests.ConnectionError(_LEAKY))
    stats, _ = _run(c, writer)
    saved = _saved_text(c)
    assert 'Authorization=' in saved          # 確實存了錯誤欄位
    assert 'SECRET123' not in saved and 'SECRET123' not in str(stats)


def test_pla_activity_list_error_redacted_in_saved_result(writer_with_mock_pool):  # noqa: F811
    writer, _ = writer_with_mock_pool
    from collectors.pla_activity_daily import PlaActivityDailyCollector
    c = _prepare(PlaActivityDailyCollector, writer)
    c._fetch_list_ids = MagicMock(side_effect=requests.ConnectionError(_LEAKY))
    stats, _ = _run(c, writer)
    saved = _saved_text(c)
    assert '_collector_error' in saved
    assert 'SECRET123' not in saved and 'SECRET123' not in str(stats)
