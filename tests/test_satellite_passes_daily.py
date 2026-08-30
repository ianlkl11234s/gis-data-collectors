from datetime import datetime, timezone

from collectors.satellite_passes_daily import SatellitePassesDailyCollector


class _Cursor:
    def __init__(self, rows):
        self._rows = iter(rows)
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchone(self):
        return next(self._rows)


class _Connection:
    def __init__(self, cursor_rows):
        self.cursors = [_Cursor(rows) for rows in cursor_rows]
        self.used_cursors = []

    def cursor(self):
        cursor = self.cursors.pop(0)
        self.used_cursors.append(cursor)
        return cursor


def test_refresh_isr_daily_calls_each_tier_mode_per_day_and_sums_rows():
    conn = _Connection([[(1,), (1,), (1,), (1,), (0,), (1,)]])
    collector = SatellitePassesDailyCollector()
    days = [
        datetime(2026, 8, 28, tzinfo=timezone.utc),
        datetime(2026, 8, 29, tzinfo=timezone.utc),
    ]

    refreshed = collector._refresh_isr_daily(conn, days)

    assert refreshed == 5
    assert conn.used_cursors[0].executed == [
        (
            "SELECT public.refresh_isr_satellite_passes_daily(%s::date, %s, %s)",
            (days[0].date(), "twmain_12nm", "confirmed_only"),
        ),
        (
            "SELECT public.refresh_isr_satellite_passes_daily(%s::date, %s, %s)",
            (days[0].date(), "twmain_12nm", "confirmed_plus_dual_use"),
        ),
        (
            "SELECT public.refresh_isr_satellite_passes_daily(%s::date, %s, %s)",
            (days[0].date(), "twmain_12nm", "all_non_excluded"),
        ),
        (
            "SELECT public.refresh_isr_satellite_passes_daily(%s::date, %s, %s)",
            (days[1].date(), "twmain_12nm", "confirmed_only"),
        ),
        (
            "SELECT public.refresh_isr_satellite_passes_daily(%s::date, %s, %s)",
            (days[1].date(), "twmain_12nm", "confirmed_plus_dual_use"),
        ),
        (
            "SELECT public.refresh_isr_satellite_passes_daily(%s::date, %s, %s)",
            (days[1].date(), "twmain_12nm", "all_non_excluded"),
        ),
    ]


def test_report_direct_write_heartbeat_uses_collector_name_and_record_count():
    heartbeat_cursor = _Cursor([(None,)])

    class HeartbeatConnection:
        def cursor(self):
            return heartbeat_cursor

    collector = SatellitePassesDailyCollector()
    collector._report_direct_write_heartbeat(HeartbeatConnection(), 2)

    assert heartbeat_cursor.executed == [
        (
            "SELECT public.report_collector_heartbeat(%s, %s, %s, %s)",
            ("satellite_passes_daily", True, 2, None),
        )
    ]
