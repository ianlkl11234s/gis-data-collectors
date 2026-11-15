"""interval 型 collector 的首次觸發錯開上限（2026-11-16：每小時 collector 擠在整點後 5 分鐘）。"""
import schedule


class _Sched:
    def submit(self, _collector):
        return None


class _Collector:
    fixed_minutes = ()

    def __init__(self, interval_minutes):
        self.name = f"fake_{interval_minutes}"
        self.interval_minutes = interval_minutes


def test_jitter_cap_by_interval():
    import main

    assert main._jitter_cap_seconds(1) == 60        # 不超過 interval 本身
    assert main._jitter_cap_seconds(3) == 180
    assert main._jitter_cap_seconds(10) == 300      # 短週期仍 ≤ 5 分鐘
    assert main._jitter_cap_seconds(59) == 300
    assert main._jitter_cap_seconds(60) == 900      # 每小時以上最多 15 分鐘
    assert main._jitter_cap_seconds(1440) == 900


def test_schedule_collector_uses_cap(monkeypatch):
    import main

    seen = []
    monkeypatch.setattr(main.random, "uniform", lambda lo, hi: seen.append((lo, hi)) or hi)
    schedule.clear()
    try:
        for interval in (3, 60, 1440):
            main._schedule_collector(_Collector(interval), _Sched())
        assert seen == [(0, 180), (0, 900), (0, 900)]
        assert all(job.unit == "minutes" for job in schedule.get_jobs())
    finally:
        schedule.clear()


def test_fixed_minutes_collector_not_jittered(monkeypatch):
    import main

    called = []
    monkeypatch.setattr(main.random, "uniform", lambda lo, hi: called.append(1) or 0)
    c = _Collector(60)
    c.fixed_minutes = (7, 37)
    schedule.clear()
    try:
        main._schedule_collector(c, _Sched())
        assert called == []
        assert sorted(job.at_time.minute for job in schedule.get_jobs()) == [7, 37]
    finally:
        schedule.clear()
