"""utils.memory 與 AISStream 靜態快取上限（記憶體改造 D）。"""
import sys

from utils import memory


def test_malloc_trim_is_quiet_noop_off_linux(monkeypatch):
    monkeypatch.setattr(memory, "_libc_loaded", False)
    monkeypatch.setattr(memory, "_libc", None)
    monkeypatch.setattr(sys, "platform", "darwin")
    assert memory.malloc_trim() is False


def test_malloc_trim_calls_libc_when_available(monkeypatch):
    calls = []

    class _Lib:
        def malloc_trim(self, pad):
            calls.append(pad)
            return 1

    monkeypatch.setattr(memory, "_libc_loaded", True)
    monkeypatch.setattr(memory, "_libc", _Lib())
    assert memory.malloc_trim() is True
    assert calls == [0]


def test_main_sets_arrow_system_pool_without_overriding(monkeypatch):
    import importlib
    monkeypatch.setenv("ARROW_DEFAULT_MEMORY_POOL", "jemalloc")
    import main
    importlib.reload(main)
    import os
    assert os.environ["ARROW_DEFAULT_MEMORY_POOL"] == "jemalloc"
    monkeypatch.delenv("ARROW_DEFAULT_MEMORY_POOL")
    importlib.reload(main)
    assert os.environ["ARROW_DEFAULT_MEMORY_POOL"] == "system"


def test_aisstream_static_cache_is_bounded_lru():
    from workers.aisstream import _BoundedLRU

    cache = _BoundedLRU(2)
    cache["a"] = {"ship_name": "A"}
    cache["b"] = {"ship_name": "B"}
    assert cache.get("a") == {"ship_name": "A"}  # a 變最新
    cache["c"] = {"ship_name": "C"}               # 擠掉最舊的 b
    assert list(cache) == ["a", "c"]
    assert cache.get("b", {}) == {}
    cache["a"] = {"ship_name": "A2"}
    assert len(cache) == 2 and cache.get("a") == {"ship_name": "A2"}
