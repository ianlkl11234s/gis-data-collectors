"""AISStream registry metadata shim。

實際執行由 ``workers.aisstream.AISStreamWorker`` 負責；此 class 只讓既有
collector registry / cross-layer consistency tooling 能辨識 persistent provider。
"""

from .base import BaseCollector


class AISStreamCollector(BaseCollector):
    name = "aisstream"
    interval_minutes = 1

    def collect(self) -> dict:
        raise RuntimeError("AISStream 使用 workers.AISStreamWorker，不應走 interval scheduler")
