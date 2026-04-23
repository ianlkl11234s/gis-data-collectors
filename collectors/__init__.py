"""資料收集器模組

所有 collector 透過 registry.COLLECTOR_REGISTRY 統一註冊，
__init__.py 從 registry 自動 re-export 所有 class，讓
`from collectors import YouBikeCollector` 仍維持原本用法。
"""

from .base import BaseCollector
from .registry import COLLECTOR_REGISTRY, CollectorEntry

# 將 registry 裡每個 collector class 暴露到 package 名稱空間
_g = globals()
for _entry in COLLECTOR_REGISTRY:
    _g[_entry.cls.__name__] = _entry.cls

__all__ = [
    'BaseCollector',
    'COLLECTOR_REGISTRY',
    'CollectorEntry',
    *[_entry.cls.__name__ for _entry in COLLECTOR_REGISTRY],
]
