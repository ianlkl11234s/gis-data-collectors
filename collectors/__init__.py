"""資料收集器模組"""

from .base import BaseCollector
from .youbike import YouBikeCollector
from .weather import WeatherCollector
from .vd import VDCollector

__all__ = ['BaseCollector', 'YouBikeCollector', 'WeatherCollector', 'VDCollector']
