"""資料收集器模組"""

from .base import BaseCollector
from .youbike import YouBikeCollector
from .weather import WeatherCollector
from .vd import VDCollector
from .temperature import TemperatureGridCollector
from .parking import ParkingCollector
from .tra_train import TRATrainCollector
from .tra_static import TRAStaticCollector
from .ship_tdx import ShipTDXCollector
from .ship_ais import ShipAISCollector

__all__ = [
    'BaseCollector',
    'YouBikeCollector',
    'WeatherCollector',
    'VDCollector',
    'TemperatureGridCollector',
    'ParkingCollector',
    'TRATrainCollector',
    'TRAStaticCollector',
    'ShipTDXCollector',
    'ShipAISCollector',
]
