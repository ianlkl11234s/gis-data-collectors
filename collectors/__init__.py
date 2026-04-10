"""資料收集器模組"""

from .base import BaseCollector
from .youbike import YouBikeCollector
from .weather import WeatherCollector
from .vd import VDCollector
from .temperature import TemperatureGridCollector
from .parking import ParkingCollector
from .tra_train import TRATrainCollector
from .tra_static import TRAStaticCollector
from .rail_timetable import RailTimetableCollector
from .ship_tdx import ShipTDXCollector
from .ship_ais import ShipAISCollector
from .flight_fr24 import FlightFR24Collector
from .flight_fr24_zone import FlightFR24ZoneCollector
from .flight_opensky import FlightOpenSkyCollector
from .bus import BusCollector
from .freeway_vd import FreewayVDCollector
from .earthquake import EarthquakeCollector
from .satellite import SatelliteCollector
from .launch import LaunchCollector
from .ncdr_alerts import NCDRAlertsCollector
from .cwa_satellite import CWASatelliteCollector
from .foursquare_poi import FoursquarePOICollector

__all__ = [
    'BaseCollector',
    'YouBikeCollector',
    'WeatherCollector',
    'VDCollector',
    'TemperatureGridCollector',
    'ParkingCollector',
    'TRATrainCollector',
    'TRAStaticCollector',
    'RailTimetableCollector',
    'ShipTDXCollector',
    'ShipAISCollector',
    'FlightFR24Collector',
    'FlightFR24ZoneCollector',
    'FlightOpenSkyCollector',
    'BusCollector',
    'FreewayVDCollector',
    'EarthquakeCollector',
    'SatelliteCollector',
    'LaunchCollector',
    'NCDRAlertsCollector',
    'CWASatelliteCollector',
    'FoursquarePOICollector',
]
