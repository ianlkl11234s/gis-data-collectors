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
from .bus_intercity import BusIntercityCollector
from .freeway_vd import FreewayVDCollector
from .earthquake import EarthquakeCollector
from .satellite import SatelliteCollector
from .launch import LaunchCollector
from .ncdr_alerts import NCDRAlertsCollector
from .cwa_satellite import CWASatelliteCollector
from .foursquare_poi import FoursquarePOICollector
from .air_quality_imagery import AirQualityImageryCollector
from .air_quality import AirQualityCollector
from .air_quality_microsensors import AirQualityMicroSensorCollector
from .water_reservoir import WaterReservoirCollector
from .river_water_level import RiverWaterLevelCollector
from .rain_gauge_realtime import RainGaugeRealtimeCollector

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
    'BusIntercityCollector',
    'FreewayVDCollector',
    'EarthquakeCollector',
    'SatelliteCollector',
    'LaunchCollector',
    'NCDRAlertsCollector',
    'CWASatelliteCollector',
    'FoursquarePOICollector',
    'AirQualityImageryCollector',
    'AirQualityCollector',
    'AirQualityMicroSensorCollector',
    'WaterReservoirCollector',
    'RiverWaterLevelCollector',
    'RainGaugeRealtimeCollector',
]
