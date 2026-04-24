"""Collector Registry — 集中註冊所有 collector 的 metadata

新增 collector 只需在 COLLECTOR_REGISTRY 加一筆：
    CollectorEntry(MyCollector, "我的 收集器", "MY_COLLECTOR")

Registry 會自動被 main.py / collectors/__init__.py / supabase_writer.py 使用，
不需要手動同步多處。
"""

from dataclasses import dataclass
from typing import Tuple, Type

from .base import BaseCollector
from .air_quality import AirQualityCollector
from .air_quality_imagery import AirQualityImageryCollector
from .air_quality_microsensors import AirQualityMicroSensorCollector
from .bus import BusCollector
from .bus_intercity import BusIntercityCollector
from .cwa_satellite import CWASatelliteCollector
from .earthquake import EarthquakeCollector
from .flight_fr24 import FlightFR24Collector
from .flight_fr24_zone import FlightFR24ZoneCollector
from .flight_opensky import FlightOpenSkyCollector
from .foursquare_poi import FoursquarePOICollector
from .freeway_vd import FreewayVDCollector
from .groundwater_level import GroundwaterLevelCollector
from .iot_wra import IotWraCollector
from .launch import LaunchCollector
from .ncdr_alerts import NCDRAlertsCollector
from .parking import ParkingCollector
from .rail_timetable import RailTimetableCollector
from .rain_gauge_realtime import RainGaugeRealtimeCollector
from .river_water_level import RiverWaterLevelCollector
from .satellite import SatelliteCollector
from .ship_ais import ShipAISCollector
from .ship_tdx import ShipTDXCollector
from .temperature import TemperatureGridCollector
from .tra_static import TRAStaticCollector
from .tra_train import TRATrainCollector
from .vd import VDCollector
from .water_reservoir import WaterReservoirCollector
from .water_reservoir_daily_ops import WaterReservoirDailyOpsCollector
from .weather import WeatherCollector
from .youbike import YouBikeCollector


@dataclass(frozen=True)
class CollectorEntry:
    """單一 collector 的註冊 metadata"""

    cls: Type[BaseCollector]
    display_name: str
    config_prefix: str
    required_env: Tuple[str, ...] = ()


# 順序 = main.py 啟動訊息的顯示順序（沿用重構前的既有順序）
COLLECTOR_REGISTRY: Tuple[CollectorEntry, ...] = (
    CollectorEntry(YouBikeCollector, "YouBike 收集器", "YOUBIKE"),
    CollectorEntry(WeatherCollector, "Weather 收集器", "WEATHER", ("CWA_API_KEY",)),
    CollectorEntry(VDCollector, "VD 收集器", "VD"),
    CollectorEntry(FreewayVDCollector, "Freeway VD 收集器", "FREEWAY_VD"),
    CollectorEntry(TemperatureGridCollector, "Temperature Grid 收集器", "TEMPERATURE", ("CWA_API_KEY",)),
    CollectorEntry(ParkingCollector, "Parking 收集器", "PARKING"),
    CollectorEntry(BusCollector, "Bus 收集器", "BUS"),
    CollectorEntry(BusIntercityCollector, "Bus InterCity 收集器", "BUS_INTERCITY"),
    CollectorEntry(TRATrainCollector, "TRA Train 收集器", "TRA_TRAIN"),
    CollectorEntry(TRAStaticCollector, "TRA Static 收集器", "TRA_STATIC"),
    CollectorEntry(RailTimetableCollector, "Rail Timetable 收集器", "RAIL_TIMETABLE"),
    CollectorEntry(ShipTDXCollector, "Ship TDX 收集器", "SHIP_TDX"),
    CollectorEntry(ShipAISCollector, "Ship AIS 收集器", "SHIP_AIS"),
    CollectorEntry(FlightFR24Collector, "Flight FR24 收集器", "FLIGHT_FR24"),
    CollectorEntry(FlightFR24ZoneCollector, "FR24 Zone 收集器", "FLIGHT_FR24_ZONE"),
    CollectorEntry(EarthquakeCollector, "Earthquake 收集器", "EARTHQUAKE", ("CWA_API_KEY",)),
    CollectorEntry(FlightOpenSkyCollector, "OpenSky 收集器", "FLIGHT_OPENSKY"),
    CollectorEntry(SatelliteCollector, "Satellite 收集器", "SATELLITE"),
    CollectorEntry(LaunchCollector, "Launch 收集器", "LAUNCH"),
    CollectorEntry(CWASatelliteCollector, "CWA Satellite 影像收集器", "CWA_SATELLITE", ("CWA_API_KEY",)),
    CollectorEntry(NCDRAlertsCollector, "NCDR Alerts 收集器", "NCDR_ALERTS"),
    CollectorEntry(FoursquarePOICollector, "Foursquare POI 收集器", "FOURSQUARE_POI", ("HF_TOKEN",)),
    CollectorEntry(AirQualityImageryCollector, "Air Quality Imagery 收集器", "AIR_QUALITY_IMAGERY"),
    CollectorEntry(AirQualityCollector, "Air Quality 觀測收集器", "AIR_QUALITY", ("MOENV_API_KEY",)),
    CollectorEntry(AirQualityMicroSensorCollector, "Air Quality MicroSensors 收集器", "AIR_QUALITY_MICROSENSORS"),
    CollectorEntry(WaterReservoirCollector, "水庫水情收集器", "WATER_RESERVOIR"),
    CollectorEntry(RiverWaterLevelCollector, "河川水位收集器", "RIVER_WATER_LEVEL"),
    CollectorEntry(RainGaugeRealtimeCollector, "即時雨量站收集器", "RAIN_GAUGE_REALTIME", ("CWA_API_KEY",)),
    CollectorEntry(GroundwaterLevelCollector, "地下水水位收集器", "GROUNDWATER_LEVEL"),
    CollectorEntry(WaterReservoirDailyOpsCollector, "水庫每日營運資料收集器", "WATER_RESERVOIR_DAILY_OPS"),
    CollectorEntry(IotWraCollector, "水利署 IoT 水文感測收集器", "IOT_WRA"),
)


def get_entry_by_name(collector_name: str) -> "CollectorEntry | None":
    """依 collector 的 `name` 類屬性查找（例如 'youbike' → YouBike entry）"""
    for entry in COLLECTOR_REGISTRY:
        if entry.cls.name == collector_name:
            return entry
    return None
