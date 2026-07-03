"""Supabase 寫入的表對應設定

把 collector_name → (history_table, current_table, columns) 的對應從
SupabaseWriter class 拆出，讓 writer 專注在寫入邏輯、設定集中管理。

配置欄位：
- history: 歷史（append-only / 分區）表名
- columns: history INSERT 欄位順序
- current: 最新狀態表名（可選）
- current_key: current 表的唯一鍵（配合 UPSERT）
- current_columns: current 欄位順序（可選，省略則同 columns）
- upsert_key: history 表的唯一鍵（ON CONFLICT 使用）
- upsert_strategy: 'do_nothing' 則 ON CONFLICT DO NOTHING
- current_prune_by: 寫完 current 後刪掉該欄位 < 本批次 ts 的 stale rows
- is_reference: True 代表 collector 走自訂的 reference 寫入流程（不用此 map）
- is_multi_table: True 代表 collector 有特殊多表寫入邏輯（由 writer 內 _write_multi_table 處理）
"""


# collector_name → 配置 dict
TABLE_MAP = {
    'youbike': {
        'history': 'realtime.youbike_snapshots',
        'current': 'realtime.youbike_current',
        'current_key': 'station_uid',
        'columns': ['station_uid', 'city', 'available_rent', 'available_return', 'total', 'collected_at'],
    },
    'bus': {
        'history': 'realtime.bus_positions',
        'current': 'realtime.bus_current',
        'current_key': 'plate_numb',
        'columns': ['plate_numb', 'route_uid', 'route_name', 'direction', 'bus_lat', 'bus_lng', 'speed', 'city', 'collected_at'],
    },
    'bus_intercity': {
        'history': 'realtime.bus_intercity_positions',
        'current': 'realtime.bus_intercity_current',
        'current_key': 'plate_numb',
        'columns': ['plate_numb', 'route_uid', 'route_name', 'direction', 'bus_lat', 'bus_lng', 'speed', 'city', 'collected_at'],
    },
    'weather': {
        'history': 'realtime.weather_observations',
        'current': 'realtime.weather_current',
        'current_key': 'station_id',
        'columns': ['station_id', 'station_name', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'rainfall', 'observed_at', 'collected_at', 'geom'],
        'current_columns': ['station_id', 'station_name', 'temperature', 'humidity', 'pressure', 'wind_speed', 'wind_direction', 'rainfall', 'observed_at', 'geom'],
    },
    'temperature': {
        'history': 'realtime.temperature_grids',
        'columns': ['grid_lat', 'grid_lng', 'temperature', 'observed_at', 'collected_at'],
    },
    'tra_train': {
        'history': 'realtime.train_positions',
        'columns': ['train_no', 'train_type', 'station_id', 'delay_minutes', 'collected_at'],
    },
    'tourist_shuttle': {
        'history': 'realtime.tourist_shuttle_positions',
        'current': 'realtime.tourist_shuttle_current',
        'current_key': 'plate_numb',
        'columns': [
            'plate_numb', 'operator_id', 'route_uid', 'sub_route_uid',
            'sub_route_name', 'taiwan_trip_name', 'direction',
            'lat', 'lng', 'speed', 'azimuth', 'gps_time', 'collected_at',
        ],
    },
    'parking': {
        # OnStreet 路邊停車（既有 collector parking.py，221 補 Supabase 寫入）
        'history': 'realtime.parking_segments_availability',
        'current': 'realtime.parking_segments_current',
        'current_key': 'segment_id',
        'columns': [
            'segment_id', 'segment_name', 'city',
            'total_spaces', 'available_spaces', 'occupancy',
            'full_status', 'service_status', 'charge_status',
            'space_types', 'data_collect_time', 'collected_at',
        ],
    },
    'road_congestion': {
        # 省道+市區即時路況統一表（source/city 分流）
        'history': 'realtime.road_sections_live',
        'current': 'realtime.road_sections_current',
        'current_key': 'section_uid',
        'columns': [
            'section_uid', 'section_id', 'source', 'city', 'authority_code',
            'travel_time', 'travel_speed', 'congestion_level', 'congestion_level_id',
            'data_sources', 'data_collect_time', 'collected_at',
        ],
    },
    'parking_offstreet': {
        # OffStreet 路外場館（新 collector parking_offstreet.py，City/SA/Tourism 3 變體）
        'history': 'realtime.parking_lots_availability',
        'current': 'realtime.parking_lots_current',
        'current_key': 'car_park_uid',
        'columns': [
            'car_park_uid', 'car_park_id', 'car_park_name',
            'source_category', 'authority_code', 'sub_category',
            'total_spaces', 'available_spaces',
            'full_status', 'service_status', 'charge_status',
            'space_types', 'data_collect_time', 'collected_at',
        ],
    },
    'ship_ais': {
        'history': 'realtime.ship_positions',
        'current': 'realtime.ship_current',
        'current_key': 'mmsi',
        'columns': ['mmsi', 'ship_name', 'ship_type', 'lat', 'lng', 'speed', 'heading', 'collected_at', 'geom'],
        'current_columns': ['mmsi', 'ship_name', 'ship_type', 'lat', 'lng', 'speed', 'heading', 'collected_at', 'geom'],
    },
    'earthquake': {
        'history': 'realtime.earthquake_events',
        'columns': ['event_id', 'magnitude', 'depth_km', 'epicenter_lat', 'epicenter_lng', 'location_desc', 'occurred_at', 'report_type', 'geom', 'raw_data'],
        'upsert_key': 'event_id',
    },
    'rail_timetable': {
        'is_reference': True,
    },
    'flight_fr24': {
        'is_multi_table': True,  # 特殊處理：trail 寫入 flight_trails
    },
    'flight_fr24_zone': {
        'history': 'realtime.flight_positions',
        'columns': ['flight_id', 'callsign', 'aircraft_type', 'origin', 'destination', 'lat', 'lng', 'altitude', 'speed', 'heading', 'collected_at', 'geom'],
    },
    'flight_opensky': {
        'history': 'realtime.flight_positions',
        'columns': ['flight_id', 'callsign', 'aircraft_type', 'origin', 'destination', 'lat', 'lng', 'altitude', 'speed', 'heading', 'collected_at', 'geom'],
    },
    'freeway_vd': {
        'is_multi_table': True,  # 特殊處理：sections + vd 分兩張表
    },
    'satellite': {
        'history': 'realtime.satellite_positions',
        'current': 'realtime.satellite_current',
        'current_key': 'norad_id',
        'columns': ['norad_id', 'name', 'constellation', 'orbit_type', 'lat', 'lng', 'altitude_km', 'velocity_kms', 'inclination', 'period_min', 'tle_epoch', 'collected_at', 'geom'],
        'current_columns': ['norad_id', 'name', 'constellation', 'orbit_type', 'lat', 'lng', 'altitude_km', 'velocity_kms', 'inclination', 'period_min', 'tle_epoch', 'collected_at', 'geom'],
        # 寫完 current 後把本批次沒更新到的 stale rows 刪掉，維持 current 語意為「最新快照」
        'current_prune_by': 'collected_at',
    },
    'launch': {
        'is_multi_table': True,  # 特殊處理：launches + pads + events 分三張表
    },
    'ncdr_alerts': {
        'history': 'realtime.disaster_alerts',
        'columns': [
            'identifier', 'sender', 'sender_name', 'author', 'category', 'event', 'event_term',
            'urgency', 'severity', 'certainty', 'status', 'msg_type', 'scope',
            'headline', 'description', 'instruction', 'area_desc', 'geocodes',
            'sent', 'effective', 'onset', 'expires', 'cap_url',
            'feed_title', 'feed_summary', 'geom', 'collected_at',
        ],
        'upsert_key': 'identifier',
    },
    'cwa_satellite': {
        'history': 'realtime.cwa_imagery_frames',
        'columns': [
            'dataset_id', 'observed_at', 'image_bytes', 'mime_type',
            'lon_min', 'lon_max', 'lat_min', 'lat_max',
            'width', 'height', 'image_size', 'product_url', 'resource_desc',
            'image_key', 'collected_at',
        ],
        # 複合 PK (dataset_id, observed_at)：用 ON CONFLICT DO NOTHING
        # 因為同一觀測時間的 PNG 內容固定，重複抓不需更新
        'upsert_key': 'dataset_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'foursquare_poi': {
        'history': 'reference.foursquare_poi',
        'columns': [
            'fsq_place_id', 'name', 'category', 'subcategory',
            'city', 'district', 'address', 'geom',
            'tel', 'website', 'fsq_category_ids',
            'date_refreshed', 'date_closed', 'properties', 'imported_at',
        ],
        'upsert_key': 'fsq_place_id',
    },
    'air_quality_imagery': {
        'history': 'realtime.aqi_imagery_frames',
        'columns': [
            'product_type', 'observed_at', 'image_bytes', 'mime_type',
            'image_size', 'product_url', 'collected_at',
        ],
        # PK (product_type, observed_at)：同小時同產品重複抓不寫入
        'upsert_key': 'product_type,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'air_quality': {
        'history': 'realtime.air_quality_observations',
        'current': 'realtime.air_quality_current',
        'current_key': 'station_id',
        'columns': [
            'station_id', 'station_name', 'county', 'aqi', 'pollutant', 'status',
            'pm25', 'pm10', 'o3', 'o3_8hr', 'no2', 'so2', 'co', 'co_8hr',
            'nox', 'no', 'pm25_avg', 'pm10_avg', 'so2_avg',
            'wind_speed', 'wind_direction',
            'observed_at', 'collected_at', 'geom',
        ],
        'current_columns': [
            'station_id', 'station_name', 'county', 'aqi', 'pollutant', 'status',
            'pm25', 'pm10', 'o3', 'o3_8hr', 'no2', 'so2', 'co', 'co_8hr',
            'nox', 'no', 'pm25_avg', 'pm10_avg', 'so2_avg',
            'wind_speed', 'wind_direction',
            'observed_at', 'geom',
        ],
    },
    'air_quality_microsensors': {
        'history': 'realtime.micro_sensor_readings',
        'columns': [
            'device_id', 'source', 'site_name', 'area', 'app',
            'pm25', 'pm10', 'pm1', 'temperature', 'humidity',
            'observed_at', 'collected_at', 'geom',
        ],
    },
    'water_reservoir': {
        'history': 'realtime.reservoir_status',
        'columns': [
            'reservoir_id', 'snapshot_at',
            'water_level_m', 'effective_storage_wan_m3',
            'inflow_cms', 'total_outflow_cms', 'spillway_outflow_cms',
            'basin_rainfall_mm', 'hourly_rainfall_mm',
            'status_type', 'collected_at',
        ],
        'upsert_key': 'reservoir_id,snapshot_at',
        'upsert_strategy': 'do_nothing',
    },
    'river_water_level': {
        'history': 'realtime.river_water_level',
        'columns': ['station_id', 'observed_at', 'water_level_m', 'check_result', 'collected_at'],
        'upsert_key': 'station_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'rain_gauge_realtime': {
        'history': 'realtime.rain_gauge_readings',
        'columns': [
            'station_id', 'station_name', 'county', 'town', 'lat', 'lng',
            'precipitation_10min', 'precipitation_1hr', 'precipitation_3hr',
            'precipitation_6hr', 'precipitation_12hr', 'precipitation_24hr',
            'observed_at', 'collected_at', 'geom',
        ],
        'upsert_key': 'station_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'npa_traffic_accident_a1': {
        'history': 'realtime.traffic_accidents_a1',
        'columns': [
            'accident_class', 'occurred_at', 'agency', 'location',
            'lat', 'lon', 'weather', 'light', 'road_type', 'speed_limit',
            'accident_type_major', 'accident_type_sub',
            'cause_main_major', 'cause_main_sub',
            'death_injury', 'party_order', 'party_type_major', 'party_type_sub',
            'party_gender', 'party_age', 'is_hit_and_run',
            'dedup_hash', 'geom', 'collected_at',
        ],
        'upsert_key': 'dedup_hash',
        'upsert_strategy': 'do_nothing',
    },
    'immigration_apis_airport': {
        'history': 'realtime.border_airport_snapshot',
        'columns': [
            'airport', 'terminal', 'in_out', 'in_out_code',
            'gender', 'nationality', 'age_band', 'pax_count',
            'endpoint_code', 'collected_at',
        ],
        # append-only：每次 snapshot 全寫進去（無 upsert key）
    },
    'correctional_daily_snapshot': {
        'history': 'realtime.prison_population_daily',
        'columns': [
            'observed_date', 'total_inmates', 'male_inmates', 'female_inmates',
            'approved_capacity', 'over_capacity_pct', 'new_in_count', 'new_out_count',
            'collected_at',
        ],
        'upsert_key': 'observed_date',
        'upsert_strategy': 'update',  # 同日多次抓取覆寫，最新值勝
    },
    'er_hospital_realtime': {
        'history': 'realtime.er_hospital_status',
        'current': 'realtime.er_hospital_current',
        'current_key': 'hosp_id',
        'columns': [
            'hosp_id', 'hosp_name', 'area_no', 'area_name', 'cont_type', 'level_name',
            'inform', 'wait_see_cnt', 'wait_bed_cnt', 'wait_general_cnt', 'wait_icu_cnt',
            'source_url', 'observed_at', 'collected_at',
        ],
        'current_columns': [
            'hosp_id', 'hosp_name', 'area_no', 'area_name', 'cont_type', 'level_name',
            'inform', 'wait_see_cnt', 'wait_bed_cnt', 'wait_general_cnt', 'wait_icu_cnt',
            'source_url', 'observed_at',
        ],
        'upsert_key': 'hosp_id,observed_at',
        'upsert_strategy': 'do_nothing',
        'current_touch_updated_at': True,
    },
    'groundwater_level': {
        'history': 'realtime.groundwater_level_readings',
        'columns': [
            'station_id', 'well_name', 'agency_unit',
            'water_level_m', 'voltage',
            'observed_at', 'collected_at',
        ],
        'upsert_key': 'station_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'water_reservoir_daily_ops': {
        'history': 'realtime.reservoir_daily_ops',
        'columns': [
            'reservoir_id', 'reservoir_name', 'observed_at',
            'effective_capacity_wan', 'dead_water_level_m', 'normal_water_level_max',
            'basin_rainfall_mm', 'inflow_wan_m3', 'crossflow_wan_m3',
            'outflow_discharge_wan', 'outflow_total_wan',
            'regulatory_discharge_wan', 'outflow_wan',
            'collected_at',
        ],
        'upsert_key': 'reservoir_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'wra_drought_alert': {
        # history: ON CONFLICT (region_name, published_date) DO NOTHING（同公告日去重）
        # current: UPSERT by region_name（每縣市最新狀態）
        'history': 'public.drought_alert_history',
        'current': 'public.drought_alert_current',
        'current_key': 'region_name',
        'columns': [
            'region_name', 'alert_level', 'alert_label', 'alert_color',
            'published_date', 'source_hash', 'source_url', 'fetched_at',
        ],
        'upsert_key': 'region_name,published_date',
        'upsert_strategy': 'do_nothing',
    },
    'iot_wra': {
        'history': 'realtime.iot_wra_measurements',
        'columns': [
            'iow_station_id', 'physical_quantity_id', 'station_type',
            'observed_at', 'name', 'full_name', 'si_unit', 'value',
            'collected_at',
        ],
        'upsert_key': 'iow_station_id,physical_quantity_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'uswg': {
        'history': 'realtime.uswg_measurements',
        'columns': [
            'iow_station_id', 'physical_quantity_id', 'observed_at',
            'name', 'si_unit', 'value', 'collected_at',
        ],
        'upsert_key': 'iow_station_id,physical_quantity_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'wic_sewer': {
        'history': 'realtime.taipei_sewer_measurements',
        'columns': [
            'station_no', 'observed_at', 'level_out', 'ground_far', 'voltage', 'collected_at',
        ],
        'upsert_key': 'station_no,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'wic_evacuate': {
        'history': 'realtime.taipei_evacuate_status',
        'columns': [
            'station_no', 'observed_at',
            'fo01', 'fc01', 'flt01', 'fo02', 'fc02', 'flt02', 'collected_at',
        ],
        'upsert_key': 'station_no,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'wic_pumb': {
        'history': 'realtime.taipei_pumb_status',
        'columns': [
            'stn_id', 'observed_at', 'inner_value', 'outer_value',
            'pumb_status', 'door_status', 'collected_at',
        ],
        'upsert_key': 'stn_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'precipitation_raster': {
        'history': 'realtime.precipitation_raster_frames',
        'columns': [
            'cumulative_hours', 'observed_at', 'image_bytes', 'mime_type',
            'image_size', 'ul_lat', 'ul_lng', 'br_lat', 'br_lng',
            'width_m', 'height_m', 'is_empty', 'source_url', 'collected_at',
        ],
        'upsert_key': 'cumulative_hours,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'waste_positions': {
        # 表 schema 見 gis-platform/migrations/065_waste_management.sql §5.5
        # 純 append-only history（無 UNIQUE constraint）；前端用
        # DISTINCT ON (vehicle_no) ORDER BY observed_at DESC 取最新位置
        'history': 'spatial.waste_positions_realtime',
        'columns': [
            'city', 'vehicle_no', 'route_id', 'status',
            'geometry', 'observed_at', 'source_url',
        ],
    },
    'road_event_live': {
        # 表 schema 見 gis-platform/migrations/078_road_events.sql
        # 同表 history append + current upsert (PK: event_id, source)
        # 走 is_multi_table 自訂寫入（既有 current_key 不支援複合 PK）
        'is_multi_table': True,
    },
    'road_event_planned': {
        'is_multi_table': True,
    },
    'news_events': {
        # 表 schema 見 gis-platform/migrations/162（realtime.news_events）+ 164（v2 三維度）
        # geom 不由 collector 提供：DB trigger 由 admin_code 查 township centroid
        # url_norm 有 UNIQUE constraint → ON CONFLICT DO NOTHING（重複抓不更新）
        'history': 'realtime.news_events',
        'columns': [
            'source', 'url', 'url_norm', 'title', 'summary', 'category',
            'location_name', 'county', 'admin_code',
            'published_ts', 'confidence', 'title_simhash',
            # v2（2026-06-13）：LLM 評估的 GIS 相關性 / 嚴重度 / 是否為事件
            'gis_relevance', 'severity', 'is_event',
        ],
        'upsert_key': 'url_norm',
        'upsert_strategy': 'do_nothing',
    },
    'power_taipower': {
        # 台電即時電力供需：單一 collector 寫 3 張表
        #   realtime.power_system_status   UNIQUE(observed_at)             DO NOTHING
        #   realtime.power_generation_unit UNIQUE(unit_name, observed_at)  DO NOTHING
        #   realtime.power_region_demand   UNIQUE(region, observed_at)     DO NOTHING
        # 表 schema 見 gis-platform/migrations/145_power_taipower_realtime.sql
        'is_multi_table': True,
    },
    'global_climate_usgs_earthquake': {
        # USGS 全球地震 hourly feed — gis-platform migration 261
        # UNIQUE(event_id) + UNIQUE(dedup_hash) 雙保險，ON CONFLICT DO NOTHING
        'history': 'realtime.earthquakes_global',
        'columns': [
            'event_id', 'mag', 'place', 'observed_at', 'depth_km',
            'raw_json', 'dedup_hash', 'geom', 'collected_at',
        ],
        'upsert_key': 'event_id',
        'upsert_strategy': 'do_nothing',
    },
    'global_climate_jma_typhoon': {
        # JMA RSMC Tokyo 颱風 time-point decomposed — gis-platform migration 261
        # UNIQUE(storm_id, source, valid_at, point_type, advisory_number) DO NOTHING
        'history': 'realtime.typhoon_positions',
        'columns': [
            'storm_id', 'source', 'valid_at', 'point_type', 'advisory_number',
            'advisory_issued_at', 'name_local', 'name_en',
            'center_lat', 'center_lon', 'center_pressure_hpa', 'max_wind_kt',
            'gale_radius_km', 'storm_radius_km', 'geom', 'raw_json', 'collected_at',
        ],
        'upsert_key': 'storm_id,source,valid_at,point_type,advisory_number',
        'upsert_strategy': 'do_nothing',
    },
    'global_climate_jtwc': {
        # JTWC ATCF 颱風 time-point decomposed — gis-platform migration 261
        # 跟 jma 共表，靠 source='jtwc' 區分
        'history': 'realtime.typhoon_positions',
        'columns': [
            'storm_id', 'source', 'valid_at', 'point_type', 'advisory_number',
            'advisory_issued_at', 'name_local', 'name_en',
            'center_lat', 'center_lon', 'center_pressure_hpa', 'max_wind_kt',
            'gale_radius_km', 'storm_radius_km', 'geom', 'raw_json', 'collected_at',
        ],
        'upsert_key': 'storm_id,source,valid_at,point_type,advisory_number',
        'upsert_strategy': 'do_nothing',
    },
    'global_climate_cmems': {
        # CMEMS NetCDF digest — gis-platform migration 261
        # 每 time slice 一筆 row
        'history': 'realtime.global_climate_grids',
        'columns': [
            'dataset_id', 'observed_at', 'init_at', 'leadtime_hr',
            'bbox', 'digest', 's3_uri', 'pmtiles_uri', 'raw_size_bytes', 'collected_at',
        ],
        'upsert_key': 'dataset_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'global_climate_cams': {
        'history': 'realtime.global_climate_grids',
        'columns': [
            'dataset_id', 'observed_at', 'init_at', 'leadtime_hr',
            'bbox', 'digest', 's3_uri', 'pmtiles_uri', 'raw_size_bytes', 'collected_at',
        ],
        'upsert_key': 'dataset_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'global_climate_noaa_gfs': {
        'history': 'realtime.global_climate_grids',
        'columns': [
            'dataset_id', 'observed_at', 'init_at', 'leadtime_hr',
            'bbox', 'digest', 's3_uri', 'pmtiles_uri', 'raw_size_bytes', 'collected_at',
        ],
        'upsert_key': 'dataset_id,observed_at',
        'upsert_strategy': 'do_nothing',
    },
    'lightning_events': {
        # 台電落雷 nid 61139（snapshot 1 分鐘覆寫，collector 5 分 cron 去重累積）
        # 表 schema 見 gis-platform/migrations/183_realtime_lightning_events.sql
        # UNIQUE(event_id) + UNIQUE(dedup_hash) 雙保險，ON CONFLICT DO NOTHING
        'history': 'realtime.lightning_events',
        'columns': [
            'event_id', 'strike_time', 'lon', 'lat',
            'intensity_ka', 'strike_type', 'dedup_hash',
            'geom', 'observed_at', 'collected_at',
        ],
        'upsert_key': 'event_id',
        'upsert_strategy': 'do_nothing',
    },
    'nuclear_radiation': {
        # 核設施環境輻射劑量 nid 42326（51 站，15 分 cron）
        # 表 schema 見 gis-platform/migrations/184_realtime_nuclear_radiation.sql
        # history: realtime.nuclear_radiation_measurements UNIQUE(station_id, observed_at) DO NOTHING
        # current: realtime.nuclear_radiation_stations PK=station_id UPSERT
        'history': 'realtime.nuclear_radiation_measurements',
        'current': 'realtime.nuclear_radiation_stations',
        'current_key': 'station_id',
        'columns': [
            'station_id', 'station_name', 'dose_usvh', 'observed_at',
            'lon', 'lat', 'is_stale', 'geom', 'collected_at',
        ],
        'current_columns': [
            'station_id', 'station_name', 'dose_usvh', 'observed_at',
            'lon', 'lat', 'is_stale', 'geom',
        ],
        'upsert_key': 'station_id,observed_at',
        'upsert_strategy': 'do_nothing',
        'current_touch_updated_at': True,
    },
    'twse_market_index': {
        # TWSE 加權指數 ticker — gis-platform migration 204
        # history: realtime.market_index_tick UNIQUE(index_code, observed_at) DO NOTHING
        # current: realtime.market_index_current PK=index_code UPSERT
        'history': 'realtime.market_index_tick',
        'current': 'realtime.market_index_current',
        'current_key': 'index_code',
        'columns': [
            'index_code', 'index_name', 'current_value', 'prev_close',
            'open_value', 'high_value', 'low_value',
            'volume_lots', 'value_thousands', 'is_market_open',
            'observed_at', 'collected_at',
        ],
        'current_columns': [
            'index_code', 'index_name', 'current_value', 'prev_close',
            'open_value', 'high_value', 'low_value',
            'volume_lots', 'value_thousands', 'is_market_open',
            'observed_at',
        ],
        'upsert_key': 'index_code,observed_at',
        'upsert_strategy': 'do_nothing',
        'current_touch_updated_at': True,
    },
    'pla_activity_daily': {
        # 共機每日通報 — gis-platform migration 205
        # 單表，PK = report_date，每日重抓同一日要 UPSERT（內容可能修正）
        'history': 'realtime.pla_activity_daily',
        'columns': [
            'report_date', 'aircraft_sorties', 'crossed_median_line_cnt',
            'plan_vessels', 'official_ships',
            'adiz_north', 'adiz_central', 'adiz_southwestern', 'adiz_eastern',
            'raw_text', 'source_lang', 'source_url', 'collected_at',
        ],
        'upsert_key': 'report_date',
        'upsert_strategy': 'update',
    },
    'yt_live_video_resolver': {
        # YouTube 14 家新聞台直播 videoId 解析 — gis-platform migration 209
        # history: realtime.yt_live_history UNIQUE(handle, video_id, observed_at) DO NOTHING
        # current: realtime.yt_live_current PK=handle UPSERT
        'history': 'realtime.yt_live_history',
        'current': 'realtime.yt_live_current',
        'current_key': 'handle',
        'columns': [
            'handle', 'channel_id', 'video_id', 'title',
            'is_live', 'view_count', 'last_error',
            'observed_at', 'collected_at',
        ],
        'current_columns': [
            'handle', 'channel_id', 'video_id', 'title',
            'is_live', 'view_count', 'last_error', 'observed_at',
        ],
        'upsert_key': 'handle,video_id,observed_at',
        'upsert_strategy': 'do_nothing',
        'current_touch_updated_at': True,
    },
    'cdc_public_health_weekly': {
        # CDC 公衛週報 — gis-platform migration 206
        # UNIQUE(disease_code, iso_year, iso_week, county_code, township_code, age_group, gender, is_imported)
        # 同週重抓 DO NOTHING（CDC 確認後不會回修）
        'history': 'realtime.public_health_weekly',
        'columns': [
            'disease_code', 'iso_year', 'iso_week',
            'county_code', 'county_name', 'township_code', 'township_name',
            'age_group', 'gender', 'is_imported',
            'metric_value', 'source_dataset', 'collected_at',
        ],
        'upsert_key': 'disease_code,iso_year,iso_week,county_code,township_code,age_group,gender,is_imported',
        'upsert_strategy': 'do_nothing',
    },
}
