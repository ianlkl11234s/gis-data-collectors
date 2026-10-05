# 全 manifest 唯讀篩查

查詢快照 2026-10-05 12:43–12:52 Asia/Taipei。OK 只代表指定時間欄位通過 3x 判定，不代表 source completeness/quality。NEVER 不等於停跑；DEFER_HEAVY 是白天禁止重掃下的未驗證，不是健康。詳細 SQL/result 見 scan2.json、small-remaining.json、remaining-cost.json。

|表|owner|latest UTC|判定|
|---|---|---|---|
|intel.global_event_candidate_observations|global_events|2026-10-05 04:27:17.969868+00|OK|
|intel.global_event_collector_run_receipts|global_events|2026-10-05 04:26:57.819591+00|OK|
|live.youbike_snapshots|youbike|—|DEFER_HEAVY|
|live.youbike_current|youbike|2026-10-05 04:37:50.246849+00|OK|
|live.bus_positions|bus|2026-10-05 04:45:02.425164+00|OK|
|live.bus_current|bus|2026-10-05 04:45:02.425164+00|OK|
|live.bus_intercity_positions|bus_intercity|2026-10-05 04:46:11.444856+00|OK|
|live.bus_intercity_current|bus_intercity|2026-10-05 04:46:11.444856+00|OK|
|live.freeway_sections|freeway_vd|2026-10-05 04:38:57.281207+00|OK|
|live.freeway_sections_current|freeway_vd|2026-10-05 04:38:57.281207+00|OK|
|live.freeway_vd_traffic|freeway_vd|—|DEFER_HEAVY|
|live.train_positions|tra_train|2026-10-05 04:46:29.450208+00|OK|
|live.ship_positions|ship_ais|2026-10-05 04:40:01.622915+00|OK|
|live.ship_current|ship_ais|—|DEFER_HEAVY|
|reference.marine_observation_stations|cwa_marine_observation|2026-10-05 04:42:37.354609+00|OK|
|live.marine_observation_readings|cwa_marine_observation|2026-10-05 04:40:00+00|OK|
|live.marine_observation_current|cwa_marine_observation|2026-10-05 04:42:37.354609+00|OK|
|live.marine_observation_quarantine|cwa_marine_observation|2026-10-05 04:42:37.354609+00|OK|
|live.aisstream_ingest_runs|aisstream|2026-10-02 15:24:05.706372+00|OK|
|live.aisstream_archive_manifests|aisstream|—|CONFIG_ERROR|
|live.aisstream_position_observations|aisstream|—|DEFER_HEAVY|
|live.aisstream_vessel_current|aisstream|2026-10-05 04:46:43.929174+00|OK|
|live.aisstream_ingest_health|aisstream|2026-10-05 04:46:44.612252+00|OK|
|live.gfw_vessel_presence_runs|gfw_vessel_presence|—|NEVER|
|live.gfw_vessel_presence_archive_manifests|gfw_vessel_presence|—|NEVER|
|live.gfw_vessel_presence_snapshots|gfw_vessel_presence|—|NEVER|
|live.gfw_vessel_presence_current|gfw_vessel_presence|—|NEVER|
|reference.daily_schedules|rail_timetable|2026-10-04 15:24:59.652575+00|OK|
|live.parking_segments_current|parking|2026-10-05 04:42:07.34467+00|OK|
|live.flight_positions|flight_fr24|2026-10-05 04:46:24.450454+00|OK|
|air_tickets.fare_offers|air_ticket_radar|2026-09-08 15:06:09.749321+00|DEAD|
|live.weather_observations|weather|—|DEFER_HEAVY|
|live.weather_current|weather|2026-10-05 04:00:00+00|OK|
|live.temperature_grids|temperature|—|DEFER_HEAVY|
|live.cwa_imagery_frames|cwa_satellite|2026-10-05 04:20:00+00|OK|
|live.earthquake_events|earthquake|2026-10-04 21:26:10+00|OK|
|live.earthquake_station_obs|earthquake|2026-10-04 21:43:34.921707+00|OK|
|live.tsunami_alerts|earthquake|2026-07-29 06:04:51.933644+00|OK|
|live.earthquake_town_intensity|earthquake_town_intensity|2026-09-30 05:10:48.887277+00|OK|
|live.earthquake_shakemap_grid|earthquake_shakemap_grid|2026-08-26 22:11:10.606054+00|STALE|
|live.earthquake_moment_tensor|earthquake_moment_tensor|2026-09-30 06:08:57.17201+00|OK|
|live.disaster_alerts|ncdr_alerts|2026-10-05 04:44:35.417053+00|OK|
|live.internet_health_source_runs|cloudflare_radar|—|DEFER_HEAVY|
|live.internet_health_observations|cloudflare_radar|—|DEFER_HEAVY|
|live.internet_health_current|cloudflare_radar|2026-10-05 04:45:00+00|OK|
|live.internet_health_incidents|cloudflare_radar|—|NEVER|
|live.satellite_positions|satellite|—|DEFER_HEAVY|
|live.satellite_current|satellite|2026-10-05 03:28:59.477303+00|OK|
|live.satellite_tle|satellite|—|DEFER_HEAVY|
|live.satellite_tle_history|satellite|2026-10-05 03:28:59.477303+00|OK|
|live.launches|launch|2026-10-05 04:43:13.394575+00|OK|
|live.launch_events|launch|2026-10-05 04:28:13.054604+00|OK|
|live.launch_pads|launch|2026-10-02 17:56:22.969801+00|OK|
|live.air_quality_observations|air_quality|—|DEFER_HEAVY|
|live.air_quality_current|air_quality|2026-10-05 04:00:00+00|OK|
|live.aqi_imagery_frames|air_quality_imagery|2026-10-05 04:12:26+00|OK|
|live.micro_sensor_readings|air_quality_microsensors|—|DEFER_HEAVY|
|live.reservoir_status|water_reservoir|2026-10-05 04:00:00+00|OK|
|live.reservoir_daily_ops|water_reservoir_daily_ops|2026-10-03 00:00:00+00|OK|
|live.river_water_level|river_water_level|2026-10-05 04:20:00+00|OK|
|live.rain_gauge_readings|rain_gauge_realtime|2026-10-05 04:30:00+00|OK|
|live.groundwater_level_readings|groundwater_level|2026-10-05 04:20:00+00|OK|
|live.iot_wra_measurements|iot_wra|2026-10-05 04:46:54+00|OK|
|live.uswg_measurements|uswg|2026-10-05 04:30:27.968+00|OK|
|public.drought_alert_current|wra_drought_alert|2026-05-15 09:30:52.860582+00|STALE|
|live.road_events|road_event_live|2026-10-05 04:39:26.289099+00|OK|
|live.road_events_current|road_event_live|2026-10-05 04:39:26.289099+00|OK|
|spatial.waste_positions_realtime|waste_positions|2026-10-05 04:44:59+00|OK|
|live.er_hospital_status|er_hospital_realtime|2026-10-05 04:45:00+00|OK|
|live.er_hospital_current|er_hospital_realtime|2026-10-05 04:45:00+00|OK|
|live.power_system_status|power_taipower|2026-10-05 04:30:00+00|OK|
|live.power_generation_unit|power_taipower|2026-10-05 04:30:00+00|OK|
|live.power_region_demand|power_taipower|2026-10-05 04:30:00+00|OK|
|live.news_events|news_events|2026-10-05 04:38:08.188951+00|OK|
|live.public_health_weekly|cdc_public_health_weekly|2026-10-01 03:00:06.758194+00|OK|
|live.road_sections_current|road_congestion|2026-10-05 04:43:18.395973+00|OK|
|live.parking_lots_current|parking_offstreet|2026-10-05 04:44:40.418629+00|OK|
|live.tourist_shuttle_current|tourist_shuttle|2026-10-05 04:46:21.447617+00|OK|
|live.nuclear_radiation_stations|nuclear_radiation|2026-10-05 04:41:57.81311+00|OK|
|live.taipei_sewer_measurements|wic_sewer|2026-10-05 04:40:00+00|OK|
|live.taipei_evacuate_status|wic_evacuate|2026-10-05 04:30:00+00|OK|
|live.taipei_pumb_status|wic_pumb|2026-10-05 04:36:00+00|OK|
|live.pla_activity_daily|pla_activity_daily|2026-10-05 04:27:53.048304+00|OK|
|live.food_price_daily|food_prices|—|DEFER_HEAVY|
|spatial.pla_tracks_runs|pla_tracks_vectorize|2026-10-03 15:27:05.013214+00|OK|
|live.yt_live_current|yt_live_video_resolver|2026-10-05 04:45:08.617386+00|OK|
|live.precipitation_raster_frames|precipitation_raster|2026-10-05 03:28:48.431801+00|OK|
|live.earthquakes_global|global_climate_usgs_earthquake|2026-10-05 04:29:18.08305+00|OK|
|live.typhoon_positions|global_climate_jma_typhoon|—|DEFER_HEAVY|
|live.global_climate_grids|global_climate_cmems|2026-10-04 15:27:26.388352+00|OK|
|live.border_airport_snapshot|immigration_apis_airport|2026-10-05 04:07:01.548307+00|OK|
|live.lightning_events|lightning_events|2026-10-05 04:27:50.051429+00|OK|
|live.market_index_current|twse_market_index|2026-10-05 04:45:57.69022+00|OK|
|live.prison_population_daily|correctional_daily_snapshot|2026-10-04 15:27:07.353079+00|OK|
|live.animal_adoption_snapshot_runs|animal_adoption|2026-10-04 00:00:00+00|OK|
|live.animal_adoption_snapshots|animal_adoption|2026-10-04 00:00:00+00|OK|
|live.animal_adoption_current|animal_adoption|2026-10-04 15:26:45.668418+00|OK|
|analytics.animal_adoption_daily|animal_adoption|2026-10-04 00:00:00+00|OK|
|live.animal_shelter_outcome_runs|animal_shelter_outcomes|2026-10-02 00:00:00+00|OK|
|live.animal_shelter_outcomes|animal_shelter_outcomes|2026-10-02 00:00:00+00|OK|
|live.animal_welfare_point_runs|animal_veterinary_clinics|2026-10-02 00:00:00+00|OK|
|live.animal_welfare_point_snapshots|animal_protection_offices|2026-10-02 00:00:00+00|OK|
|live.traffic_accidents_a1|npa_traffic_accident_a1|2026-06-27 16:20:24.90393+00|DEAD|
|live.tpml_seat_status|tpml_seat|2026-10-05 04:38:37.275202+00|OK|
|live.tpml_seat_current|tpml_seat|2026-10-05 04:38:37.275202+00|OK|
|live.nusc_gamma_stations|nusc_gamma_radiation|2026-10-05 04:44:25.697333+00|OK|
|live.water_effluent_current|water_effluent_monitoring|2026-10-05 04:29:53.471489+00|OK|
|live.cems_stack_current|cems_stack_monitoring|2026-10-05 04:26:04.949744+00|OK|
|live.cwa_uv_daily|cwa_uv_daily|2026-10-05 03:27:03.327116+00|OK|
