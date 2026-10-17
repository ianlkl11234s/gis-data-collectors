# S3 lifecycle 備份：2026-10-17 新增 G02 颱風兩條規則之前

bucket `migu-gis-data-collector`，`get-bucket-lifecycle-configuration` 原樣輸出，共 94 條（2026-10-17 19:59Z 前後讀取）。
之後 append `archive-deep-270d:global_climate_cwa_typhoon`、`archive-deep-270d:global_climate_ncdr_typhoon_routes` 兩條（Prefix `{name}/archives/`，270 天 → DEEP_ARCHIVE），共 96 條；put 後重讀比對：既有 94 條內容 0 變動。說明見 `docs/AWS_INVENTORY.md` §lifecycle。

要還原：把下方 JSON 存檔後 `aws s3api put-bucket-lifecycle-configuration --bucket migu-gis-data-collector --lifecycle-configuration file://<檔>`（整份覆寫）。

```json
{
 "Rules": [
  {
   "ID": "tiered-cold-storage",
   "Filter": {
    "Prefix": ""
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 30,
     "StorageClass": "STANDARD_IA"
    },
    {
     "Days": 90,
     "StorageClass": "GLACIER_IR"
    }
   ],
   "AbortIncompleteMultipartUpload": {
    "DaysAfterInitiation": 7
   }
  },
  {
   "ID": "supabase-snapshots-realtime-tiering",
   "Filter": {
    "Prefix": "supabase-snapshots/realtime/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 30,
     "StorageClass": "STANDARD_IA"
    },
    {
     "Days": 90,
     "StorageClass": "GLACIER_IR"
    },
    {
     "Days": 180,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:air_quality",
   "Filter": {
    "Prefix": "air_quality/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:air_quality_imagery",
   "Filter": {
    "Prefix": "air_quality_imagery/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:air_quality_microsensors",
   "Filter": {
    "Prefix": "air_quality_microsensors/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:animal_adoption",
   "Filter": {
    "Prefix": "animal_adoption/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:animal_licensed_pet_businesses",
   "Filter": {
    "Prefix": "animal_licensed_pet_businesses/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:animal_protection_offices",
   "Filter": {
    "Prefix": "animal_protection_offices/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:animal_shelter_outcomes",
   "Filter": {
    "Prefix": "animal_shelter_outcomes/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:animal_shelter_pressure",
   "Filter": {
    "Prefix": "animal_shelter_pressure/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:animal_veterinary_clinics",
   "Filter": {
    "Prefix": "animal_veterinary_clinics/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:bus",
   "Filter": {
    "Prefix": "bus/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:bus_intercity",
   "Filter": {
    "Prefix": "bus_intercity/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:cdc_public_health_weekly",
   "Filter": {
    "Prefix": "cdc_public_health_weekly/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:cems_stack_monitoring",
   "Filter": {
    "Prefix": "cems_stack_monitoring/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:cloudflare_radar",
   "Filter": {
    "Prefix": "cloudflare_radar/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:correctional_daily_snapshot",
   "Filter": {
    "Prefix": "correctional_daily_snapshot/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:cwa_marine_observation",
   "Filter": {
    "Prefix": "cwa_marine_observation/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:cwa_satellite",
   "Filter": {
    "Prefix": "cwa_satellite/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:cwa_uv_daily",
   "Filter": {
    "Prefix": "cwa_uv_daily/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:earthquake",
   "Filter": {
    "Prefix": "earthquake/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:earthquake_catalog",
   "Filter": {
    "Prefix": "earthquake_catalog/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:earthquake_moment_tensor",
   "Filter": {
    "Prefix": "earthquake_moment_tensor/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:earthquake_shakemap_grid",
   "Filter": {
    "Prefix": "earthquake_shakemap_grid/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:earthquake_town_intensity",
   "Filter": {
    "Prefix": "earthquake_town_intensity/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:er_hospital_realtime",
   "Filter": {
    "Prefix": "er_hospital_realtime/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:flight_fr24",
   "Filter": {
    "Prefix": "flight_fr24/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:flight_fr24_zone",
   "Filter": {
    "Prefix": "flight_fr24_zone/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:flight_opensky",
   "Filter": {
    "Prefix": "flight_opensky/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:food_prices",
   "Filter": {
    "Prefix": "food_prices/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:freeway_vd",
   "Filter": {
    "Prefix": "freeway_vd/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:global_climate_cams",
   "Filter": {
    "Prefix": "global_climate_cams/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:global_climate_cmems",
   "Filter": {
    "Prefix": "global_climate_cmems/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:global_climate_jma_typhoon",
   "Filter": {
    "Prefix": "global_climate_jma_typhoon/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:global_climate_jtwc",
   "Filter": {
    "Prefix": "global_climate_jtwc/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:global_climate_noaa_gfs",
   "Filter": {
    "Prefix": "global_climate_noaa_gfs/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:global_climate_usgs_earthquake",
   "Filter": {
    "Prefix": "global_climate_usgs_earthquake/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:global_events",
   "Filter": {
    "Prefix": "global_events/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:groundwater_level",
   "Filter": {
    "Prefix": "groundwater_level/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:immigration_apis_airport",
   "Filter": {
    "Prefix": "immigration_apis_airport/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:ioda_internet_health",
   "Filter": {
    "Prefix": "ioda_internet_health/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:iot_wra",
   "Filter": {
    "Prefix": "iot_wra/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:isohe_port_marine",
   "Filter": {
    "Prefix": "isohe_port_marine/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_amedas",
   "Filter": {
    "Prefix": "jma_amedas/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_quake",
   "Filter": {
    "Prefix": "jma_quake/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_raster",
   "Filter": {
    "Prefix": "jma_raster/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_warnings",
   "Filter": {
    "Prefix": "jma_warnings/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:launch",
   "Filter": {
    "Prefix": "launch/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:lightning_cwa",
   "Filter": {
    "Prefix": "lightning_cwa/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:lightning_events",
   "Filter": {
    "Prefix": "lightning_events/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:ncdr_alerts",
   "Filter": {
    "Prefix": "ncdr_alerts/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:news_events",
   "Filter": {
    "Prefix": "news_events/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:npa_traffic_accident_a1",
   "Filter": {
    "Prefix": "npa_traffic_accident_a1/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:nuclear_radiation",
   "Filter": {
    "Prefix": "nuclear_radiation/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:nusc_gamma_radiation",
   "Filter": {
    "Prefix": "nusc_gamma_radiation/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:parking",
   "Filter": {
    "Prefix": "parking/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:parking_offstreet",
   "Filter": {
    "Prefix": "parking_offstreet/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:pla_activity_daily",
   "Filter": {
    "Prefix": "pla_activity_daily/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:pla_tracks_vectorize",
   "Filter": {
    "Prefix": "pla_tracks_vectorize/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:power_taipower",
   "Filter": {
    "Prefix": "power_taipower/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:precipitation_raster",
   "Filter": {
    "Prefix": "precipitation_raster/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:rail_timetable",
   "Filter": {
    "Prefix": "rail_timetable/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:rain_gauge_realtime",
   "Filter": {
    "Prefix": "rain_gauge_realtime/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:ripe_atlas_internet_health",
   "Filter": {
    "Prefix": "ripe_atlas_internet_health/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:river_water_level",
   "Filter": {
    "Prefix": "river_water_level/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:road_congestion",
   "Filter": {
    "Prefix": "road_congestion/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:road_event_live",
   "Filter": {
    "Prefix": "road_event_live/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:road_event_planned",
   "Filter": {
    "Prefix": "road_event_planned/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:satellite",
   "Filter": {
    "Prefix": "satellite/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:satellite_passes_daily",
   "Filter": {
    "Prefix": "satellite_passes_daily/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:ship_ais",
   "Filter": {
    "Prefix": "ship_ais/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:temperature",
   "Filter": {
    "Prefix": "temperature/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:tourist_shuttle",
   "Filter": {
    "Prefix": "tourist_shuttle/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:tpml_seat",
   "Filter": {
    "Prefix": "tpml_seat/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:tra_static",
   "Filter": {
    "Prefix": "tra_static/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:tra_train",
   "Filter": {
    "Prefix": "tra_train/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:twse_market_index",
   "Filter": {
    "Prefix": "twse_market_index/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:uswg",
   "Filter": {
    "Prefix": "uswg/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:waste_positions",
   "Filter": {
    "Prefix": "waste_positions/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:water_effluent_monitoring",
   "Filter": {
    "Prefix": "water_effluent_monitoring/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:water_reservoir",
   "Filter": {
    "Prefix": "water_reservoir/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:water_reservoir_daily_ops",
   "Filter": {
    "Prefix": "water_reservoir_daily_ops/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:weather",
   "Filter": {
    "Prefix": "weather/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:wic_evacuate",
   "Filter": {
    "Prefix": "wic_evacuate/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:wic_pumb",
   "Filter": {
    "Prefix": "wic_pumb/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:wic_sewer",
   "Filter": {
    "Prefix": "wic_sewer/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:wra_drought_alert",
   "Filter": {
    "Prefix": "wra_drought_alert/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:youbike",
   "Filter": {
    "Prefix": "youbike/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:yt_live_video_resolver",
   "Filter": {
    "Prefix": "yt_live_video_resolver/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_seasonal",
   "Filter": {
    "Prefix": "jma_seasonal/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_lightning",
   "Filter": {
    "Prefix": "jma_lightning/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_forecast",
   "Filter": {
    "Prefix": "jma_forecast/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:env_jp_wbgt",
   "Filter": {
    "Prefix": "env_jp_wbgt/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  },
  {
   "ID": "archive-deep-270d:jma_ocean_env",
   "Filter": {
    "Prefix": "jma_ocean_env/archives/"
   },
   "Status": "Enabled",
   "Transitions": [
    {
     "Days": 270,
     "StorageClass": "DEEP_ARCHIVE"
    }
   ]
  }
 ]
}
```
