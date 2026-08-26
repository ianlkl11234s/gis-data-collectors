"""Standalone ISOHE normalizer for the VM mirror; keep aligned with tested source."""
import hashlib
import json

MISSING = {'', '-99', '-99.0', '-999', '-999.0', 'nan', 'null', 'none'}
METRICS = {
    'wave': {'Hs_m': ('wave_height', 'm'), 'Tp_sec': ('wave_period', 's'), 'Wave_Direction_degree': ('wave_direction_deg', 'degree'), 'Direction_deg': ('wave_direction_deg', 'degree')},
    'current': {'Velocity_cms': ('current_speed', 'cm/s'), 'Velocity_Direction_degree': ('current_direction_deg', 'degree'), 'Current_Direction': ('current_direction_deg', 'degree'), 'Direction_deg': ('current_direction_deg', 'degree')},
    'wind': {'Wind_Speed_ms': ('wind_speed', 'm/s'), 'Wind_Direction_degree': ('wind_direction_deg', 'degree'), 'Wind_Direction': ('wind_direction_deg', 'degree'), 'Direction_deg': ('wind_direction_deg', 'degree')},
    'tide': {'Tide_TWVD_m': ('tide_twvd', 'm'), 'Tide_CDL_m': ('tide_cdl', 'm'), 'Tide_REF_m': ('tide_ref', 'm')},
}

def _number(value):
    if value is None or str(value).strip().lower() in MISSING:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def isohe_payload_to_long(port, kind, payload, collected_at):
    envelopes = payload if isinstance(payload, list) else [payload]
    rows = []
    for envelope in envelopes:
        if not isinstance(envelope, dict):
            continue
        nested = envelope.get('Datas') or envelope.get('data') or envelope.get('Data')
        if isinstance(nested, list):
            rows.extend((row, envelope) for row in nested if isinstance(row, dict))
        else:
            rows.append((envelope, envelope))
    source_id = f'{port}:{kind}'
    uid = f'isohe:{port.lower()}:{kind.lower()}'
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
    output = []
    first_lon = first_lat = None
    for row, envelope in rows:
        observed_at = row.get('DateTime') or row.get('ObsTime') or row.get('Time') or row.get('datetime')
        if not observed_at:
            continue
        observed_text = str(observed_at).strip()
        if len(observed_text) == 14 and observed_text.isdigit():
            observed_at = f'{observed_text[:4]}-{observed_text[4:6]}-{observed_text[6:8]}T{observed_text[8:10]}:{observed_text[10:12]}:{observed_text[12:14]}+08:00'
        lon = _number(row.get('Station_Longitude') or row.get('Longitude') or row.get('lon') or envelope.get('Station_Longitude') or envelope.get('Longitude') or envelope.get('lon'))
        lat = _number(row.get('Station_Latitude') or row.get('Latitude') or row.get('lat') or envelope.get('Station_Latitude') or envelope.get('Latitude') or envelope.get('lat'))
        if first_lon is None and first_lat is None and lon is not None and lat is not None:
            first_lon, first_lat = lon, lat
        seen_metrics = set()
        for field, (metric, unit) in METRICS[kind].items():
            if field not in row or metric in seen_metrics:
                continue
            seen_metrics.add(metric)
            raw = row.get(field)
            missing = raw is None or str(raw).strip().lower() in MISSING
            value = _number(raw)
            valid = not missing and value is not None
            output.append({'station_uid': uid, 'source_network': 'isohe', 'source_station_id': source_id,
                           'observed_at': observed_at, 'metric_code': metric, 'depth_key': 'surface',
                           'value_raw': None if raw is None else str(raw),
                           'value_numeric': value / 100 if field == 'Velocity_cms' and value is not None else value,
                           'unit_source': unit, 'unit_canonical': 'm/s' if field == 'Velocity_cms' else unit,
                           'vertical_datum': metric.removeprefix('tide_').upper() if metric.startswith('tide_') else None,
                           'is_missing': missing, 'is_valid': valid,
                           'missing_reason': 'source_missing_sentinel' if missing else ('non_numeric_value' if not valid else None),
                           'source_status': None, 'quality_flags': {'missing': missing, 'valid': valid},
                           'payload_sha256': digest, 'collected_at': collected_at, 'longitude': lon, 'latitude': lat})
    if first_lon is None or first_lat is None:
        return [], []
    for row in output:
        if row['longitude'] is None or row['latitude'] is None:
            row['longitude'], row['latitude'] = first_lon, first_lat
    station = {'station_uid': uid, 'source_network': 'isohe', 'source_station_id': source_id,
               'origin_org': 'ISOHE/port', 'distribution_org': 'ISOHE/port', 'station_type': 'port_sensor',
               'name_zh': source_id, 'aliases': [], 'longitude': first_lon, 'latitude': first_lat,
               'observed_elements': sorted({metric for metric, _unit in METRICS[kind].values()}), 'source_status': None,
               'source_url': 'https://isohe.ihmt.gov.tw/opendata/', 'license': None,
               'provenance': {'port': port, 'kind': kind},
               'first_seen_at': collected_at, 'last_seen_at': collected_at}
    return [station], output
