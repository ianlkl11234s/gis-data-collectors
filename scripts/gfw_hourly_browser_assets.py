"""Local, lossless browser assets for the schema-v3 GFW hourly release.

All vector-tile invocations disable Tippecanoe's feature and tile-size limits.
This module deliberately has no S3, network, or deployment dependency.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import subprocess
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable


TIPPECANOE = Path("/opt/homebrew/bin/tippecanoe")
PMTILES = Path("/opt/homebrew/bin/pmtiles")
DETAIL_BUCKETS = tuple(f"{number:x}" for number in range(16))
_EMPTY_PMTILES_CACHE: dict[tuple[tuple[str, ...], int, int], bytes] = {}


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(value)
    temporary.replace(path)


def _gzip_json(path: Path, value: Any) -> None:
    payload = _canonical(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("wb") as handle:
        with gzip.GzipFile(fileobj=handle, mode="wb", filename="", mtime=0) as zipped:
            zipped.write(payload)
    temporary.replace(path)


def _bucket(identifier: str) -> str:
    return hashlib.sha256(identifier.encode("utf-8")).hexdigest()[0]


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"timestamp must be explicitly UTC: {value!r}")
    return parsed.astimezone(timezone.utc)


def _epoch(value: str) -> int:
    return int(_parse_utc(value).timestamp())


def _ship_type_bucket(value: Any) -> str:
    """Stable coarse class for browser filtering; retain the original type too."""
    text = str(value or "").strip().casefold()
    if "fish" in text:
        return "fishing"
    if "tank" in text:
        return "tanker"
    if any(token in text for token in ("passeng", "ferry", "cruise")):
        return "passenger"
    if "cargo" in text or "freight" in text:
        return "cargo"
    if "tug" in text or "tow" in text:
        return "tug"
    if any(token in text for token in ("military", "navy", "warship")):
        return "military"
    return "other"


def _run(command: list[str], *, runner: Callable[..., Any] = subprocess.run) -> None:
    result = runner(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"browser asset command failed ({result.returncode}): {' '.join(command)}\n"
            f"{result.stderr[-2000:]}"
        )


def _empty_mbtiles(path: Path, *, layers: list[str], minimum_zoom: int, maximum_zoom: int) -> None:
    """Create a standards-shaped zero-tile MBTiles archive for an empty observed hour."""
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE metadata (name TEXT PRIMARY KEY, value TEXT)")
    connection.execute(
        "CREATE TABLE tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB, PRIMARY KEY (zoom_level, tile_column, tile_row))"
    )
    metadata = {
        "name": "gfw-empty-observed-frame",
        "format": "pbf",
        "bounds": "-180,-85.05112878,180,85.05112878",
        "minzoom": str(minimum_zoom),
        "maxzoom": str(maximum_zoom),
        "json": json.dumps({
            "vector_layers": [
                {"id": layer, "fields": {}, "minzoom": minimum_zoom, "maxzoom": maximum_zoom}
                for layer in layers
            ]
        }, sort_keys=True, separators=(",", ":")),
    }
    connection.executemany("INSERT INTO metadata VALUES (?, ?)", sorted(metadata.items()))
    # PMTiles conversion requires at least one addressed tile. An empty MVT
    # protobuf is the zero-length message; gzip it deterministically so the
    # archive is valid while still containing no feature or invented geometry.
    connection.execute(
        "INSERT INTO tiles VALUES (?, 0, 0, ?)",
        (minimum_zoom, gzip.compress(b"", mtime=0)),
    )
    connection.commit()
    connection.close()


def _pmtiles(
    *, named_inputs: list[tuple[str, Path]], output: Path,
    minimum_zoom: int, maximum_zoom: int,
    runner: Callable[..., Any] = subprocess.run,
) -> None:
    """Build a PMTiles archive with hard no-dropping options and verify it."""
    if not TIPPECANOE.is_file() or not PMTILES.is_file():
        raise RuntimeError("tippecanoe and pmtiles executables are required for browser assets")
    output.parent.mkdir(parents=True, exist_ok=True)
    empty_inputs = all(source.stat().st_size == 0 for _, source in named_inputs)
    empty_key = (tuple(layer for layer, _ in named_inputs), minimum_zoom, maximum_zoom)
    if empty_inputs and empty_key in _EMPTY_PMTILES_CACHE:
        _atomic_bytes(output, _EMPTY_PMTILES_CACHE[empty_key])
        return
    with tempfile.TemporaryDirectory(prefix="gfw-tippecanoe-", dir=output.parent) as temporary:
        mbtiles = Path(temporary) / "asset.mbtiles"
        if empty_inputs:
            _empty_mbtiles(
                mbtiles, layers=[layer for layer, _ in named_inputs],
                minimum_zoom=minimum_zoom, maximum_zoom=maximum_zoom,
            )
        else:
            command = [
                str(TIPPECANOE), "--force", f"--output={mbtiles}", "--quiet",
                f"--minimum-zoom={minimum_zoom}", f"--maximum-zoom={maximum_zoom}",
                "--no-feature-limit", "--no-tile-size-limit", "--no-line-simplification",
                "--no-clipping",
            ]
            for layer, source in named_inputs:
                command.append(f"--named-layer={layer}:{source}")
            _run(command, runner=runner)
        temporary_output = Path(temporary) / "asset.pmtiles"
        _run([str(PMTILES), "convert", str(mbtiles), str(temporary_output)], runner=runner)
        _run([str(PMTILES), "verify", str(temporary_output)], runner=runner)
        if empty_inputs:
            _EMPTY_PMTILES_CACHE[empty_key] = temporary_output.read_bytes()
        temporary_output.replace(output)


def _write_ndjson(path: Path, features: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    count = 0
    with temporary.open("wb") as handle:
        for feature in features:
            handle.write(_canonical(feature) + b"\n")
            count += 1
    temporary.replace(path)
    return count


def grid_polygon_feature(feature: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Convert a grid-cell center into the explicitly inferred 0.01 degree square."""
    props = feature["properties"]
    observed_at = str(props["observed_at"])
    center_lon = float(props.get("grid_lon", feature["geometry"]["coordinates"][0]))
    center_lat = float(props.get("grid_lat", feature["geometry"]["coordinates"][1]))
    cell_id = str(props.get("cell_id") or feature["id"])
    members = json.loads(props.get("vessels_json", "[]"))
    if int(props["vessel_count"]) != len(members):
        raise ValueError(f"grid cell member count mismatch: {cell_id}")
    delta = 0.005
    polygon = [[
        [center_lon - delta, center_lat - delta], [center_lon + delta, center_lat - delta],
        [center_lon + delta, center_lat + delta], [center_lon - delta, center_lat + delta],
        [center_lon - delta, center_lat - delta],
    ]]
    return {
        "type": "Feature", "id": cell_id,
        "properties": {
            "cell_id": cell_id, "observed_at": observed_at,
            "center_lon": center_lon, "center_lat": center_lat,
            "vessel_count": len(members),
            "geometry_semantics": "inferred_0_01_degree_footprint",
            "coordinate_semantics": "GFW_HIGH_grid_cell_center",
        },
        "geometry": {"type": "Polygon", "coordinates": polygon},
    }, members


def build_grid_browser_assets(
    hour_entries: Iterable[dict[str, Any]], *, source_root: Path, output_root: Path,
    release_id: str,
    runner: Callable[..., Any] = subprocess.run,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Build one lossless grid PMTiles and sixteen deterministic detail buckets per hour."""
    outputs: list[dict[str, Any]] = []
    cells = members = 0
    for entry in sorted(hour_entries, key=lambda value: str(value["observed_at"])):
        collection = json.loads((source_root / entry["path"]).read_text(encoding="utf-8"))
        observed_at = str(entry["observed_at"])
        stamp = _parse_utc(observed_at).strftime("%Y%m%dT%HZ")
        bucketed: dict[str, dict[str, list[dict[str, Any]]]] = {bucket: {} for bucket in DETAIL_BUCKETS}
        browser_features = []
        for feature in collection.get("features") or []:
            browser, cell_members = grid_polygon_feature(feature)
            browser_features.append(browser)
            bucketed[_bucket(browser["properties"]["cell_id"])][browser["properties"]["cell_id"]] = cell_members
            members += len(cell_members)
        ndjson = output_root / "grid" / "input" / f"{stamp}.ndjson"
        _write_ndjson(ndjson, browser_features)
        pmtiles = output_root / "grid" / "hours" / f"{stamp}.pmtiles"
        _pmtiles(
            named_inputs=[("gfw_grid", ndjson)], output=pmtiles,
            minimum_zoom=4, maximum_zoom=12, runner=runner,
        )
        # This is only a local tool input. Keep it on failures for diagnosis,
        # but never let it become a release asset after successful conversion.
        ndjson.unlink()
        details = []
        for bucket in DETAIL_BUCKETS:
            path = output_root / "grid" / "details" / stamp / f"{bucket}.json.gz"
            entries = {
                cell_id: {"vessel_count": len(vessels), "vessels": vessels}
                for cell_id, vessels in bucketed[bucket].items()
            }
            payload = {
                "schema_version": 1, "release_id": release_id,
                "observed_at": observed_at, "bucket": bucket, "key": "cell_id",
                "entry_count": len(entries),
                "vessel_count": sum(value["vessel_count"] for value in entries.values()),
                "entries": entries,
            }
            _gzip_json(path, payload)
            details.append({"bucket": bucket, "path": path.relative_to(output_root).as_posix(), "entry_count": payload["entry_count"], "vessel_count": payload["vessel_count"]})
        if sum(detail["vessel_count"] for detail in details) != sum(int(feature["properties"]["vessel_count"]) for feature in browser_features):
            raise RuntimeError("grid browser detail count mismatch")
        outputs.append({"observed_at": observed_at, "path": pmtiles.relative_to(output_root).as_posix(), "format": "pmtiles", "source_layer": "gfw_grid", "cell_count": len(browser_features), "vessel_count": sum(int(feature["properties"]["vessel_count"]) for feature in browser_features), "detail_buckets": details})
        cells += len(browser_features)
    input_root = output_root / "grid" / "input"
    if input_root.is_dir():
        input_root.rmdir()
    return outputs, {"hour_count": len(outputs), "cell_count": cells, "member_count": members}


def _edge_features(feature: dict[str, Any], *, display_day: str) -> Iterable[dict[str, Any]]:
    props = feature["properties"]
    coordinates = feature["geometry"]["coordinates"]
    times = props["observed_times"]
    for index, (first, second) in enumerate(zip(times, times[1:])):
        if first[:10] != display_day and second[:10] != display_day:
            continue
        yield {"type": "Feature", "id": f"{props['track_id']}:{index}", "properties": {"track_id": props["track_id"], "vessel_id": props["vessel_id"], "type": props.get("vessel_type"), "vessel_type": props.get("vessel_type"), "ship_type_bucket": _ship_type_bucket(props.get("vessel_type")), "display_date": display_day, "from_at": first, "to_at": second, "from_epoch": _epoch(first), "to_epoch": _epoch(second)}, "geometry": {"type": "LineString", "coordinates": [coordinates[index], coordinates[index + 1]]}}


def _singleton_feature(feature: dict[str, Any], *, display_day: str) -> dict[str, Any] | None:
    if feature["properties"]["start_at"][:10] != display_day:
        return None
    props = feature["properties"]
    observed_at = props["start_at"]
    return {"type": "Feature", "id": props["track_id"], "properties": {"track_id": props["track_id"], "vessel_id": props["vessel_id"], "vessel_type": props.get("vessel_type"), "ship_type_bucket": _ship_type_bucket(props.get("vessel_type")), "display_date": display_day, "observed_at": observed_at, "observed_epoch": _epoch(observed_at)}, "geometry": feature["geometry"]}


def _track_detail(feature: dict[str, Any]) -> dict[str, Any]:
    props = feature["properties"]
    observed_times = [str(value) for value in props["observed_times"]]
    if len(observed_times) != int(props["point_count"]):
        raise ValueError(f"track point/time count mismatch: {props['track_id']}")
    if any(_parse_utc(first) >= _parse_utc(second) for first, second in zip(observed_times, observed_times[1:])):
        raise ValueError(f"track timestamps are not strictly increasing: {props['track_id']}")
    return {
        "track_id": str(props["track_id"]),
        "vessel_id": str(props["vessel_id"]),
        "mmsi": props.get("mmsi"),
        "ship_name": props.get("ship_name"),
        "vessel_type": props.get("vessel_type"),
        "flag": props.get("flag"),
        "start_at": str(props["start_at"]),
        "end_at": str(props["end_at"]),
        "point_count": int(props["point_count"]),
        "observed_times": observed_times,
    }


def build_track_browser_assets(
    store: Any, *, start: date, latest: date, output_root: Path,
    release_id: str,
    runner: Callable[..., Any] = subprocess.run,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    """Build lossless daily edge/singleton PMTiles, frames, and detail buckets."""
    output_root.mkdir(parents=True, exist_ok=True)
    days: list[dict[str, Any]] = []
    frame_entries: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="gfw-frames-", dir=output_root) as temporary:
        connection = sqlite3.connect(Path(temporary) / "frames.sqlite3")
        connection.execute("CREATE TABLE frames (observed_at TEXT, vessel_id TEXT, frame_json TEXT, PRIMARY KEY (observed_at, vessel_id)) WITHOUT ROWID")

        def add_frame(frame: dict[str, Any]) -> None:
            connection.execute("INSERT INTO frames VALUES (?, ?, ?)", (frame["observed_at"], frame["vessel_id"], _canonical(frame).decode("utf-8")))

        for feature in store.iter_tracks():
            props, coordinates, times = feature["properties"], feature["geometry"]["coordinates"], feature["properties"]["observed_times"]
            for index, (coordinate, observed_at) in enumerate(zip(coordinates, times)):
                frame = {
                    "vessel_id": props["vessel_id"], "track_id": props["track_id"],
                    "mmsi": props.get("mmsi"), "ship_name": props.get("ship_name"),
                    "vessel_type": props.get("vessel_type"), "ship_type_bucket": _ship_type_bucket(props.get("vessel_type")), "flag": props.get("flag"),
                    "observed_at": observed_at, "observed_epoch": _epoch(observed_at),
                    "lon": coordinate[0], "lat": coordinate[1],
                }
                if index + 1 < len(times) and (_parse_utc(times[index + 1]) - _parse_utc(observed_at)).total_seconds() == 3600:
                    frame.update({"to_lon": coordinates[index + 1][0], "to_lat": coordinates[index + 1][1], "to_at": times[index + 1], "to_epoch": _epoch(times[index + 1])})
                add_frame(frame)
        for feature in store.iter_singleton_nodes():
            props, coordinate = feature["properties"], feature["geometry"]["coordinates"]
            add_frame({
                "vessel_id": props["vessel_id"], "track_id": props["track_id"],
                "mmsi": props.get("mmsi"), "ship_name": props.get("ship_name"),
                "vessel_type": props.get("vessel_type"), "ship_type_bucket": _ship_type_bucket(props.get("vessel_type")), "flag": props.get("flag"),
                "observed_at": props["start_at"], "observed_epoch": _epoch(props["start_at"]),
                "lon": coordinate[0], "lat": coordinate[1],
            })
        connection.commit()

        current = start
        while current <= latest:
            day_text = current.isoformat()
            edge_input = output_root / "tracks" / "input" / f"{day_text}-edges.ndjson"
            singleton_input = output_root / "tracks" / "input" / f"{day_text}-singletons.ndjson"
            edges = (edge for feature in store.iter_tracks() for edge in _edge_features(feature, display_day=day_text))
            singles = (node for feature in store.iter_singleton_nodes() if (node := _singleton_feature(feature, display_day=day_text)) is not None)
            edge_count = _write_ndjson(edge_input, edges)
            singleton_count = _write_ndjson(singleton_input, singles)
            pmtiles = output_root / "tracks" / "days" / f"{day_text}.pmtiles"
            _pmtiles(
                named_inputs=[("gfw_track_edges", edge_input), ("gfw_track_singletons", singleton_input)],
                output=pmtiles, minimum_zoom=5, maximum_zoom=12, runner=runner,
            )
            edge_input.unlink()
            singleton_input.unlink()
            bucketed: dict[str, dict[str, dict[str, Any]]] = {bucket: {} for bucket in DETAIL_BUCKETS}
            for feature in store.iter_features():
                if any(time[:10] == day_text for time in feature["properties"]["observed_times"]):
                    detail = _track_detail(feature)
                    bucketed[_bucket(detail["track_id"])][detail["track_id"]] = detail
            details = []
            for bucket in DETAIL_BUCKETS:
                path = output_root / "tracks" / "details" / day_text / f"{bucket}.json.gz"
                entries = bucketed[bucket]
                point_count = sum(value["point_count"] for value in entries.values())
                payload = {
                    "schema_version": 1, "release_id": release_id,
                    "display_date": day_text, "bucket": bucket, "key": "track_id",
                    "entry_count": len(entries), "point_count": point_count,
                    "entries": entries,
                }
                _gzip_json(path, payload)
                details.append({
                    "bucket": bucket, "path": path.relative_to(output_root).as_posix(),
                    "entry_count": payload["entry_count"], "point_count": point_count,
                })
            days.append({
                "display_date": day_text, "path": pmtiles.relative_to(output_root).as_posix(),
                "format": "pmtiles",
                "source_layers": {"edges": "gfw_track_edges", "singletons": "gfw_track_singletons"},
                "edge_count": edge_count, "singleton_count": singleton_count,
                "detail_buckets": details,
            })
            current += timedelta(days=1)
        input_root = output_root / "tracks" / "input"
        if input_root.is_dir():
            input_root.rmdir()

        current_hour = datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc)
        end_exclusive = datetime.combine(latest + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
        while current_hour < end_exclusive:
            observed_at = current_hour.isoformat()
            stamp = _parse_utc(observed_at).strftime("%Y%m%dT%HZ")
            path = output_root / "tracks" / "frames" / f"{stamp}.geojson.gz"
            values = [json.loads(value) for (value,) in connection.execute("SELECT frame_json FROM frames WHERE observed_at = ? ORDER BY vessel_id", (observed_at,))]
            payload = {
                "type": "FeatureCollection",
                "metadata": {
                    "schema_version": 1, "release_id": release_id,
                    "observed_at": observed_at, "entry_count": len(values),
                },
                "features": [{
                    "type": "Feature", "id": frame["vessel_id"],
                    "properties": {key: value for key, value in frame.items() if key not in {"lon", "lat"}},
                    "geometry": {"type": "Point", "coordinates": [frame["lon"], frame["lat"]]},
                } for frame in values],
            }
            _gzip_json(path, payload)
            frame_entries.append({
                "observed_at": observed_at, "path": path.relative_to(output_root).as_posix(),
                "features": len(values), "format": "geojson", "content_encoding": "gzip",
            })
            current_hour += timedelta(hours=1)
        connection.close()
    return days, frame_entries, {
        "day_count": len(days), "frame_count": len(frame_entries),
        "frame_vessel_count": sum(entry["features"] for entry in frame_entries),
    }
