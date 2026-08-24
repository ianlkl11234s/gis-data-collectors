"""AISStream 常駐 WebSocket collector。

這條管線刻意不使用既有 ``ship_ais`` / ``SupabaseWriter``：AISStream 是另一個
provider，資料表、raw archive、dedup key 與健康狀態都保持獨立，方便日後和
航港局 AIS 做覆蓋率比較。完整 raw event 先寫本機 durable spool，再以每小時
gzip NDJSON 永久歸檔到 S3；S3 object 不設定 expiry / delete marker / TTL。

執行方式：
    AISSTREAM_ENABLED=true python3 -m workers.aisstream

主程式會在同一 process 內以 daemon thread 啟動本 worker（預設 disabled）。
"""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import queue
import random
import re
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import config

try:  # optional dependency: disabled deployments need not import websockets
    from websockets.sync.client import connect as websocket_connect
except ImportError:  # pragma: no cover - exercised only on minimal installs
    websocket_connect = None

logger = logging.getLogger(__name__)
UTC = timezone.utc
PROVIDER = "aisstream"
SCHEMA_VERSION = "aisstream.raw.v1"
MESSAGE_TYPES = (
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
    "ShipStaticData",
    "StaticDataReport",
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: Any, fallback: datetime | None = None) -> str | None:
    """將 AISStream 的 timestamp 轉為 UTC ISO 字串。"""
    if value is None:
        return fallback.isoformat() if fallback else None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC).isoformat()
    text = str(value).strip()
    if not text:
        return fallback.isoformat() if fallback else None
    if text.endswith(" UTC"):
        text = text[:-4]
    text = re.sub(r"\s+([+-]\d{4})$", r"\1", text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return fallback.isoformat() if fallback else None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _observed_at(event: dict, received_at: datetime | str | None = None) -> str | None:
    """Use the provider's complete metadata time, never AIS Timestamp as epoch."""
    metadata = event.get("MetaData") or event.get("Metadata") or {}
    candidates = (
        metadata.get("time_utc"), metadata.get("TimeUTC"),
        event.get("time_utc"), event.get("TimeUTC"),
    )
    fallback = received_at
    if isinstance(fallback, str):
        try:
            fallback = datetime.fromisoformat(fallback.replace("Z", "+00:00"))
        except ValueError:
            fallback = None
    for candidate in candidates:
        parsed = _iso(candidate, None)
        if parsed:
            return parsed
    return _iso(None, fallback if isinstance(fallback, datetime) else _utc_now())


def _is_subscription_confirmation(event: dict) -> bool:
    kind = str(event.get("MessageType") or event.get("type") or event.get("Type") or "").lower()
    return kind in {"subscriptionconfirmation", "subscription_confirmed", "subscriptionconfirmationmessage"}


def _number(value: Any) -> float | int | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _canonical_hash(payload: dict) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _message_body(event: dict) -> dict:
    message = event.get("Message") or {}
    # Some AISStream samples wrap the actual message one level deeper.
    if isinstance(message, dict) and len(message) == 1:
        value = next(iter(message.values()))
        if isinstance(value, dict):
            return value
    return message if isinstance(message, dict) else {}


def _event_type(event: dict) -> str:
    return str(event.get("MessageType") or event.get("message_type") or "Unknown")


def _mmsi(event: dict, body: dict) -> str | None:
    metadata = event.get("MetaData") or event.get("Metadata") or {}
    value = (
        event.get("MMSI")
        or metadata.get("MMSI")
        or body.get("UserID")
        or body.get("Mmsi")
        or body.get("MMSI")
    )
    if value is None:
        return None
    text = str(value).strip()
    return text if text.isdigit() and len(text) == 9 else None


def _coordinates(event: dict, body: dict) -> tuple[float | None, float | None]:
    metadata = event.get("MetaData") or event.get("Metadata") or {}
    lat = body.get("Latitude", body.get("latitude", metadata.get("latitude")))
    lon = body.get("Longitude", body.get("longitude", metadata.get("longitude")))
    lat_number = _number(lat)
    lon_number = _number(lon)
    if lat_number is None or lon_number is None:
        return None, None
    if not -90 <= float(lat_number) <= 90 or not -180 <= float(lon_number) <= 180:
        return None, None
    return float(lat_number), float(lon_number)


def _subscription_boxes() -> tuple[list[list[list[float]]], dict[str, str]]:
    try:
        configured = json.loads(config.AISSTREAM_BBOXES)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("AISSTREAM_BBOXES 必須是 JSON array") from exc
    boxes: list[list[list[float]]] = []
    names: dict[str, str] = {}
    for item in configured:
        box = item.get("box") if isinstance(item, dict) else item
        name = item.get("name", f"bbox_{len(boxes) + 1}") if isinstance(item, dict) else f"bbox_{len(boxes) + 1}"
        if not isinstance(box, list) or len(box) != 2:
            raise ValueError(f"AISSTREAM_BBOXES 區域格式錯誤: {name}")
        corners = [[float(point[0]), float(point[1])] for point in box]
        if not (-90 <= corners[0][0] <= corners[1][0] <= 90 and -180 <= corners[0][1] <= corners[1][1] <= 180):
            raise ValueError(f"AISSTREAM_BBOXES 座標超出範圍: {name}")
        boxes.append(corners)
        names[name] = name
    if not boxes:
        raise ValueError("AISSTREAM_BBOXES 不可為空")
    return boxes, names


@dataclass
class WorkerStats:
    started_at: str = field(default_factory=lambda: _utc_now().isoformat())
    messages_received: int = 0
    observations_seen: int = 0
    static_seen: int = 0
    db_written: int = 0
    db_errors: int = 0
    reconnects: int = 0
    s3_archives: int = 0
    last_message_at: str | None = None
    first_observed_at: str | None = None
    last_observed_at: str | None = None
    last_s3_success_at: str | None = None
    last_error: str | None = None


class SpoolManager:
    """本機 durable NDJSON spool + S3 cold archive。

    檔案只有在 S3 upload + HEAD 驗證通過後才刪除；S3 永不由此 class 刪檔。
    """

    def __init__(self, base_dir: Path, s3_storage=None, *, on_archive=None, run_id: str | None = None):
        self.base_dir = base_dir / "aisstream_spool"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.s3 = s3_storage
        self.on_archive = on_archive
        self.run_id = run_id
        self._path: Path | None = None
        self._file = None
        self._opened_at: datetime | None = None
        self._count = 0
        self._first_received: str | None = None
        self._last_received: str | None = None

    def append(self, event: dict, received_at: str) -> None:
        if self._file is None:
            self._opened_at = _utc_now()
            stamp = self._opened_at.strftime("%Y%m%dT%H%M%SZ")
            self._path = self.base_dir / f"aisstream-{stamp}-{uuid.uuid4().hex[:10]}.jsonl"
            self._file = self._path.open("a", encoding="utf-8")
            self._count = 0
            self._first_received = received_at
        record = {"schema_version": SCHEMA_VERSION, "received_at": received_at, "event": event}
        self._file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._file.flush()
        self._count += 1
        self._last_received = received_at

    def should_rotate(self) -> bool:
        if not self._file or not self._opened_at:
            return False
        age = (_utc_now() - self._opened_at).total_seconds() / 60
        return age >= config.AISSTREAM_SPOOL_ROTATE_MINUTES

    def exceeds_limit(self) -> bool:
        """回報 local spool 是否超過 hard limit；永不自動刪未驗證 raw。"""
        total = sum(path.stat().st_size for path in self.base_dir.glob("*") if path.is_file())
        return total >= config.AISSTREAM_SPOOL_MAX_MB * 1024 * 1024

    def rotate(self) -> Path | None:
        if not self._file or not self._path:
            return None
        self._file.flush()
        self._file.close()
        source = self._path
        self._file = None
        self._path = None
        gz_path = source.with_suffix(source.suffix + ".gz")
        with source.open("rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)
        source.unlink(missing_ok=True)
        self._upload(gz_path, self._count, self._first_received, self._last_received)
        self._opened_at = None
        self._count = 0
        self._first_received = None
        self._last_received = None
        return gz_path

    def retry_pending(self) -> int:
        uploaded = 0
        for path in sorted(self.base_dir.glob("*.jsonl.gz")):
            if self._upload(path):
                uploaded += 1
        return uploaded

    def close(self) -> None:
        if self._file:
            self.rotate()

    def _upload(self, path: Path, count: int | None = None, first: str | None = None, last: str | None = None) -> bool:
        if not self.s3 or not path.exists():
            return False
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        date_part = modified.strftime("%Y-%m-%d")
        hour_part = modified.strftime("%H")
        key = f"{config.AISSTREAM_S3_PREFIX}/date={date_part}/hour={hour_part}/{path.name}"
        try:
            if not self.s3.s3.head_object(Bucket=self.s3.bucket, Key=key):
                return False
        except self.s3.ClientError:
            try:
                self.s3.s3.upload_file(
                    str(path), self.s3.bucket, key,
                    ExtraArgs={
                        "ContentType": "application/x-ndjson",
                        "ContentEncoding": "gzip",
                        "StorageClass": config.AISSTREAM_S3_STORAGE_CLASS,
                        "Metadata": {"sha256": digest, "schema-version": SCHEMA_VERSION},
                    },
                )
            except Exception as exc:
                logger.warning("AISStream S3 upload failed: %s", exc)
                return False
        try:
            head = self.s3.s3.head_object(Bucket=self.s3.bucket, Key=key)
            if int(head.get("ContentLength", -1)) != path.stat().st_size:
                logger.error("AISStream S3 size verification failed: %s", key)
                return False
            object_meta = {str(k).lower(): str(v) for k, v in (head.get("Metadata") or {}).items()}
            if object_meta.get("sha256") != digest:
                logger.error("AISStream S3 sha256 verification failed: %s", key)
                return False
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "object_key": key,
                "sha256": digest,
                "bytes": path.stat().st_size,
                "record_count": count,
                "first_received_at": first,
                "last_received_at": last,
                "bbox_config_sha256": hashlib.sha256(config.AISSTREAM_BBOXES.encode()).hexdigest(),
                "storage_policy": "permanent_cold_archive_no_expiry",
                "run_id": self.run_id,
            }
            manifest_key = key.removesuffix(".jsonl.gz") + ".manifest.json"
            self.s3.s3.put_object(
                Bucket=self.s3.bucket, Key=manifest_key,
                Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
                ContentType="application/json",
                StorageClass=config.AISSTREAM_S3_STORAGE_CLASS,
                Metadata={"sha256": digest, "schema-version": SCHEMA_VERSION},
            )
            manifest_head = self.s3.s3.head_object(Bucket=self.s3.bucket, Key=manifest_key)
            if int(manifest_head.get("ContentLength", -1)) <= 0:
                logger.error("AISStream manifest size verification failed: %s", manifest_key)
                return False
            manifest_meta = {str(k).lower(): str(v) for k, v in (manifest_head.get("Metadata") or {}).items()}
            if manifest_meta.get("sha256") != digest:
                logger.error("AISStream manifest metadata verification failed: %s", manifest_key)
                return False
            persisted = self.s3.s3.get_object(Bucket=self.s3.bucket, Key=manifest_key)["Body"].read()
            persisted_manifest = json.loads(persisted.decode("utf-8"))
            if persisted_manifest.get("sha256") != digest or int(persisted_manifest.get("bytes", -1)) != path.stat().st_size:
                logger.error("AISStream manifest content verification failed: %s", manifest_key)
                return False
            if self.on_archive:
                self.on_archive(manifest)
            # The DB manifest ledger is part of the verification contract.
            # Delete the durable local retry copy only after that callback
            # commits successfully; callback errors are caught below.
            path.unlink(missing_ok=True)
            return True
        except Exception as exc:
            logger.warning("AISStream S3 verification/manifest failed: %s", exc)
            return False


class AISStreamWorker:
    """單一連線、多 bbox、可重連的 AISStream worker。"""

    def __init__(self, *, api_key: str | None = None, db_url: str | None = None, s3_storage=None):
        self.api_key = api_key or config.AISSTREAM_API_KEY
        self.db_url = db_url or config.SUPABASE_DB_URL
        self.stop_event = threading.Event()
        self.stats = WorkerStats()
        self.run_id = str(uuid.uuid4())
        self.boxes, self.bbox_names = _subscription_boxes()
        self.queue: queue.Queue[tuple[dict, str]] = queue.Queue(maxsize=config.AISSTREAM_QUEUE_MAXSIZE)
        # 直接重用共用 pool，避免在 Supavisor transaction mode 維持一條 session
        # 級連線；raw spool/S3 仍可在 DB 暫時不可用時獨立繼續。
        from collectors.base import get_supabase_writer
        self._writer = get_supabase_writer() if config.SUPABASE_ENABLED and self.db_url else None
        self._batch: list[tuple[dict, str]] = []
        self._static_cache: dict[str, dict] = {}
        self._last_db_flush = time.monotonic()
        self._health_last = 0.0
        self._started = _utc_now()
        self._prepared = False
        self._s3 = s3_storage
        if self._s3 is None and config.S3_BUCKET:
            try:
                from storage.s3 import S3Storage
                self._s3 = S3Storage()
            except Exception as exc:
                logger.warning("AISStream S3 disabled: %s", exc)
        self.spool = SpoolManager(config.LOCAL_DATA_DIR, self._s3, on_archive=self._record_archive_manifest, run_id=self.run_id)

    def preflight(self) -> None:
        if not self.api_key:
            raise RuntimeError("AISSTREAM_API_KEY 未設定")
        if websocket_connect is None:
            raise RuntimeError("缺少 websockets，請安裝 requirements.txt")
        if self._writer is None:
            raise RuntimeError("AISStream 需要可用的 Supabase writer，不可只收 raw 而無 run/health ledger")
        if self._s3 is None:
            raise RuntimeError("AISStream 需要可用的 S3 cold archive，不可在未永久備份時啟動")

    def prepare(self) -> None:
        """Fail closed synchronously before the daemon thread is started."""
        if self._prepared:
            return
        self.preflight()
        self._write_run_start()
        self.spool.retry_pending()
        self._prepared = True

    def run(self) -> None:
        self.prepare()
        logger.info("AISStream worker started: %d bboxes, %d message types", len(self.boxes), len(MESSAGE_TYPES))
        delay = 1.0
        try:
            while not self.stop_event.is_set() and not self._campaign_expired():
                try:
                    self._connect_and_consume()
                    delay = 1.0
                except Exception as exc:
                    self.stats.last_error = str(exc)[:500]
                    logger.warning("AISStream connection failed: %s", exc)
                    self.stats.reconnects += 1
                    self._write_health("reconnecting")
                    if self.stop_event.wait(random.uniform(0, delay)):
                        break
                    delay = min(config.AISSTREAM_RECONNECT_MAX_SECONDS, delay * 2)
        finally:
            self._flush_batch()
            self.spool.close()
            self._write_run_end("stopped" if not self._campaign_expired() else "succeeded")
            self._write_health("stopped")
            logger.info("AISStream worker stopped: messages=%d observations=%d archives=%d", self.stats.messages_received, self.stats.observations_seen, self.stats.s3_archives)

    def stop(self) -> None:
        self.stop_event.set()

    def _campaign_expired(self) -> bool:
        return config.AISSTREAM_CAMPAIGN_DAYS > 0 and _utc_now() >= self._started + timedelta(days=config.AISSTREAM_CAMPAIGN_DAYS)

    def _connect_and_consume(self) -> None:
        ws = websocket_connect(
            config.AISSTREAM_WS_URL,
            open_timeout=max(5, config.REQUEST_TIMEOUT),
            compression="deflate",
        )
        subscription_confirmed = False
        try:
            subscription = {
                "APIKey": self.api_key,
                "BoundingBoxes": self.boxes,
                "FilterMessageTypes": list(MESSAGE_TYPES),
            }
            ws.send(json.dumps(subscription, separators=(",", ":")))
            while not self.stop_event.is_set():
                payload = ws.recv()
                if payload is None:
                    raise ConnectionError("AISStream websocket closed")
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8")
                event = json.loads(payload)
                if event.get("error"):
                    raise ConnectionError(f"AISStream subscription error: {event.get('error')}")
                if not subscription_confirmed:
                    if _is_subscription_confirmation(event):
                        if event.get("success") is False or event.get("Success") is False:
                            raise ConnectionError("AISStream subscription was rejected")
                    elif _event_type(event) not in MESSAGE_TYPES:
                        raise ConnectionError("AISStream did not confirm subscription")
                    # Socket-open alone is not healthy; an explicit ack or valid AIS
                    # event proves that the subscription was accepted.
                    subscription_confirmed = True
                    self._write_health("connected")
                received_at = _utc_now().isoformat()
                self.stats.messages_received += 1
                self.stats.last_message_at = received_at
                try:
                    self.spool.append(event, received_at)
                except OSError as exc:
                    self.stats.last_error = f"spool: {exc}"
                    logger.error("AISStream spool write failed: %s", exc)
                if self.spool.exceeds_limit():
                    self.stats.last_error = "local AISStream spool exceeds AISSTREAM_SPOOL_MAX_MB"
                    logger.error("AISStream spool limit reached; stopping before deleting any unverified raw")
                    self.stop_event.set()
                    break
                try:
                    self.queue.put((event, received_at), timeout=0.25)
                except queue.Full:
                    # 原始事件已在 spool；DB 壅塞時丟棄 normalized queue，不能阻塞 socket。
                    logger.warning("AISStream bounded queue full; raw event remains in spool")
                self._drain_queue(max_items=50)
                if self.spool.should_rotate():
                    path = self.spool.rotate()
                    if path and not path.exists():
                        self.stats.s3_archives += 1
                if len(self._batch) >= config.AISSTREAM_DB_BATCH_SIZE or time.monotonic() - self._last_db_flush >= config.AISSTREAM_DB_FLUSH_SECONDS:
                    self._flush_batch()
                if time.monotonic() - self._health_last >= config.AISSTREAM_HEALTH_INTERVAL_SECONDS:
                    self._write_health("connected")
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _drain_queue(self, max_items: int) -> None:
        for _ in range(max_items):
            try:
                event, received_at = self.queue.get_nowait()
            except queue.Empty:
                return
            self._batch.append((event, received_at))
            self._normalize_count(event)

    def _normalize_count(self, event: dict) -> None:
        body = _message_body(event)
        mmsi = _mmsi(event, body)
        if not mmsi:
            return
        if _event_type(event) in ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport"):
            lat, lon = _coordinates(event, body)
            if lat is not None and lon is not None:
                self.stats.observations_seen += 1
                self.stats.last_observed_at = _observed_at(event, _utc_now())
                if self.stats.first_observed_at is None:
                    self.stats.first_observed_at = self.stats.last_observed_at
        elif _event_type(event) in ("ShipStaticData", "StaticDataReport"):
            self.stats.static_seen += 1
            self._static_cache[mmsi] = self._static_fields(body)

    @staticmethod
    def _static_fields(body: dict) -> dict:
        def text(value):
            return None if value is None or value == "" else str(value)

        return {
            "ship_name": text(body.get("Name")),
            "ship_type": text(body.get("Type", body.get("ShipType"))),
            "imo": text(body.get("ImoNumber", body.get("IMO"))),
            "call_sign": text(body.get("CallSign")),
            "destination": text(body.get("Destination")),
        }

    def _coverage_zone(self, lat: float, lon: float) -> str | None:
        try:
            configured = json.loads(config.AISSTREAM_BBOXES)
        except (TypeError, json.JSONDecodeError):
            return None
        for item in configured:
            box = item.get("box") if isinstance(item, dict) else item
            if box and box[0][0] <= lat <= box[1][0] and box[0][1] <= lon <= box[1][1]:
                return item.get("name") if isinstance(item, dict) else None
        return None

    def _flush_batch(self) -> None:
        self._drain_queue(max_items=config.AISSTREAM_DB_BATCH_SIZE * 4)
        if not self._batch:
            self._last_db_flush = time.monotonic()
            return
        batch, self._batch = self._batch, []
        try:
            self._write_db(batch)
        except Exception as exc:
            self.stats.db_errors += 1
            self.stats.last_error = f"db: {exc}"
            logger.warning("AISStream DB batch failed; raw events remain archived: %s", exc)
        self._last_db_flush = time.monotonic()

    def _write_db(self, batch: list[tuple[dict, str]]) -> None:
        if self._writer is None:
            return
        from psycopg2.extras import Json, execute_values
        observations: list[tuple] = []
        current: dict[str, tuple] = {}
        static_updates: dict[str, tuple] = {}
        for event, received_at in batch:
            body = _message_body(event)
            mmsi = _mmsi(event, body)
            if not mmsi:
                continue
            message_type = _event_type(event)
            event_hash = _canonical_hash(event)
            observed_at = _observed_at(event, received_at)
            lat, lon = _coordinates(event, body)
            if message_type in ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport") and lat is not None and lon is not None:
                static = self._static_cache.get(mmsi, {})
                record = (
                    observed_at, PROVIDER, self.run_id, mmsi, message_type, event_hash, event_hash,
                    received_at, self._coverage_zone(lat, lon), static.get("ship_name"),
                    static.get("ship_type", body.get("ShipType")), static.get("imo"),
                    static.get("call_sign"), static.get("destination"),
                    str(body.get("NavigationalStatus")) if body.get("NavigationalStatus") is not None else None,
                    _number(body.get("Sog")), _number(body.get("Cog")), _number(body.get("TrueHeading")),
                    # longitude/latitude are repeated for the PostGIS expression
                    # used by execute_values; geom is NOT NULL in migration 371.
                    lon, lat, lon, lat, "accepted", Json([]), None,
                )
                observations.append(record)
                current[mmsi] = record
            elif message_type in ("ShipStaticData", "StaticDataReport"):
                self._static_cache[mmsi] = self._static_fields(body)
                static_updates[mmsi] = (observed_at, received_at, event_hash, self._static_cache[mmsi])
        with self._writer.with_conn(timeout=config.SUPABASE_BORROW_TIMEOUT_SEC) as db:
            db.autocommit = False
            try:
                with db.cursor() as cur:
                    if observations:
                        execute_values(cur, """INSERT INTO live.aisstream_position_observations
                    (observed_at,provider,run_id,mmsi,message_type,source_event_key,payload_sha256,received_at,coverage_zone,ship_name,ship_type,imo,call_sign,destination,nav_status,speed_knots,course_over_ground,true_heading,longitude,latitude,geom,position_quality,quality_flags,raw_archive_key)
                    VALUES %s ON CONFLICT DO NOTHING""", observations,
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),%s,%s,%s)")
                        for record in current.values():
                            cur.execute("""INSERT INTO live.aisstream_vessel_current
                        (provider,mmsi,observed_at,received_at,source_event_key,payload_sha256,coverage_zone,ship_name,ship_type,imo,call_sign,destination,nav_status,speed_knots,course_over_ground,true_heading,longitude,latitude,position_quality,quality_flags,source_run_id,geom,updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),now())
                        ON CONFLICT (provider,mmsi) DO UPDATE SET
                          observed_at=EXCLUDED.observed_at,received_at=EXCLUDED.received_at,source_event_key=EXCLUDED.source_event_key,
                          payload_sha256=EXCLUDED.payload_sha256,coverage_zone=EXCLUDED.coverage_zone,ship_name=COALESCE(EXCLUDED.ship_name,live.aisstream_vessel_current.ship_name),
                          ship_type=COALESCE(EXCLUDED.ship_type,live.aisstream_vessel_current.ship_type),imo=COALESCE(EXCLUDED.imo,live.aisstream_vessel_current.imo),
                          call_sign=COALESCE(EXCLUDED.call_sign,live.aisstream_vessel_current.call_sign),destination=COALESCE(EXCLUDED.destination,live.aisstream_vessel_current.destination),
                          nav_status=EXCLUDED.nav_status,speed_knots=EXCLUDED.speed_knots,course_over_ground=EXCLUDED.course_over_ground,true_heading=EXCLUDED.true_heading,
                          longitude=EXCLUDED.longitude,latitude=EXCLUDED.latitude,position_quality=EXCLUDED.position_quality,quality_flags=EXCLUDED.quality_flags,
                          source_run_id=EXCLUDED.source_run_id,geom=EXCLUDED.geom,updated_at=now()
                        WHERE live.aisstream_vessel_current.observed_at < EXCLUDED.observed_at
                           OR (live.aisstream_vessel_current.observed_at = EXCLUDED.observed_at AND live.aisstream_vessel_current.received_at <= EXCLUDED.received_at)""", (
                               record[1], record[3], record[0], record[7], record[5], record[6], record[8], record[9], record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18], record[19], record[22], record[23], record[2], record[20], record[21]))
                    for mmsi, (observed_at, received_at, event_hash, fields) in static_updates.items():
                        cur.execute("""UPDATE live.aisstream_vessel_current SET
                            ship_name=COALESCE(%s,ship_name),ship_type=COALESCE(%s,ship_type),
                            imo=COALESCE(%s,imo),call_sign=COALESCE(%s,call_sign),destination=COALESCE(%s,destination),
                            updated_at=now()
                            WHERE provider=%s AND mmsi=%s""",
                            (fields.get("ship_name"), fields.get("ship_type"), fields.get("imo"),
                             fields.get("call_sign"), fields.get("destination"), PROVIDER, mmsi))
                db.commit()
            except Exception:
                db.rollback()
                raise
        self.stats.db_written += len(observations)

    def _write_health(self, status: str) -> None:
        self._health_last = time.monotonic()
        if self._writer is None:
            return
        status = {"connected": "healthy", "reconnecting": "degraded"}.get(status, status)
        try:
            with self._writer.with_conn(timeout=config.SUPABASE_BORROW_TIMEOUT_SEC) as db:
                db.autocommit = False
                with db.cursor() as cur:
                    cur.execute("""INSERT INTO live.aisstream_ingest_health
                    (provider,current_run_id,status,last_message_observed_at,last_message_received_at,last_archive_verified_at,messages_today,positions_today,duplicates_today,rejected_today,updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,%s,now())
                    ON CONFLICT (provider) DO UPDATE SET current_run_id=EXCLUDED.current_run_id,status=EXCLUDED.status,
                    last_message_observed_at=EXCLUDED.last_message_observed_at,last_message_received_at=EXCLUDED.last_message_received_at,
                    last_archive_verified_at=EXCLUDED.last_archive_verified_at,messages_today=EXCLUDED.messages_today,positions_today=EXCLUDED.positions_today,
                    updated_at=now()""",
                    (PROVIDER, self.run_id, status, self.stats.last_observed_at, self.stats.last_message_at, self.stats.last_s3_success_at, self.stats.messages_received, self.stats.observations_seen, 0))
                db.commit()
        except Exception as exc:
            logger.debug("AISStream health write failed: %s", exc)

    def _write_run_end(self, status: str) -> None:
        """將本次 worker lifecycle 寫入 ingest_runs（best effort）。"""
        if self._writer is None:
            return
        from psycopg2.extras import Json

        try:
            with self._writer.with_conn(timeout=config.SUPABASE_BORROW_TIMEOUT_SEC) as db:
                db.autocommit = False
                with db.cursor() as cur:
                    cur.execute("""UPDATE live.aisstream_ingest_runs SET
                        completed_at=now(),status=%s,message_count=%s,position_count=%s,
                        first_observed_at=%s,last_observed_at=%s,last_received_at=%s,raw_archive_prefix=%s,archive_verified_at=%s,
                        quality_summary=%s,error_message=%s WHERE run_id=%s""",
                        (status, self.stats.messages_received, self.stats.observations_seen,
                         self.stats.first_observed_at, self.stats.last_observed_at, self.stats.last_message_at, config.AISSTREAM_S3_PREFIX,
                         self.stats.last_s3_success_at, Json({"db_written": self.stats.db_written, "reconnects": self.stats.reconnects}),
                         self.stats.last_error, self.run_id))
                db.commit()
        except Exception as exc:
            logger.debug("AISStream ingest run write failed: %s", exc)

    def _write_run_start(self) -> None:
        if self._writer is None:
            return
        from psycopg2.extras import Json

        # Position observations have a required FK to this run.  Treating this
        # insert as best effort would make every later batch fail while the
        # socket continues consuming data, so fail closed before connecting.
        with self._writer.with_conn(timeout=config.SUPABASE_BORROW_TIMEOUT_SEC) as db:
            db.autocommit = False
            try:
                with db.cursor() as cur:
                    cur.execute("""INSERT INTO live.aisstream_ingest_runs
                        (run_id,provider,started_at,status,subscription_config,subscription_config_sha256,
                         websocket_endpoint,collector_version,raw_archive_prefix)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (run_id) DO NOTHING""",
                        (self.run_id, PROVIDER, self.stats.started_at, "running",
                         Json({"bounding_boxes": self.boxes, "message_types": list(MESSAGE_TYPES)}),
                         hashlib.sha256(config.AISSTREAM_BBOXES.encode()).hexdigest(), config.AISSTREAM_WS_URL,
                         SCHEMA_VERSION, config.AISSTREAM_S3_PREFIX))
                db.commit()
            except Exception:
                db.rollback()
                raise

    def _record_archive_manifest(self, manifest: dict) -> None:
        """把已完成 S3 object 的 manifest 寫入 DB，作為 archive audit ledger。"""
        if self._writer is None:
            raise RuntimeError("AISStream archive manifest requires Supabase writer")
        from psycopg2.extras import Json

        try:
            with self._writer.with_conn(timeout=config.SUPABASE_BORROW_TIMEOUT_SEC) as db:
                db.autocommit = False
                with db.cursor() as cur:
                    cur.execute("""INSERT INTO live.aisstream_archive_manifests
                        (provider,run_id,s3_key,sha256,storage_class,schema_version,archive_format,period_start,period_end,message_count,byte_size,status,verified_at,verification_details)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'verified',now(),%s)
                        ON CONFLICT (s3_key) DO NOTHING""",
                        (PROVIDER, self.run_id, manifest["object_key"], manifest["sha256"], config.AISSTREAM_S3_STORAGE_CLASS,
                         "v1", "jsonl.gz", manifest.get("first_received_at") or _utc_now().isoformat(),
                         manifest.get("last_received_at") or manifest.get("first_received_at") or _utc_now().isoformat(),
                         manifest.get("record_count") or 0, manifest.get("bytes"),
                         Json({"sha256": manifest["sha256"], "head_verified": True, "manifest_key": manifest["object_key"].removesuffix(".jsonl.gz") + ".manifest.json"})))
                db.commit()
        except Exception as exc:
            raise RuntimeError(f"AISStream archive manifest DB write failed: {exc}") from exc
        self.stats.s3_archives += 1
        self.stats.last_s3_success_at = _utc_now().isoformat()


def main() -> None:
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))
    worker = AISStreamWorker()
    signal.signal(signal.SIGTERM, lambda *_: worker.stop())
    signal.signal(signal.SIGINT, lambda *_: worker.stop())
    worker.run()


if __name__ == "__main__":
    main()
