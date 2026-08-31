"""Bounded, internal-only RIPE RIS Live WebSocket worker.

The worker never connects to an unfiltered firehose.  It requires an approved
versioned prefix/ASN roster, one Zeabur replica, a process-local lock, a DB
writer, and private S3 archive access before opening the socket.  Raw JSON is
durably spooled before aggregation.  Any connection gap makes the entire
five-minute window partial and all canonical values NULL/unknown.
"""
from __future__ import annotations

import fcntl
import gzip
import hashlib
import ipaddress
import json
import logging
import random
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import config
from collectors.ripe_atlas_internet_health import load_ripe_roster
from collectors.internet_health import _iso, _source_run

try:  # disabled deployments must still import without the optional runtime
    from websockets.sync.client import connect as websocket_connect
except ImportError:  # pragma: no cover
    websocket_connect = None


logger = logging.getLogger(__name__)
UTC = timezone.utc
SOURCE = "ripe_ris_live"
EVIDENCE_FAMILY = "ripe_ris"
SCHEMA_VERSION = "ripe_ris_live.raw.v1"
WINDOW_SECONDS = 300


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _canonical_hash(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _window_start(now: datetime) -> datetime:
    epoch = int(now.timestamp()) // WINDOW_SECONDS * WINDOW_SECONDS
    return datetime.fromtimestamp(epoch, UTC)


def _network(value: str) -> ipaddress.IPv4Network | ipaddress.IPv6Network | None:
    try:
        return ipaddress.ip_network(value, strict=True)
    except ValueError:
        return None


def _ris_roster(roster: dict[str, Any]) -> dict[str, Any]:
    section = roster.get("ripe_ris_live")
    if not isinstance(section, dict):
        raise ValueError("RIPE RIS Live roster section is missing")
    raw_prefixes = section.get("prefixes") or []
    raw_asns = section.get("origin_asns") or []
    if not isinstance(raw_prefixes, list) or not isinstance(raw_asns, list):
        raise ValueError("RIPE RIS prefix/ASN roster must be arrays")
    if len(raw_prefixes) > 256 or len(raw_asns) > 64:
        raise ValueError("RIPE RIS roster exceeds the bounded safe limit")
    prefixes: list[str] = []
    for raw in raw_prefixes:
        network = _network(str(raw))
        if network is None:
            raise ValueError(f"RIPE RIS roster prefix is invalid: {raw}")
        canonical = str(network)
        if canonical not in prefixes:
            prefixes.append(canonical)
    asns: list[int] = []
    for raw in raw_asns:
        try:
            asn = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("RIPE RIS origin ASN is invalid") from exc
        if not 1 <= asn <= 4_294_967_295:
            raise ValueError("RIPE RIS origin ASN is outside the valid range")
        if asn not in asns:
            asns.append(asn)
    if not prefixes and not asns:
        raise ValueError("RIPE RIS reviewed prefix/ASN roster is empty")
    return {
        "prefixes": prefixes,
        "origin_asns": asns,
        "more_specific": bool(section.get("more_specific", True)),
        "less_specific": bool(section.get("less_specific", False)),
    }


def _subscription_specs(roster: dict[str, Any]) -> list[dict[str, Any]]:
    """Create bounded subscriptions; prefixes take precedence over ASN paths.

    When both are present, origin_asns is a local announcement allowlist.  This
    avoids duplicate delivery from overlapping prefix and path subscriptions.
    """
    specs: list[dict[str, Any]] = []
    for prefix in roster["prefixes"]:
        specs.append({
            "type": "UPDATE",
            "prefix": prefix,
            "moreSpecific": roster["more_specific"],
            "lessSpecific": roster["less_specific"],
        })
    if not specs:
        for asn in roster["origin_asns"]:
            specs.append({"type": "UPDATE", "path": f"{asn}$"})
    return specs


@dataclass
class WindowState:
    start: datetime
    end: datetime
    gap: bool
    gap_reasons: list[str] = field(default_factory=list)
    messages: int = 0
    rejected: int = 0
    duplicate_messages: int = 0
    seen_message_ids: set[tuple[str, str, str]] = field(default_factory=set)
    withdrawn_prefixes: dict[int, set[str]] = field(default_factory=lambda: {4: set(), 6: set()})
    origin_changes: dict[int, int] = field(default_factory=lambda: {4: 0, 6: 0})
    last_provider_at: str | None = None


@dataclass
class WorkerStats:
    messages_received: int = 0
    reconnects: int = 0
    archives_verified: int = 0
    windows_written: int = 0
    last_error: str | None = None


class RipeRisSpoolManager:
    """Durable gzip NDJSON spool with S3 object and manifest readback."""

    def __init__(self, base_dir: Path, s3_storage, *, subscription_hash: str):
        self.base_dir = base_dir / "ripe_ris_live_spool"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.s3 = s3_storage
        self.subscription_hash = subscription_hash
        self._path: Path | None = None
        self._file = None
        self._opened_at: datetime | None = None
        self._count = 0
        self._first_received: str | None = None
        self._last_received: str | None = None

    def append(self, envelope: dict[str, Any], received_at: str) -> None:
        if self._file is None:
            self._opened_at = _utc_now()
            stamp = self._opened_at.strftime("%Y%m%dT%H%M%SZ")
            self._path = self.base_dir / f"ripe-ris-{stamp}-{uuid.uuid4().hex[:10]}.jsonl"
            self._file = self._path.open("a", encoding="utf-8")
            self._count = 0
            self._first_received = received_at
        record = {"schema_version": SCHEMA_VERSION, "received_at": received_at, "message": envelope}
        self._file.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self._file.flush()
        self._count += 1
        self._last_received = received_at

    def should_rotate(self) -> bool:
        return bool(
            self._file and self._opened_at
            and (_utc_now() - self._opened_at).total_seconds() >= config.RIPE_RIS_SPOOL_ROTATE_MINUTES * 60
        )

    def exceeds_limit(self) -> bool:
        total = sum(path.stat().st_size for path in self.base_dir.glob("*") if path.is_file())
        return total >= config.RIPE_RIS_SPOOL_MAX_MB * 1024 * 1024

    def rotate(self) -> bool:
        if not self._file or not self._path:
            return False
        self._file.flush()
        self._file.close()
        source = self._path
        gz_path = source.with_suffix(source.suffix + ".gz")
        with source.open("rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            while chunk := src.read(1024 * 1024):
                dst.write(chunk)
        source.unlink(missing_ok=True)
        uploaded = self._upload(
            gz_path,
            count=self._count,
            first=self._first_received,
            last=self._last_received,
        )
        self._path = None
        self._file = None
        self._opened_at = None
        self._count = 0
        self._first_received = None
        self._last_received = None
        return uploaded

    def retry_pending(self) -> int:
        uploaded = 0
        for path in sorted(self.base_dir.glob("*.jsonl.gz")):
            if self._upload(path):
                uploaded += 1
        return uploaded

    def close(self) -> bool:
        return self.rotate() if self._file else False

    def _upload(
        self,
        path: Path,
        *,
        count: int | None = None,
        first: str | None = None,
        last: str | None = None,
    ) -> bool:
        if self.s3 is None or not path.exists():
            return False
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
        key = (
            f"{config.RIPE_RIS_S3_PREFIX}/date={modified:%Y-%m-%d}/"
            f"hour={modified:%H}/{path.name}"
        )
        try:
            self.s3.s3.head_object(Bucket=self.s3.bucket, Key=key)
        except self.s3.ClientError:
            try:
                self.s3.s3.upload_file(
                    str(path), self.s3.bucket, key,
                    ExtraArgs={
                        "ContentType": "application/x-ndjson",
                        "ContentEncoding": "gzip",
                        "StorageClass": config.RIPE_RIS_S3_STORAGE_CLASS,
                        "Metadata": {"sha256": digest, "schema-version": SCHEMA_VERSION},
                    },
                )
            except Exception as exc:
                logger.warning("RIPE RIS S3 upload failed: %s", exc)
                return False
        try:
            head = self.s3.s3.head_object(Bucket=self.s3.bucket, Key=key)
            metadata = {str(k).lower(): str(v) for k, v in (head.get("Metadata") or {}).items()}
            if int(head.get("ContentLength", -1)) != path.stat().st_size or metadata.get("sha256") != digest:
                return False
            manifest = {
                "schema_version": SCHEMA_VERSION,
                "object_key": key,
                "sha256": digest,
                "bytes": path.stat().st_size,
                "record_count": count,
                "first_received_at": first,
                "last_received_at": last,
                "subscription_sha256": self.subscription_hash,
                "visibility": "private_internal_only",
            }
            manifest_key = key.removesuffix(".jsonl.gz") + ".manifest.json"
            body = json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8")
            self.s3.s3.put_object(
                Bucket=self.s3.bucket,
                Key=manifest_key,
                Body=body,
                ContentType="application/json",
                StorageClass=config.RIPE_RIS_S3_STORAGE_CLASS,
                Metadata={"sha256": digest, "schema-version": SCHEMA_VERSION},
            )
            manifest_head = self.s3.s3.head_object(Bucket=self.s3.bucket, Key=manifest_key)
            if int(manifest_head.get("ContentLength", -1)) <= 0:
                return False
            persisted = self.s3.s3.get_object(Bucket=self.s3.bucket, Key=manifest_key)["Body"].read()
            readback = json.loads(persisted.decode("utf-8"))
            if readback.get("sha256") != digest or int(readback.get("bytes", -1)) != path.stat().st_size:
                return False
            path.unlink(missing_ok=True)
            return True
        except Exception as exc:
            logger.warning("RIPE RIS S3 manifest/readback failed: %s", exc)
            return False


class RipeRisLiveWorker:
    def __init__(self, *, writer=None, s3_storage=None, connect_factory=None, now_fn=None):
        self.stop_event = threading.Event()
        self.stats = WorkerStats()
        self._now = now_fn or _utc_now
        self._connect = connect_factory or websocket_connect
        self._writer = writer
        if self._writer is None and config.SUPABASE_ENABLED and config.SUPABASE_DB_URL:
            from collectors.base import get_supabase_writer
            self._writer = get_supabase_writer()
        self._s3 = s3_storage
        if self._s3 is None and config.S3_BUCKET:
            from storage.s3 import S3Storage
            self._s3 = S3Storage()
        self._roster: dict[str, Any] | None = None
        self._subscriptions: list[dict[str, Any]] = []
        self._subscription_hash = ""
        self.spool: RipeRisSpoolManager | None = None
        self._prepared = False
        self._connected = False
        self._acked = False
        self._lock_file = None
        start = _window_start(self._now())
        self._window = WindowState(start=start, end=start + timedelta(seconds=WINDOW_SECONDS), gap=True, gap_reasons=["startup_partial"])
        self._origin_by_peer_prefix: dict[tuple[str, str, str], int] = {}

    def preflight(self) -> None:
        if config.RIPE_RIS_REPLICA_COUNT != 1:
            raise RuntimeError("RIPE RIS Live requires RIPE_RIS_REPLICA_COUNT=1; distributed lease is not supported")
        if self._connect is None:
            raise RuntimeError("RIPE RIS Live requires websockets>=16")
        if self._writer is None:
            raise RuntimeError("RIPE RIS Live requires a Supabase writer for source_run/observation ledger")
        if self._s3 is None:
            raise RuntimeError("RIPE RIS Live requires private S3 durable archive")
        roster = load_ripe_roster()
        self._roster = _ris_roster(roster)
        self._subscriptions = _subscription_specs(self._roster)
        self._subscription_hash = _canonical_hash({
            "schema_version": roster["schema_version"],
            "version": roster.get("version"),
            "subscriptions": self._subscriptions,
            "origin_asns": self._roster["origin_asns"],
        })

    def _acquire_process_lock(self) -> None:
        lock_path = config.LOCAL_DATA_DIR / "ripe_ris_live.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = lock_path.open("a+")
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._lock_file.close()
            self._lock_file = None
            raise RuntimeError("another RIPE RIS Live worker holds the process-local lock") from exc

    def prepare(self) -> None:
        if self._prepared:
            return
        self.preflight()
        self._acquire_process_lock()
        self.spool = RipeRisSpoolManager(
            config.LOCAL_DATA_DIR,
            self._s3,
            subscription_hash=self._subscription_hash,
        )
        self.stats.archives_verified += self.spool.retry_pending()
        self._prepared = True

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:
        self.prepare()
        delay = 1.0
        try:
            while not self.stop_event.is_set():
                connected_at = time.monotonic()
                try:
                    self._connect_and_consume()
                except Exception as exc:
                    self.stats.last_error = str(exc)[:300]
                    self.stats.reconnects += 1
                    self._mark_gap(type(exc).__name__)
                    self._connected = False
                    self._acked = False
                    if self.stop_event.wait(random.uniform(0, delay)):
                        break
                    if time.monotonic() - connected_at >= WINDOW_SECONDS:
                        delay = 1.0
                    else:
                        delay = min(config.RIPE_RIS_RECONNECT_MAX_SECONDS, delay * 2)
        finally:
            # Do not publish an unfinished bucket with a future observed_at.
            if self.spool and self.spool.close():
                self.stats.archives_verified += 1
            if self._lock_file:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
                self._lock_file = None

    def _connect_and_consume(self) -> None:
        ws = self._connect(
            config.RIPE_RIS_LIVE_WS_URL,
            open_timeout=max(5, config.REQUEST_TIMEOUT),
            compression="deflate",
        )
        expected_acks = {
            json.dumps(spec, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            for spec in self._subscriptions
        }
        received_acks: set[str] = set()
        last_server = time.monotonic()
        last_ping = last_server
        awaiting_pong_since: float | None = None
        try:
            for spec in self._subscriptions:
                data = dict(spec)
                data["socketOptions"] = {"includeRaw": False, "acknowledge": True}
                ws.send(json.dumps({"type": "ris_subscribe", "data": data}, separators=(",", ":")))
            while not self.stop_event.is_set():
                self._roll_windows(self._now())
                try:
                    payload = ws.recv(timeout=1.0)
                except TimeoutError:
                    payload = None
                now_mono = time.monotonic()
                if payload is not None:
                    if isinstance(payload, bytes):
                        payload = payload.decode("utf-8")
                    envelope = json.loads(payload)
                    if not isinstance(envelope, dict):
                        raise ValueError("RIS Live envelope must be an object")
                    last_server = now_mono
                    message_type = envelope.get("type")
                    if message_type == "ris_error":
                        raise ConnectionError("RIS Live returned ris_error")
                    if message_type == "ris_subscribe_ok":
                        data = envelope.get("data") or {}
                        subscription = data.get("subscription")
                        if not isinstance(subscription, dict):
                            raise ValueError("RIS Live subscribe ack is malformed")
                        ack_key = json.dumps(subscription, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                        if ack_key in expected_acks:
                            received_acks.add(ack_key)
                        if received_acks == expected_acks:
                            self._connected = True
                            self._acked = True
                    elif message_type == "pong":
                        awaiting_pong_since = None
                    elif message_type == "ris_message":
                        if not self._acked:
                            raise ConnectionError("RIS Live message arrived before all subscriptions were acknowledged")
                        received_at = self._now().isoformat()
                        assert self.spool is not None
                        self.spool.append(envelope, received_at)
                        self.stats.messages_received += 1
                        self._aggregate(envelope, received_at)
                        if self.spool.exceeds_limit():
                            raise RuntimeError("RIPE RIS local spool exceeds hard limit")
                        if self.spool.should_rotate() and self.spool.rotate():
                            self.stats.archives_verified += 1
                if self._acked and awaiting_pong_since is None and now_mono - last_ping >= config.RIPE_RIS_PING_INTERVAL_SECONDS:
                    ws.send(json.dumps({"type": "ping", "data": None}, separators=(",", ":")))
                    last_ping = now_mono
                    awaiting_pong_since = now_mono
                if awaiting_pong_since is not None and now_mono - awaiting_pong_since > config.RIPE_RIS_PONG_TIMEOUT_SECONDS:
                    raise TimeoutError("RIS Live pong timeout")
                if now_mono - last_server > config.RIPE_RIS_IDLE_TIMEOUT_SECONDS:
                    raise TimeoutError("RIS Live idle timeout")
        finally:
            try:
                ws.close()
            except Exception:
                pass

    def _mark_gap(self, reason: str) -> None:
        self._window.gap = True
        if reason not in self._window.gap_reasons:
            self._window.gap_reasons.append(reason[:80])

    def _roll_windows(self, now: datetime) -> None:
        while now >= self._window.end:
            self._flush_window(self._window)
            start = self._window.end
            complete_transport = self._connected and self._acked
            self._window = WindowState(
                start=start,
                end=start + timedelta(seconds=WINDOW_SECONDS),
                gap=not complete_transport,
                gap_reasons=[] if complete_transport else ["not_connected_at_window_start"],
            )

    def _tracked_prefix(self, prefix: str) -> bool:
        assert self._roster is not None
        candidate = _network(prefix)
        if candidate is None:
            return False
        if not self._roster["prefixes"]:
            return True
        for raw in self._roster["prefixes"]:
            monitored = _network(raw)
            if monitored is None or monitored.version != candidate.version:
                continue
            if candidate == monitored:
                return True
            if self._roster["more_specific"] and candidate.subnet_of(monitored):
                return True
            if self._roster["less_specific"] and monitored.subnet_of(candidate):
                return True
        return False

    def _aggregate(self, envelope: dict[str, Any], received_at: str) -> None:
        data = envelope.get("data")
        if not isinstance(data, dict) or data.get("type") != "UPDATE":
            self._window.rejected += 1
            return
        message_id = str(data.get("id") or "")
        host = str(data.get("host") or "")
        peer = str(data.get("peer") or "")
        if not message_id or not host or not peer:
            self._window.rejected += 1
            return
        identity = (host, peer, message_id)
        if identity in self._window.seen_message_ids:
            self._window.duplicate_messages += 1
            return
        self._window.seen_message_ids.add(identity)
        observed_at = _iso(data.get("timestamp"))
        if observed_at and (self._window.last_provider_at is None or observed_at > self._window.last_provider_at):
            self._window.last_provider_at = observed_at
        self._window.messages += 1

        withdrawals = data.get("withdrawals") or []
        if isinstance(withdrawals, list):
            for raw_prefix in withdrawals:
                prefix = str(raw_prefix)
                network = _network(prefix)
                if network and self._tracked_prefix(prefix):
                    self._window.withdrawn_prefixes[network.version].add(prefix)

        path = data.get("path")
        origin_asn = path[-1] if isinstance(path, list) and path and isinstance(path[-1], int) else None
        assert self._roster is not None
        if self._roster["origin_asns"] and origin_asn is not None and origin_asn not in self._roster["origin_asns"]:
            return
        announcements = data.get("announcements") or []
        if not isinstance(announcements, list):
            self._window.rejected += 1
            return
        for announcement in announcements:
            if not isinstance(announcement, dict):
                self._window.rejected += 1
                continue
            for raw_prefix in announcement.get("prefixes") or []:
                prefix = str(raw_prefix)
                network = _network(prefix)
                if not network or not self._tracked_prefix(prefix) or origin_asn is None:
                    continue
                key = (host, peer, prefix)
                prior = self._origin_by_peer_prefix.get(key)
                if prior is not None and prior != origin_asn:
                    self._window.origin_changes[network.version] += 1
                self._origin_by_peer_prefix[key] = origin_asn

    def _flush_window(self, window: WindowState) -> None:
        assert self._roster is not None
        # A failed DB flush may retry the same closed window after reconnect.
        # Stable UUID keeps that retry idempotent in the source-run ledger.
        run_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{SOURCE}:{self._subscription_hash}:{window.start.isoformat()}",
        ))
        collected_at = self._now().isoformat()
        observations: list[dict[str, Any]] = []
        configured_counts = {4: 0, 6: 0}
        for prefix in self._roster["prefixes"]:
            network = _network(prefix)
            if network:
                configured_counts[network.version] += 1
        address_families = [af for af in (4, 6) if configured_counts[af] > 0]
        if not address_families:  # ASN-only roster may observe both families.
            address_families = [4, 6]
        for af in address_families:
            denominator = configured_counts[af]
            complete = not window.gap
            values = {
                f"prefix_visibility_ratio_ipv{af}": None,  # no RIB snapshot: state intentionally unknown
                f"withdrawn_prefix_ratio_ipv{af}": (
                    len(window.withdrawn_prefixes[af]) / denominator
                    if complete and denominator > 0 else None
                ),
                f"origin_change_count_ipv{af}": float(window.origin_changes[af]) if complete else None,
            }
            for signal, value in values.items():
                flags: dict[str, bool] = {}
                if value is None:
                    flags["missing_value"] = True
                if window.gap:
                    flags["stream_gap"] = True
                if signal.startswith("prefix_visibility"):
                    flags["state_uninitialized"] = True
                observations.append({
                    "_type": "observation",
                    "run_id": run_id,
                    "source": SOURCE,
                    "evidence_family": EVIDENCE_FAMILY,
                    "source_observation_id": f"ripe_ris_live:{signal}:TW:{window.end.isoformat()}",
                    "entity_type": "country",
                    "entity_id": "TW",
                    "entity_name": "Taiwan",
                    "signal": signal,
                    "observed_at": window.end.isoformat(),
                    "window_start": window.start.isoformat(),
                    "window_end": window.end.isoformat(),
                    "value": value,
                    "unit": "count" if "count" in signal else "ratio",
                    "baseline_value": None,
                    "change_ratio": None,
                    "reported_status": "unknown",
                    "incident_kind": None,
                    "confidence": None,
                    "sample_count": window.messages,
                    "stale_after_seconds": config.RIPE_RIS_STALE_AFTER_SECONDS,
                    # Do not fabricate a provider timestamp merely because the
                    # transport produced a pong and no matching BGP UPDATE.
                    "source_updated_at": window.last_provider_at,
                    "collected_at": collected_at,
                    "quality_flags": flags,
                    "metadata": {
                        "address_family": af,
                        "configured_prefix_count": denominator,
                        "subscription_sha256": self._subscription_hash,
                        "independence_group": "ripe_ncc",
                        "scope": "single_provider_family",
                        "public_visibility": "internal_only",
                        "composite_detector": "deferred",
                    },
                })
        status = "partial" if window.gap else "succeeded"
        error_code = "stream_gap" if window.gap else ("empty" if window.messages == 0 else None)
        run = _source_run(
            run_id=run_id,
            source=SOURCE,
            started_at=window.start.isoformat(),
            finished_at=collected_at,
            status=status,
            requested_from=window.start.isoformat(),
            requested_to=window.end.isoformat(),
            source_updated_at=window.last_provider_at,
            received=window.messages,
            written=len(observations),
            rejected=window.rejected,
            error_code=error_code,
            error_message=None,
            metadata={
                "subscription_sha256": self._subscription_hash,
                "gap": window.gap,
                "gap_reasons": window.gap_reasons,
                "duplicate_messages": window.duplicate_messages,
                "reconnects_total": self.stats.reconnects,
                "replica_gate": 1,
                "lease_scope": "process_local_only",
                "raw_archive_prefix": config.RIPE_RIS_S3_PREFIX,
                "public_visibility": "internal_only",
                "independence_group": "ripe_ncc",
            },
        )
        result = {
            "data": [run, *observations],
            "run_id": run_id,
            "run_status": status,
            "observation_count": len(observations),
        }
        if not self._writer.write(SOURCE, result, self._now()):
            raise RuntimeError("RIPE RIS canonical DB write failed; raw remains in durable spool/S3")
        self.stats.windows_written += 1


def main() -> None:
    logging.basicConfig(level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO))
    worker = RipeRisLiveWorker()
    signal.signal(signal.SIGTERM, lambda *_: worker.stop())
    signal.signal(signal.SIGINT, lambda *_: worker.stop())
    worker.run()


if __name__ == "__main__":
    main()
