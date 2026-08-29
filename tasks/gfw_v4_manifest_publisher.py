"""Schema-4 immutable release publisher.

The builder intentionally remains local-only.  This module is the narrow
boundary that may add S3 and service-role ledger side effects after every local
gate has passed.  Both clients are injected so tests never need credentials or
an external service.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable


SCHEMA_VERSION = 4
ROOT_KEY_PREFIX = "deploy-assets/global-maritime/gfw-hourly/v4"
ROOT_MANIFEST_KEY = f"{ROOT_KEY_PREFIX}/manifest.json"
ROOT_CACHE_CONTROL = "public,max-age=60,s-maxage=60,stale-while-revalidate=300"
RELEASE_CACHE_CONTROL = "public,max-age=604800,s-maxage=604800,immutable"
ROOT_CONTENT_TYPE = "application/json; charset=utf-8"
IDENTITY = "identity"
REQUIRED_ASSET_TYPES = frozenset(
    {
        "tracks_day_pmtiles",
        "track_frame_pmtiles",
        "track_detail_bucket",
        "grid_hour_pmtiles",
        "grid_detail_bucket",
        "fishing_effort_day",
    }
)
ALLOWED_ASSET_TYPES = REQUIRED_ASSET_TYPES | {"gear_observations"}
RELEASE_ID = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}__[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class V4ManifestPublishError(RuntimeError):
    """Raised before, or during, a schema-4 publication transition."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def slug_resolved_dataset_version(value: str) -> str:
    """Match migration 379's regexp_replace exactly; preserve provider truth."""
    return re.sub(r"[^A-Za-z0-9._-]+", "-", str(value))


def _safe_root_relative(value: Any, *, release_id: str) -> str:
    path = str(value or "")
    parsed = PurePosixPath(path)
    if (
        not path
        or parsed.is_absolute()
        or ".." in parsed.parts
        or "." in parsed.parts
        or "//" in path
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", path)
        or not path.startswith(f"releases/{release_id}/")
    ):
        raise V4ManifestPublishError(f"unsafe or non-v4 artifact path: {path}")
    return path


def _status_passed(value: Any) -> bool:
    return value is True or value == "passed"


def _validate_track_frame_pmtiles(asset: dict[str, Any], *, path: str) -> None:
    """Require the formal z6, no-drop spatial identity proof for one frame."""
    if asset.get("content_type") != "application/octet-stream" or asset.get("content_encoding") != IDENTITY:
        raise V4ManifestPublishError(f"track_frame_pmtiles headers must be octet-stream identity: {path}")
    counts = asset.get("semantic_counts")
    spatial = asset.get("spatial_contract")
    if not isinstance(counts, dict) or not isinstance(spatial, dict):
        raise V4ManifestPublishError(f"track_frame_pmtiles spatial contract is missing: {path}")
    if not counts.get("observed_at") or not counts.get("bucket"):
        raise V4ManifestPublishError(f"track_frame_pmtiles bucket/time identity is missing: {path}")
    if spatial.get("fixed_zoom") != 6:
        raise V4ManifestPublishError(f"track_frame_pmtiles must be fixed z6: {path}")
    source_count = spatial.get("source_feature_count")
    decoded_count = spatial.get("decoded_feature_count")
    if (
        not isinstance(source_count, int)
        or source_count < 0
        or not isinstance(decoded_count, int)
        or decoded_count != source_count
        or counts.get("feature_count") != source_count
        or spatial.get("identity_duplicate_count") != 0
        or spatial.get("identity_missing_count") != 0
    ):
        raise V4ManifestPublishError(f"track_frame_pmtiles identity/no-drop proof failed: {path}")


def _read_object_json(root: Path, relative: str) -> dict[str, Any]:
    path = root / relative
    if path.is_symlink() or not path.is_file():
        raise V4ManifestPublishError(f"manifest file is not a plain file: {relative}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise V4ManifestPublishError(f"invalid manifest JSON: {relative}") from exc
    if not isinstance(value, dict):
        raise V4ManifestPublishError(f"manifest JSON must be an object: {relative}")
    return value


def validate_v4_release_candidate(
    release_root: Path,
    *,
    tier2_evidence_id: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    """Validate all local gates before a client method can be called."""
    root = release_root.resolve()
    if root.is_symlink() or not root.is_dir():
        raise V4ManifestPublishError(f"release root is not a plain directory: {release_root}")
    root_manifest = _read_object_json(root, "manifest.json")
    release_pointer = root_manifest.get("release_manifest")
    if not isinstance(release_pointer, dict):
        raise V4ManifestPublishError("root manifest lacks release_manifest pointer")
    release_path = str(release_pointer.get("path") or "")
    parsed = PurePosixPath(release_path)
    if parsed.is_absolute() or ".." in parsed.parts or release_path != parsed.as_posix():
        raise V4ManifestPublishError("root release manifest path is unsafe")
    release_manifest = _read_object_json(root, release_path)
    release_id = release_manifest.get("release_id")
    if not isinstance(release_id, str) or not RELEASE_ID.fullmatch(release_id):
        raise V4ManifestPublishError("v4 release_id is invalid")
    if release_path != f"releases/{release_id}/manifest.json":
        raise V4ManifestPublishError("release manifest path does not match release_id")
    if root_manifest.get("release_id") != release_id:
        raise V4ManifestPublishError("root/release release_id mismatch")
    if release_manifest.get("schema_version") != SCHEMA_VERSION:
        raise V4ManifestPublishError("release manifest schema_version must be 4")
    selected_day = release_manifest.get("selected_utc_date")
    if not isinstance(selected_day, str) or release_id[:10] != selected_day:
        raise V4ManifestPublishError("release selected date/release_id mismatch")
    resolved = str(release_manifest.get("resolved_dataset_version") or "")
    if not resolved or release_id.split("__", 1)[1] != slug_resolved_dataset_version(resolved):
        raise V4ManifestPublishError("release_id suffix is not the resolved dataset slug")

    truth = release_manifest.get("release_truth")
    if not isinstance(truth, dict):
        raise V4ManifestPublishError("release_truth is required")
    if not all(_status_passed(truth.get(key)) for key in ("tier1_status", "tier2_status", "readback_status")):
        raise V4ManifestPublishError("Tier 1, Tier 2, and readback must all be passed")
    production_cutover = release_manifest.get("production_cutover")
    if not _status_passed(production_cutover) and not _status_passed(truth.get("production_cutover")):
        raise V4ManifestPublishError("production_cutover must be passed")
    if not tier2_evidence_id:
        raise V4ManifestPublishError("Tier 2 evidence ID is required")
    declared_evidence = release_manifest.get("tier2_evidence_id") or truth.get("tier2_evidence_id")
    if declared_evidence is not None and declared_evidence != tier2_evidence_id:
        raise V4ManifestPublishError("Tier 2 evidence ID mismatch")

    assets = release_manifest.get("artifacts")
    if not isinstance(assets, list) or not assets:
        raise V4ManifestPublishError("release artifacts must be a non-empty array")
    seen: set[str] = set()
    seen_types: set[str] = set()
    for asset in assets:
        if not isinstance(asset, dict) or asset.get("type") not in ALLOWED_ASSET_TYPES:
            raise V4ManifestPublishError("release contains an unsupported asset type")
        path = _safe_root_relative(asset.get("path"), release_id=release_id)
        if path in seen:
            raise V4ManifestPublishError(f"duplicate artifact path: {path}")
        seen.add(path)
        seen_types.add(str(asset["type"]))
        if not isinstance(asset.get("bytes"), int) or asset["bytes"] < 0:
            raise V4ManifestPublishError(f"invalid artifact bytes: {path}")
        if asset.get("content_length") != asset["bytes"]:
            raise V4ManifestPublishError(f"artifact content_length mismatch: {path}")
        sha = asset.get("sha256")
        if not isinstance(sha, str) or not SHA256.fullmatch(sha):
            raise V4ManifestPublishError(f"invalid artifact SHA: {path}")
        if asset.get("etag") != f'"{sha}"':
            raise V4ManifestPublishError(f"artifact ETag is not a quoted strong SHA: {path}")
        if not asset.get("content_type"):
            raise V4ManifestPublishError(f"artifact content_type is missing: {path}")
        if asset.get("content_encoding") not in {IDENTITY, "gzip"}:
            raise V4ManifestPublishError(f"artifact content_encoding is invalid: {path}")
        if asset.get("cache_control") != RELEASE_CACHE_CONTROL:
            raise V4ManifestPublishError(f"artifact cache_control is not immutable: {path}")
        if not isinstance(asset.get("semantic_counts"), dict):
            raise V4ManifestPublishError(f"artifact semantic_counts are missing: {path}")
        if asset["type"] == "track_frame_pmtiles":
            _validate_track_frame_pmtiles(asset, path=path)
        local = root / path
        if local.is_symlink() or not local.is_file():
            raise V4ManifestPublishError(f"artifact is not a plain local file: {path}")
        if local.stat().st_size != asset["bytes"] or _sha256_file(local) != sha:
            raise V4ManifestPublishError(f"local artifact hash/size mismatch: {path}")
    if not REQUIRED_ASSET_TYPES <= seen_types:
        raise V4ManifestPublishError(f"required v4 asset types missing: {sorted(REQUIRED_ASSET_TYPES - seen_types)}")
    if root_manifest.get("production_cutover") not in (True, "passed"):
        raise V4ManifestPublishError("root production_cutover must be passed")

    expected_release_sha = _sha256_file(root / release_path)
    expected_release_bytes = (root / release_path).stat().st_size
    if release_pointer.get("sha256") != expected_release_sha or release_pointer.get("bytes") != expected_release_bytes:
        raise V4ManifestPublishError("root release manifest hash/size pointer mismatch")
    return root_manifest, release_manifest, assets


def _head_value(head: dict[str, Any], name: str, default: Any = None) -> Any:
    if name in head:
        return head[name]
    lower = name.lower()
    return next((value for key, value in head.items() if str(key).lower() == lower), default)


def _put_and_verify(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
    content_encoding: str,
    cache_control: str,
    sha256: str,
) -> dict[str, Any]:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
        ContentEncoding=content_encoding,
        CacheControl=cache_control,
        Metadata={"sha256": sha256},
    )
    head = client.head_object(Bucket=bucket, Key=key)
    actual = {
        "bytes": int(_head_value(head, "ContentLength", -1)),
        "etag": str(_head_value(head, "ETag", "")),
        "content_type": str(_head_value(head, "ContentType", "")),
        "content_encoding": str(_head_value(head, "ContentEncoding", IDENTITY) or IDENTITY),
        "cache_control": str(_head_value(head, "CacheControl", "")),
    }
    expected = {
        "bytes": len(body), "etag": f'"{sha256}"',
        "content_type": content_type, "content_encoding": content_encoding,
        "cache_control": cache_control,
    }
    if actual != expected:
        raise V4ManifestPublishError(f"S3 HEAD contract mismatch for {key}: {actual}")
    return actual


def _validate_previous_entries(entries: Any, *, key_prefix: str) -> list[dict[str, Any]]:
    if entries is None:
        return []
    if not isinstance(entries, list):
        raise V4ManifestPublishError("published_releases must be an array")
    result = []
    for entry in entries:
        if not isinstance(entry, dict):
            raise V4ManifestPublishError("published release entry must be an object")
        release_id = str(entry.get("release_id") or "")
        if not RELEASE_ID.fullmatch(release_id):
            raise V4ManifestPublishError("previous release_id is invalid")
        expected_prefix = f"{key_prefix}/releases/{release_id}/"
        keys = entry.get("object_keys")
        if not isinstance(keys, list) or not keys:
            raise V4ManifestPublishError(f"previous release {release_id} lacks exact object_keys")
        normalized = []
        for key in keys:
            key = str(key)
            suffix = key.removeprefix(expected_prefix)
            if not key.startswith(expected_prefix) or not suffix or ".." in PurePosixPath(suffix).parts or "//" in suffix:
                raise V4ManifestPublishError(f"unsafe previous release key: {key}")
            normalized.append(key)
        if len(set(normalized)) != len(normalized):
            raise V4ManifestPublishError(f"previous release {release_id} repeats object keys")
        manifest_key = f"{expected_prefix}manifest.json"
        if entry.get("manifest_key") != manifest_key or manifest_key not in normalized:
            raise V4ManifestPublishError(f"previous release {release_id} has invalid manifest_key")
        result.append({**entry, "release_id": release_id, "manifest_key": manifest_key, "object_keys": normalized})
    return result


def _get_previous_root(client: Any, *, bucket: str, key: str) -> tuple[dict[str, Any] | None, bytes | None]:
    getter = getattr(client, "get_object", None)
    if getter is None:
        return None, None
    try:
        response = getter(Bucket=bucket, Key=key)
    except KeyError:
        # Small injected fakes commonly use a mapping for a missing object;
        # boto3 represents the same condition as a NoSuchKey client error.
        return None, None
    except Exception as exc:
        response_error = getattr(exc, "response", None) or {}
        code = response_error.get("Error", {}).get("Code") if isinstance(response_error, dict) else None
        if str(code) in {"404", "NoSuchKey", "NotFound"} or getattr(exc, "args", ()) == ("NoSuchKey",):
            return None, None
        raise
    body = response.get("Body")
    if body is None:
        raise V4ManifestPublishError("current root object has no body")
    raw = bytes(body.read())
    try:
        value = json.loads(raw)
    except ValueError as exc:
        raise V4ManifestPublishError("current root object is not JSON") from exc
    if not isinstance(value, dict):
        raise V4ManifestPublishError("current root object must be an object")
    return value, raw


def build_v4_ledger_payload(
    *,
    manifest: dict[str, Any],
    root_metadata: dict[str, Any],
    run_id: str,
    status: str,
    started_at: str,
    tier2_evidence_id: str,
    previous_release_id: str | None = None,
    request_summary: dict[str, Any] | None = None,
    retention_summary: dict[str, Any] | None = None,
    error_message: str | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    selected = str(manifest["selected_utc_date"])
    observed = now or datetime.now(timezone.utc).isoformat()
    passed = status == "succeeded"
    root_key = ROOT_MANIFEST_KEY
    release_id = str(manifest["release_id"])
    payload: dict[str, Any] = {
        "run_id": run_id,
        "release_id": release_id,
        "status": status,
        "started_at": started_at,
        "completed_at": observed if status != "running" else None,
        "generated_at": observed if status == "succeeded" else None,
        "published_at": observed if status == "succeeded" else None,
        "latest_complete_date": selected,
        # Migration 379 retains this legacy NOT-NULL success field; v4 does not
        # publish a SAR asset, but its layer separation remains explicit.
        "sar_latest_complete_date": selected,
        "date_start": selected,
        "date_end": selected,
        "manifest_schema_version": SCHEMA_VERSION,
        "root_manifest_key": root_key,
        "root_manifest_sha256": root_metadata.get("sha256") if passed else None,
        "root_manifest_bytes": root_metadata.get("bytes") if passed else None,
        "root_manifest_etag": root_metadata.get("etag") if passed else None,
        "root_manifest_content_type": root_metadata.get("content_type") if passed else None,
        "root_manifest_content_encoding": root_metadata.get("content_encoding", IDENTITY) if passed else IDENTITY,
        "root_manifest_cache_control": root_metadata.get("cache_control") if passed else None,
        "release_prefix": f"{ROOT_KEY_PREFIX}/releases/{release_id}/",
        "source_dataset_id": manifest.get("source_dataset_id"),
        "resolved_dataset_version": manifest.get("resolved_dataset_version"),
        "assets": manifest.get("artifacts", []),
        "manifest_summary": manifest,
        "request_summary": request_summary or {},
        "error_message": error_message,
        "readback_status": "passed" if passed else "not_run",
        "readback_checked_at": observed if passed else None,
        "tier1_status": "passed" if passed else "not_run",
        "tier2_status": "passed" if passed else "not_run",
        "tier2_summary": {"evidence_id": tier2_evidence_id} if passed else {},
        "retention_status": "passed" if passed else "not_checked",
        "retention_checked_at": observed if passed else None,
        "retention_summary": retention_summary or {},
        "previous_release_id": previous_release_id,
    }
    return payload


@dataclass
class V4ManifestPublisher:
    """DI publisher; ``s3_client`` and ``ledger`` are never constructed here."""

    s3_client: Any
    ledger: Any
    bucket: str
    release_root: Path
    tier2_evidence_id: str
    run_id: str | None = None
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    releases_to_keep: int = 2

    def __call__(self, _normalized_source: dict[str, Any] | None = None) -> dict[str, Any]:
        """Allow injection into the daily task's publisher callback.

        The built release is intentionally supplied at construction time; the
        normalized source argument is accepted only for callback compatibility
        and is not trusted as a replacement for the locally verified manifest.
        """
        del _normalized_source
        return self.publish()

    def _root_body(self, root_manifest: dict[str, Any], release_manifest: dict[str, Any], release_body: bytes, entries: list[dict[str, Any]]) -> bytes:
        value = deepcopy(root_manifest)
        release_id = str(release_manifest["release_id"])
        value.update({
            "schema_version": SCHEMA_VERSION,
            "immutable_release_contract": True,
            "release_id": release_id,
            "selected_utc_date": release_manifest["selected_utc_date"],
            "release_path": f"releases/{release_id}",
            "production_cutover": True,
            "release_manifest": {
                "path": f"releases/{release_id}/manifest.json",
                "bytes": len(release_body),
                "sha256": _sha256_bytes(release_body),
            },
            "published_releases": entries,
        })
        return _canonical_bytes(value)

    def publish(self) -> dict[str, Any]:
        if self.releases_to_keep < 2:
            raise V4ManifestPublishError("at least current and previous releases must be retained")
        # This is deliberately the first operation: no ledger/S3 method is
        # called for a POC, an unaccepted Tier 2 run, or an unsafe candidate.
        root_manifest, manifest, assets = validate_v4_release_candidate(
            self.release_root, tier2_evidence_id=self.tier2_evidence_id
        )
        release_id = str(manifest["release_id"])
        run_id = self.run_id or str(uuid.uuid4())
        started_at = self.now().astimezone(timezone.utc).isoformat()
        base_running = build_v4_ledger_payload(
            manifest=manifest, root_metadata={}, run_id=run_id, status="running",
            started_at=started_at, tier2_evidence_id=self.tier2_evidence_id,
        )
        self.ledger.write(base_running)
        root_key = ROOT_MANIFEST_KEY
        previous_root, previous_raw = _get_previous_root(self.s3_client, bucket=self.bucket, key=root_key)
        previous_entries = _validate_previous_entries(
            (previous_root or {}).get("published_releases") if previous_root else None,
            key_prefix=ROOT_KEY_PREFIX,
        )
        if any(entry["release_id"] == release_id for entry in previous_entries):
            raise V4ManifestPublishError(f"immutable release already recorded: {release_id}")

        release_body = (self.release_root / f"releases/{release_id}/manifest.json").read_bytes()
        release_key = f"{ROOT_KEY_PREFIX}/releases/{release_id}/manifest.json"
        asset_keys: list[str] = []
        for asset in assets:
            path = str(asset["path"])
            key = f"{ROOT_KEY_PREFIX}/{path}"
            body = (self.release_root / path).read_bytes()
            _put_and_verify(
                self.s3_client, bucket=self.bucket, key=key, body=body,
                content_type=str(asset["content_type"]), content_encoding=str(asset["content_encoding"]),
                cache_control=str(asset["cache_control"]), sha256=str(asset["sha256"]),
            )
            asset_keys.append(key)
        _put_and_verify(
            self.s3_client, bucket=self.bucket, key=release_key, body=release_body,
            content_type=ROOT_CONTENT_TYPE, content_encoding=IDENTITY,
            cache_control=RELEASE_CACHE_CONTROL, sha256=_sha256_bytes(release_body),
        )
        object_keys = [*asset_keys, release_key]
        new_entry = {
            "release_id": release_id,
            "manifest_key": release_key,
            "object_keys": object_keys,
            "manifest_sha256": _sha256_bytes(release_body),
            "manifest_bytes": len(release_body),
            "manifest_etag": f'"{_sha256_bytes(release_body)}"',
        }
        all_entries = [new_entry, *previous_entries]
        kept = all_entries[: self.releases_to_keep]
        retired = all_entries[self.releases_to_keep :]
        root_body = self._root_body(root_manifest, manifest, release_body, kept)
        root_sha = _sha256_bytes(root_body)
        try:
            root_meta = _put_and_verify(
                self.s3_client, bucket=self.bucket, key=root_key, body=root_body,
                content_type=ROOT_CONTENT_TYPE, content_encoding=IDENTITY,
                cache_control=ROOT_CACHE_CONTROL, sha256=root_sha,
            )
        except Exception:
            # Restore the previous reader-visible root if the final HEAD gate
            # fails.  Never guess or list old objects; the prior bytes are the
            # only safe rollback source.
            if previous_raw is not None:
                previous_sha = _sha256_bytes(previous_raw)
                _put_and_verify(
                    self.s3_client, bucket=self.bucket, key=root_key, body=previous_raw,
                    content_type=ROOT_CONTENT_TYPE, content_encoding=IDENTITY,
                    cache_control=ROOT_CACHE_CONTROL, sha256=previous_sha,
                )
            elif not previous_root:
                self.s3_client.delete_object(Bucket=self.bucket, Key=root_key)
            raise
        deleted: list[str] = []
        delete_warnings: list[dict[str, str]] = []
        for entry in retired:
            for key in entry["object_keys"]:
                try:
                    self.s3_client.delete_object(Bucket=self.bucket, Key=key)
                    deleted.append(key)
                except Exception as exc:
                    delete_warnings.append({"key": key, "error": str(exc)})
        finished_at = self.now().astimezone(timezone.utc).isoformat()
        retention_summary = {
            "kept_release_ids": [entry["release_id"] for entry in kept],
            "deleted_object_keys": deleted,
            "delete_warnings": delete_warnings,
        }
        succeeded = build_v4_ledger_payload(
            manifest=manifest, root_metadata=root_meta, run_id=run_id, status="succeeded",
            started_at=started_at, tier2_evidence_id=self.tier2_evidence_id,
            previous_release_id=kept[1]["release_id"] if len(kept) > 1 else None,
            request_summary=manifest.get("source_proof", {}).get("presence", {}).get("metrics"),
            retention_summary=retention_summary, now=finished_at,
        )
        # Reader-visible cutover is authoritative; if this final write fails,
        # leave reconciliation to the caller rather than writing a false
        # failed transition.
        self.ledger.write(succeeded)
        return {
            "run_id": run_id,
            "release_id": release_id,
            "root_manifest_key": root_key,
            "root_manifest_sha256": root_sha,
            "root_manifest_bytes": len(root_body),
            "root_manifest_etag": root_meta["etag"],
            "uploaded_object_keys": object_keys + [root_key],
            "deleted_object_keys": deleted,
            "delete_warnings": delete_warnings,
            "previous_root_present": previous_raw is not None,
            "ledger_payload": succeeded,
        }

    def rollback(self, *, current_root_manifest: dict[str, Any], target_release_id: str) -> dict[str, Any]:
        """Move only the root pointer to an exact manifest-enumerated release."""
        entries = _validate_previous_entries(
            current_root_manifest.get("published_releases"), key_prefix=ROOT_KEY_PREFIX
        )
        target = next((entry for entry in entries if entry["release_id"] == target_release_id), None)
        if target is None:
            raise V4ManifestPublishError("rollback target is not an enumerated immutable release")
        getter = getattr(self.s3_client, "get_object", None)
        if getter is None:
            raise V4ManifestPublishError("rollback requires injected S3 get_object")
        response = getter(Bucket=self.bucket, Key=target["manifest_key"])
        body = bytes(response["Body"].read())
        if _sha256_bytes(body) != target.get("manifest_sha256") or len(body) != target.get("manifest_bytes"):
            raise V4ManifestPublishError("rollback release manifest readback mismatch")
        release_manifest = json.loads(body)
        assets = release_manifest.get("artifacts")
        if not isinstance(assets, list) or not assets:
            raise V4ManifestPublishError("rollback release manifest has no artifacts")
        expected_keys = set(target["object_keys"])
        for asset in assets:
            if not isinstance(asset, dict):
                raise V4ManifestPublishError("rollback release contains an invalid artifact")
            path = _safe_root_relative(asset.get("path"), release_id=target_release_id)
            key = f"{ROOT_KEY_PREFIX}/{path}"
            if key not in expected_keys:
                raise V4ManifestPublishError(f"rollback release omits artifact key: {key}")
            head = self.s3_client.head_object(Bucket=self.bucket, Key=key)
            actual = {
                "bytes": int(_head_value(head, "ContentLength", -1)),
                "etag": str(_head_value(head, "ETag", "")),
                "content_type": str(_head_value(head, "ContentType", "")),
                "content_encoding": str(_head_value(head, "ContentEncoding", IDENTITY) or IDENTITY),
                "cache_control": str(_head_value(head, "CacheControl", "")),
            }
            expected = {
                "bytes": int(asset["bytes"]), "etag": str(asset["etag"]),
                "content_type": str(asset["content_type"]),
                "content_encoding": str(asset["content_encoding"]),
                "cache_control": str(asset["cache_control"]),
            }
            if actual != expected:
                raise V4ManifestPublishError(f"rollback asset readback mismatch: {path}")
        value = deepcopy(current_root_manifest)
        value.update({
            "release_id": target_release_id,
            "selected_utc_date": release_manifest["selected_utc_date"],
            "release_path": f"releases/{target_release_id}",
            "release_manifest": {
                "path": f"releases/{target_release_id}/manifest.json",
                "bytes": len(body), "sha256": _sha256_bytes(body),
            },
            "production_cutover": True,
        })
        root_body = _canonical_bytes(value)
        meta = _put_and_verify(
            self.s3_client, bucket=self.bucket, key=ROOT_MANIFEST_KEY, body=root_body,
            content_type=ROOT_CONTENT_TYPE, content_encoding=IDENTITY,
            cache_control=ROOT_CACHE_CONTROL, sha256=_sha256_bytes(root_body),
        )
        return {"target_release_id": target_release_id, "root_manifest_sha256": _sha256_bytes(root_body), "root_manifest_etag": meta["etag"]}
