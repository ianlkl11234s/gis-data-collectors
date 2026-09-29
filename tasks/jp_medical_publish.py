"""Transport for an exact reviewed payload; callers supply an authenticated client.

Importing this module does not create a client, read credentials, or perform I/O.
The research runner never invokes this function against a real service. Deployment
must explicitly approve the bucket and the complete hash-pinned payload first.
"""
from __future__ import annotations

import json
from pathlib import Path

from tasks.jp_medical_artifacts import (
    BundlePlan, CURRENT_CACHE, IMMUTABLE_CACHE, current_for,
    exact_upload_plan, sha256_bytes,
)


def publish_reviewed_payload(client, *, bucket: str, plan: BundlePlan,
                             payload_manifest: Path) -> dict:
    """Upload only enumerated files and promote current after every readback.

    There is deliberately no CLI, root scan, default bucket, or deletion here.
    An injectable client permits end-to-end contract tests without network access.
    """
    if not bucket or "/" in bucket:
        raise ValueError("An explicit bucket name is required")
    approved = exact_upload_plan(plan, payload_manifest)
    inventory = [{"path": relative.as_posix(), "sha256": sha256_bytes(source.read_bytes()), "bytes": source.stat().st_size} for source, relative in plan.artifacts]
    bundle_hash = sha256_bytes(json.dumps({"dataset": plan.dataset, "files": inventory}, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())
    if bundle_hash != plan.bundle_hash:
        raise ValueError("Reviewed public bundle changed")
    pointer = current_for(plan)
    operations = []
    for entry in approved["raw"]:
        operations.append((plan.raw_root / entry["path"], entry["bucket_key"],
                           entry["sha256"], entry["bytes"], "private,no-store"))
    for source, relative in plan.artifacts:
        item = pointer["files"][relative.as_posix()]
        operations.append((source, item["key"], item["sha256"], item["bytes"], IMMUTABLE_CACHE))
    # Validate the complete allowlist before the first write.
    for source, _, digest, size, _ in operations:
        data = source.read_bytes()
        if len(data) != size or sha256_bytes(data) != digest:
            raise ValueError(f"Reviewed payload changed: {source.name}")
    receipts = []

    def put_and_verify(key, data, cache):
        digest = sha256_bytes(data)
        content_type = ("application/x-ndjson" if key.endswith(".jsonl") else
                        "application/vnd.pmtiles" if key.endswith(".pmtiles") else
                        "application/json" if key.endswith((".json", ".geojson")) else
                        "application/octet-stream")
        client.put_object(Bucket=bucket, Key=key, Body=data, CacheControl=cache,
                          ContentType=content_type, Metadata={"sha256": digest})
        result = client.get_object(Bucket=bucket, Key=key)
        actual = result["Body"].read()
        if len(actual) != len(data) or sha256_bytes(actual) != digest:
            raise RuntimeError(f"Archive readback failed: {key}")
        if result.get("CacheControl") != cache:
            raise RuntimeError(f"Cache policy readback failed: {key}")
        receipts.append({"bucket": bucket, "key": key, "sha256": digest,
                         "bytes": len(data), "archive_verified": True})

    for source, key, digest, size, cache in operations:
        data = source.read_bytes()
        if len(data) != size or sha256_bytes(data) != digest:
            raise ValueError("Payload changed during publication; current retained")
        put_and_verify(key, data, cache)
    key = f"deploy-assets/medical/jp/{plan.dataset}/current.json"
    put_and_verify(key, json.dumps(pointer, ensure_ascii=False, sort_keys=True).encode(), CURRENT_CACHE)
    return {"dataset": plan.dataset, "receipts": receipts, "current_key": key}
