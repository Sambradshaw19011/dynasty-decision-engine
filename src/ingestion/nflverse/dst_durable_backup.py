"""Upload and verify immutable D/ST database snapshots in Supabase Storage.

The snapshot must already pass local structural and SHA256 validation.
No production database rows or production PBP objects are modified.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import requests

from src.ingestion.nflverse.verify_dst_snapshot import verify_snapshot


def upload_snapshot(client, *, url: str, key: str, bucket: str,
                    snapshot_path: Path) -> dict:
    document = verify_snapshot(snapshot_path)
    season = document["season"]
    payload = snapshot_path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    object_path = f"dst_database_backups/{season}/{digest}.json"
    response = requests.post(
        f"{url.rstrip('/')}/storage/v1/object/{bucket}/{object_path}",
        headers={
            "apikey": key, "Authorization": f"Bearer {key}",
            "Content-Type": "application/json", "x-upsert": "false",
        },
        data=payload,
        timeout=600,
    )
    if not response.ok:
        try:
            error = str(response.json()).lower()
        except ValueError:
            error = ""
        duplicate = response.status_code in (400, 409) and any(
            word in error for word in ("duplicate", "already exists", "resourcealreadyexists")
        )
        if not duplicate:
            response.raise_for_status()
    stored = client.storage.from_(bucket).download(object_path)
    if hashlib.sha256(stored).hexdigest() != digest or stored != payload:
        raise RuntimeError("Durable database snapshot verification failed")
    return {"season": season, "path": object_path, "sha256": digest,
            "rows": {name: table["row_count"] for name, table in document["tables"].items()}}


def verify_remote_snapshot(client, *, bucket: str, season: int,
                           object_path: str, digest: str) -> dict:
    expected = f"dst_database_backups/{season}/{digest}.json"
    if object_path != expected:
        raise RuntimeError("Unexpected database backup path")
    payload = client.storage.from_(bucket).download(object_path)
    if hashlib.sha256(payload).hexdigest() != digest:
        raise RuntimeError("Remote database backup checksum mismatch")
    document = json.loads(payload)
    if document.get("season") != season:
        raise RuntimeError("Remote database backup season mismatch")
    return document
