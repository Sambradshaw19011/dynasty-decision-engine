"""Concrete production Parquet promotion for the journal-controlled D/ST refresh.

Only call inside a verified RefreshJournal 'promoting' stage. The CLI
--execute gate in refresh_dst.py remains disabled.
"""
from __future__ import annotations

import hashlib
import requests

BUCKET = "nflverse-data"


def promote_verified_parquet(client, *, season: int, staged_digest: str,
                             backup_digest: str, url: str, key: str) -> None:
    """Promote only a previously verified staged object with a matching backup."""
    if not all(isinstance(d, str) and len(d) == 64 and
               all(c in "0123456789abcdef" for c in d)
               for d in (staged_digest, backup_digest)):
        raise RuntimeError("Invalid recorded Parquet SHA256")
    storage = client.storage.from_(BUCKET)
    staging_path = f"play_by_play_staging/{season}/{staged_digest}.parquet"
    backup_path = f"play_by_play_backups/{season}/{backup_digest}.parquet"
    production_path = f"play_by_play/{season}.parquet"

    staged = storage.download(staging_path)
    backup = storage.download(backup_path)
    current = storage.download(production_path)
    sha = lambda value: hashlib.sha256(value).hexdigest()
    if sha(staged) != staged_digest:
        raise RuntimeError("Staged Parquet checksum mismatch before promotion")
    if sha(backup) != backup_digest:
        raise RuntimeError("Backup Parquet checksum mismatch before promotion")
    if sha(current) != backup_digest:
        raise RuntimeError("Production Parquet changed before promotion; refusing overwrite")

    response = requests.post(
        f"{url.rstrip('/')}/storage/v1/object/{BUCKET}/{production_path}",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/octet-stream", "x-upsert": "true"},
        data=staged, timeout=600,
    )
    response.raise_for_status()
    promoted = storage.download(production_path)
    if sha(promoted) != staged_digest:
        raise RuntimeError("Production Parquet promotion checksum mismatch")
