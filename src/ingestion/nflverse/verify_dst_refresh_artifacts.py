"""Read-only preflight for all three immutable D/ST refresh artifacts.

Checks staged PBP, backed-up production PBP, and database backup before
creating a refresh journal operation. Does not change any production data.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.dst_durable_backup import verify_remote_snapshot
from src.ingestion.nflverse.refresh_dst import BUCKET, required


def verify_artifacts(client, *, season: int, staged_digest: str,
                     backup_parquet_digest: str, backup_database_digest: str):
    for digest in (staged_digest, backup_parquet_digest, backup_database_digest):
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise RuntimeError("All digests must be lowercase SHA256 hex")
    paths = {
        "staging": f"play_by_play_staging/{season}/{staged_digest}.parquet",
        "backup_parquet": f"play_by_play_backups/{season}/{backup_parquet_digest}.parquet",
        "production": f"play_by_play/{season}.parquet",
        "backup_database": f"dst_database_backups/{season}/{backup_database_digest}.json",
    }
    for key, expected in (("staging", staged_digest),
                          ("backup_parquet", backup_parquet_digest)):
        payload = client.storage.from_(BUCKET).download(paths[key])
        if hashlib.sha256(payload).hexdigest() != expected:
            raise RuntimeError(f"Artifact checksum mismatch: {key}")
    current = client.storage.from_(BUCKET).download(paths["production"])
    current_digest = hashlib.sha256(current).hexdigest()
    if current_digest not in (staged_digest, backup_parquet_digest):
        raise RuntimeError("Artifact checksum mismatch: production")
    snapshot = verify_remote_snapshot(
        client, bucket=BUCKET, season=season,
        object_path=paths["backup_database"], digest=backup_database_digest,
    )
    return {"season": season, "paths": paths, "snapshot_counts": {
        name: table["row_count"] for name, table in snapshot["tables"].items()
    }}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--staged-sha256", required=True)
    parser.add_argument("--backup-parquet-sha256", required=True)
    parser.add_argument("--backup-database-sha256", required=True)
    args = parser.parse_args()
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env")
    client = create_client(required("SUPABASE_URL"), required("SUPABASE_SERVICE_ROLE_KEY"))
    result = verify_artifacts(
        client, season=args.season, staged_digest=args.staged_sha256,
        backup_parquet_digest=args.backup_parquet_sha256,
        backup_database_digest=args.backup_database_sha256,
    )
    print(json.dumps(result, indent=2))
    print("ARTIFACT PREFLIGHT PASS: no production changes or journal writes")


if __name__ == "__main__":
    main()
