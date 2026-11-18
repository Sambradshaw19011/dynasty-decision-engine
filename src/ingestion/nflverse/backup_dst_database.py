"""Create and upload a durable current-season D/ST database backup.

This command reads production tables and writes ONLY an immutable backup
object to Supabase Storage. It never modifies production tables or PBP.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.dst_durable_backup import upload_snapshot, verify_remote_snapshot
from src.ingestion.nflverse.refresh_dst import BUCKET, all_rows, required
from src.ingestion.nflverse.verify_dst_snapshot import verify_snapshot

TABLES = ("fact_team_fumble_events", "fact_calculated_team_weekly_scores")


def create_snapshot(client, season: int, destination: Path) -> dict:
    snapshot = {"season": season, "created_at": datetime.now(timezone.utc).isoformat(),
                "tables": {}}
    for table in TABLES:
        rows = all_rows(client, table, "*", {"season_id": season})
        rows.sort(key=lambda row: json.dumps(row, sort_keys=True, default=str))
        snapshot["tables"][table] = {
            "season_column": "season_id", "row_count": len(rows), "rows": rows,
        }
    payload = json.dumps(snapshot, sort_keys=True, indent=2, ensure_ascii=False,
                         default=str).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / f"{digest}.json"
    if path.exists() and path.read_bytes() != payload:
        raise RuntimeError("Existing snapshot filename collision or corruption")
    if not path.exists():
        with path.open("xb") as out:
            out.write(payload)
    verify_snapshot(path)
    return {"path": path, "sha256": digest, "season": season}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, help="Season; defaults to current dim_season")
    parser.add_argument("--upload", action="store_true",
                        help="Upload verified immutable snapshot to Supabase Storage")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    load_dotenv(dotenv_path=root / ".env", override=False)
    url = required("SUPABASE_URL").rstrip("/")
    key = required("SUPABASE_SERVICE_ROLE_KEY")
    client = create_client(url, key)
    if args.season is None:
        seasons = all_rows(client, "dim_season", "season_id", {"is_current": True})
        if len(seasons) != 1:
            raise RuntimeError("Expected exactly one current season")
        season = int(seasons[0]["season_id"])
    else:
        season = args.season
    if not 2000 <= season <= 2100:
        raise RuntimeError("Invalid season")
    info = create_snapshot(client, season, root / "backups" / "dst_database" / str(season))
    print(f"LOCAL SNAPSHOT VERIFIED: {info['path']}")
    if not args.upload:
        print("READ ONLY: no Supabase objects or production rows modified")
        return
    remote = upload_snapshot(client, url=url, key=key, bucket=BUCKET,
                             snapshot_path=info["path"])
    verify_remote_snapshot(client, bucket=BUCKET, season=season,
                           object_path=remote["path"], digest=remote["sha256"])
    print(json.dumps(remote, indent=2))
    print("DURABLE BACKUP VERIFIED: immutable storage object created/verified; production data unchanged")


if __name__ == "__main__":
    main()
