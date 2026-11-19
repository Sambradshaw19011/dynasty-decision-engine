"""Scheduled D/ST ingestion using the existing audited, journaled production path."""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.refresh_dst import BUCKET, all_rows, required
from src.ingestion.nflverse.backup_dst_database import create_snapshot
from src.ingestion.nflverse.dst_durable_backup import upload_snapshot

ROOT = Path(__file__).resolve().parents[3]


def run(*args: str) -> None:
    subprocess.run(
        [sys.executable, "-m", "src.ingestion.nflverse.refresh_dst", *args],
        cwd=ROOT, check=True,
    )


def main() -> None:
    load_dotenv(ROOT / ".env", override=False)
    url = required("SUPABASE_URL").rstrip("/")
    key = required("SUPABASE_SERVICE_ROLE_KEY")
    required("PBP_INSPECT_SECRET")
    required("DST_SCORING_SETTINGS_ID")
    client = create_client(url, key)
    seasons = all_rows(client, "dim_season", "season_id", {"is_current": True})
    if len(seasons) != 1:
        raise RuntimeError("Exactly one current season required")
    season = int(seasons[0]["season_id"])
    unfinished = (
        client.table("dst_refresh_operations")
        .select("operation_id,status")
        .eq("season_id", season)
        .not_.in_("status", ["completed", "recovered"])
        .execute().data or []
    )
    if unfinished:
        raise RuntimeError(f"Unfinished D/ST operation; no automated writes: {unfinished}")

    storage = client.storage.from_(BUCKET)
    production_path = f"play_by_play/{season}.parquet"
    production = storage.download(production_path)
    original_sha = hashlib.sha256(production).hexdigest()

    # Generates, stages and audits the latest NFLverse full-season PBP.
    # Does not modify production scores or the production PBP object.
    run("--audit-staging")

    # Stage name is content-addressed, but refresh_dst currently does not
    # expose the digest in a machine-readable result. Identify it from the
    # objects present only if the candidate is unambiguous.
    staged = storage.list(f"play_by_play_staging/{season}")
    candidates = [
        item["name"] for item in staged
        if isinstance(item.get("name"), str) and item["name"].endswith(".parquet")
    ]
    # A scheduled run must not guess which of multiple old staging files
    # corresponds to the just-audited PBP. Require explicit discovery.
    if len(candidates) != 1:
        raise RuntimeError(
            "Cannot safely select staged artifact from multiple historical objects. "
            "Scheduled promotion blocked; staging audit completed."
        )
    staged_sha = candidates[0][:-8]
    if len(staged_sha) != 64 or any(c not in "0123456789abcdef" for c in staged_sha):
        raise RuntimeError("Invalid staged object name")
    staged_data = storage.download(f"play_by_play_staging/{season}/{staged_sha}.parquet")
    if hashlib.sha256(staged_data).hexdigest() != staged_sha:
        raise RuntimeError("Staged PBP checksum mismatch")
    if staged_sha == original_sha:
        print("NO PBP CHANGE: staged and production hashes match; no production writes")
        return

    # The immutable Parquet backup must be uploaded and verified before
    # the coordinator is allowed to start its journal.
    import requests
    backup_path = f"play_by_play_backups/{season}/{original_sha}.parquet"
    response = requests.post(
        f"{url}/storage/v1/object/{BUCKET}/{backup_path}",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/octet-stream", "x-upsert": "false"},
        data=production, timeout=600,
    )
    if not response.ok:
        error = response.text.lower()
        if response.status_code not in (400, 409) or not any(
            word in error for word in ("duplicate", "already exists", "resourcealreadyexists")
        ):
            response.raise_for_status()
    if hashlib.sha256(storage.download(backup_path)).hexdigest() != original_sha:
        raise RuntimeError("Immutable production backup verification failed")

    local = create_snapshot(
        client, season, ROOT / "backups" / "dst_database" / str(season)
    )
    remote = upload_snapshot(
        client, url=url, key=key, bucket=BUCKET, snapshot_path=local["path"]
    )
    run("--execute", "--staged-sha256", staged_sha,
        "--backup-parquet-sha256", original_sha,
        "--backup-database-sha256", remote["sha256"])


if __name__ == "__main__":
    main()
