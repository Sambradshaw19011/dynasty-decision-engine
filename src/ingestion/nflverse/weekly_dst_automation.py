"""Scheduled D/ST ingestion using the existing audited, journaled production path."""
from __future__ import annotations

import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.refresh_dst import BUCKET, all_rows, required
from src.ingestion.nflverse.backup_dst_database import create_snapshot
from src.ingestion.nflverse.dst_durable_backup import upload_snapshot

ROOT = Path(__file__).resolve().parents[3]


def run(*args: str) -> str:
    result = subprocess.run(
        [sys.executable, "-m", "src.ingestion.nflverse.refresh_dst", *args],
        cwd=ROOT, check=True, capture_output=True, text=True,
    )
    print(result.stdout, flush=True)
    return result.stdout


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
    audit_output = run("--audit-staging")

    # Parse the exact digest printed by the successful staging run; never
    # guess among previous weekly staging objects.
    match = re.search(
        r"^STAGING PASS: nflverse-data/play_by_play_staging/"
        + str(season) + r"/([0-9a-f]{64})\.parquet$",
        audit_output, flags=re.MULTILINE,
    )
    if not match or "STAGING CLASSIFIER AUDIT PASS" not in audit_output:
        raise RuntimeError("Audited staging digest was not confirmed")
    staged_sha = match.group(1)
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
