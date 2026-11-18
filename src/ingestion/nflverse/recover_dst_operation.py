"""Recover an interrupted D/ST refresh using its immutable backups.

Default is read-only preflight. Execute requires explicit confirmation.
Run only after the failed writer has stopped; never concurrently with refresh.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import requests
from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.refresh_dst import BUCKET, required
from src.ingestion.nflverse.restore_dst_snapshot import count_rows, TABLES, CONFIRM
from src.ingestion.nflverse.verify_dst_snapshot import verify_snapshot

RECOVER_CONFIRM = "RECOVER_INTERRUPTED_DST_REFRESH"
ACTIVE = {"prepared", "promoting", "importing", "scoring", "verifying", "recovery_failed"}
ROOT = Path(__file__).resolve().parents[3]


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def advance(client, operation_id, expected, next_status, details=None):
    result = client.rpc("advance_dst_refresh_operation", {
        "p_operation_id": operation_id, "p_expected_status": expected,
        "p_next_status": next_status, "p_details": details or {},
    }).execute().data
    if not isinstance(result, dict) or result.get("status") != next_status:
        raise RuntimeError(f"Journal transition not confirmed: {expected} -> {next_status}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation_id", help="UUID shown by check_dst_operations")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    args = parser.parse_args()
    if args.execute and args.confirm != RECOVER_CONFIRM:
        raise RuntimeError(f"Execution requires --confirm {RECOVER_CONFIRM}")
    load_dotenv(dotenv_path=ROOT / ".env", override=False)
    url = required("SUPABASE_URL").rstrip("/")
    key = required("SUPABASE_SERVICE_ROLE_KEY")
    client = create_client(url, key)
    matches = client.table("dst_refresh_operations").select("*").eq(
        "operation_id", args.operation_id
    ).execute().data or []
    if len(matches) != 1:
        raise RuntimeError("Recovery operation not found")
    op = matches[0]
    if op["status"] not in ACTIVE:
        raise RuntimeError(f"Operation cannot be recovered from status {op['status']}")
    season = int(op["season_id"])
    if op["backup_parquet_path"] != f"play_by_play_backups/{season}/{op['backup_parquet_sha256']}.parquet":
        raise RuntimeError("Unexpected immutable Parquet backup path")
    if op["staged_parquet_path"] != f"play_by_play_staging/{season}/{op['staged_parquet_sha256']}.parquet":
        raise RuntimeError("Unexpected staged Parquet path")
    snapshot_path = ROOT / "backups" / "dst_database" / str(season) / (
        op["backup_database_sha256"] + ".json"
    )
    snapshot = verify_snapshot(snapshot_path)
    if snapshot["season"] != season:
        raise RuntimeError("Snapshot season does not match operation")
    backup = client.storage.from_(BUCKET).download(op["backup_parquet_path"])
    if sha256(backup) != op["backup_parquet_sha256"]:
        raise RuntimeError("Immutable Parquet backup checksum mismatch")
    staged = client.storage.from_(BUCKET).download(op["staged_parquet_path"])
    if sha256(staged) != op["staged_parquet_sha256"]:
        raise RuntimeError("Staged Parquet checksum mismatch")
    production_path = f"play_by_play/{season}.parquet"
    production = client.storage.from_(BUCKET).download(production_path)
    current_digest = sha256(production)
    if current_digest not in (op["backup_parquet_sha256"], op["staged_parquet_sha256"]):
        raise RuntimeError("Production Parquet differs from both recorded versions; manual investigation required")
    counts = {table: count_rows(client, table, season) for table in TABLES}
    print(json.dumps({
        "operation_id": args.operation_id, "status": op["status"],
        "season": season, "production_parquet_sha256": current_digest,
        "backup_parquet_sha256": op["backup_parquet_sha256"],
        "snapshot_path": str(snapshot_path), "current_database_rows": counts,
    }, indent=2), flush=True)
    if not args.execute:
        print("PREFLIGHT PASS: no production writes. Recovery requires --execute and confirmation.")
        return

    # Recovery must be run only after the original worker has stopped. Journal
    # transition is compare-and-swap; concurrent nonparticipating writers are
    # NOT protected by this protocol.
    advance(client, args.operation_id, op["status"], "recovering")
    try:
        # Restore database first. If it fails, production Parquet remains
        # untouched and the operation remains blocked for investigation.
        result = client.rpc("restore_dst_database_snapshot", {
            "p_snapshot": snapshot, "p_confirmation": CONFIRM,
            "p_expected_event_count": counts[TABLES[0]],
            "p_expected_score_count": counts[TABLES[1]],
        }).execute().data
        if not isinstance(result, dict) or result.get("success") is not True or result.get("changes_committed") is not True:
            raise RuntimeError(f"Database restore unconfirmed: {result}")
        # Re-read before replacement: never overwrite an unexpected concurrent
        # production object.
        latest = client.storage.from_(BUCKET).download(production_path)
        if sha256(latest) != current_digest:
            raise RuntimeError("Production Parquet changed during recovery; refusing overwrite")
        if current_digest != op["backup_parquet_sha256"]:
            response = requests.post(
                f"{url}/storage/v1/object/{BUCKET}/{production_path}",
                headers={"apikey": key, "Authorization": f"Bearer {key}",
                         "Content-Type": "application/octet-stream", "x-upsert": "true"},
                data=backup, timeout=600,
            )
            response.raise_for_status()
        restored = client.storage.from_(BUCKET).download(production_path)
        if sha256(restored) != op["backup_parquet_sha256"]:
            raise RuntimeError("Restored production Parquet checksum mismatch")
        # Database RPC verifies all rows inside its transaction; verify row
        # counts again after storage restoration before clearing the journal.
        for table in TABLES:
            expected = snapshot["tables"][table]["row_count"]
            if count_rows(client, table, season) != expected:
                raise RuntimeError(f"Restored database row count mismatch: {table}")
        advance(client, args.operation_id, "recovering", "recovered",
                {"recovery": "verified_database_and_parquet"})
        print("RECOVERY VERIFIED: database and production Parquet restored; journal closed")
    except BaseException as exc:
        try:
            advance(client, args.operation_id, "recovering", "recovery_failed",
                    {"recovery_error": str(exc)[:1500]})
        except Exception:
            print("CRITICAL: unable to mark journal recovery_failed", flush=True)
        raise


if __name__ == "__main__":
    main()
