"""Inspect unfinished D/ST refresh operations. Read-only; no production changes."""
from __future__ import annotations

import json
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.refresh_dst import required

def main():
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env", override=False)
    client = create_client(required("SUPABASE_URL"), required("SUPABASE_SERVICE_ROLE_KEY"))
    result = (
        client.table("dst_refresh_operations")
        .select("operation_id,season_id,status,backup_parquet_path,backup_parquet_sha256,backup_database_sha256,staged_parquet_path,staged_parquet_sha256,created_at,updated_at")
        .not_.in_("status", ["recovered", "completed"])
        .execute()
    )
    print(json.dumps(result.data or [], indent=2))
    if result.data:
        raise SystemExit("BLOCKED: unfinished refresh operations require recovery before another promotion")
    print("PASS: no unfinished D/ST refresh operations")

if __name__ == "__main__":
    main()
