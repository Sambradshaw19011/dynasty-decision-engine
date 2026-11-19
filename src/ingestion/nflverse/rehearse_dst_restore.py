"""Run a rollback-only PostgreSQL D/ST restore rehearsal.

First install sql/rehearse_dst_database_restore.sql through Supabase SQL Editor.
No restore is committed, even when the rehearsal passes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.refresh_dst import required
from src.ingestion.nflverse.verify_dst_snapshot import verify_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    args = parser.parse_args()

    snapshot = verify_snapshot(args.snapshot)
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env", override=False)
    client = create_client(required("SUPABASE_URL"), required("SUPABASE_SERVICE_ROLE_KEY"))
    print("Starting database restore rehearsal (transaction will roll back)", flush=True)
    result = client.rpc(
        "rehearse_dst_database_restore", {"p_snapshot": snapshot}
    ).execute()
    report = result.data
    print(json.dumps(report, indent=2), flush=True)
    if (
        not isinstance(report, dict)
        or report.get("success") is not True
        or report.get("mode") != "rollback_rehearsal"
        or report.get("changes_committed") is not False
        or report.get("season") != snapshot["season"]
        or report.get("fumble_events_verified") != snapshot["tables"]["fact_team_fumble_events"]["row_count"]
        or report.get("dst_scores_verified") != snapshot["tables"]["fact_calculated_team_weekly_scores"]["row_count"]
    ):
        raise RuntimeError("Database recovery rehearsal did not pass")
    print("DATABASE RESTORE REHEARSAL PASS: all changes rolled back", flush=True)


if __name__ == "__main__":
    main()
