"""Protected manual D/ST database restore. Default mode is preflight only.

Install sql/restore_dst_database_snapshot.sql in Supabase SQL Editor first.
A verified local snapshot is required. Never run concurrently with ingestion.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import load_dotenv
from supabase import create_client

from src.ingestion.nflverse.refresh_dst import required
from src.ingestion.nflverse.verify_dst_snapshot import verify_snapshot

CONFIRM = "RESTORE_DST_DATABASE_SNAPSHOT"
TABLES = ("fact_team_fumble_events", "fact_calculated_team_weekly_scores")


def count_rows(client, table: str, season: int) -> int:
    response = (
        client.table(table)
        .select("season_id", count="exact", head=True)
        .eq("season_id", season)
        .execute()
    )
    if response.count is None:
        raise RuntimeError(f"Unable to determine production row count: {table}")
    return int(response.count)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--execute", action="store_true", help="Perform an actual database restore")
    parser.add_argument("--confirm", default="", help=f"Required with --execute: {CONFIRM}")
    args = parser.parse_args()
    snapshot = verify_snapshot(args.snapshot)
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / ".env", override=False)
    client = create_client(required("SUPABASE_URL"), required("SUPABASE_SERVICE_ROLE_KEY"))
    season = snapshot["season"]
    counts = {name: count_rows(client, name, season) for name in TABLES}
    print("CURRENT PRODUCTION ROW COUNTS:", json.dumps(counts, sort_keys=True), flush=True)
    if not args.execute:
        print("PREFLIGHT ONLY: no changes made. Explicit --execute and --confirm required.", flush=True)
        return
    if args.confirm != CONFIRM:
        raise RuntimeError("Restore refused: missing exact --confirm token")
    print("WARNING: restoring production D/ST rows for season", season, flush=True)
    result = client.rpc(
        "restore_dst_database_snapshot",
        {
            "p_snapshot": snapshot,
            "p_confirmation": CONFIRM,
            "p_expected_event_count": counts[TABLES[0]],
            "p_expected_score_count": counts[TABLES[1]],
        },
    ).execute()
    report = result.data
    print(json.dumps(report, indent=2), flush=True)
    if not isinstance(report, dict) or report.get("success") is not True or report.get("changes_committed") is not True:
        raise RuntimeError("Restore result was not confirmed; inspect database before retrying")
    print("DATABASE RESTORE COMMITTED; verify production before restarting ingestion.", flush=True)


if __name__ == "__main__":
    main()
