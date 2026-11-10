
"""
Load the Sleeper NFL player directory into Supabase.

Run:
    python -m src.ingestion.platforms.sleeper.load_players
"""

import json
import os
import uuid
from datetime import date, datetime, timezone
from urllib.request import Request, urlopen

from dotenv import load_dotenv
from supabase import create_client


SLEEPER_API_URL = "https://api.sleeper.app/v1/players/nfl"
BATCH_SIZE = 500
PIPELINE_NAME = "sleeper_player_directory"


def get_supabase_client():
    """Create the Supabase client using project environment variables."""
    load_dotenv()

    url = os.getenv("SUPABASE_URL")
    key = (
        os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        or os.getenv("SUPABASE_KEY")
    )

    if not url or not key:
        raise EnvironmentError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY "
            "(or SUPABASE_KEY) in your environment."
        )

    return create_client(url, key)


def fetch_sleeper_players():
    """Retrieve the full NFL player directory from Sleeper."""
    request = Request(
        SLEEPER_API_URL,
        headers={"User-Agent": "DynastyDecisionEngine/1.0"}
    )

    with urlopen(request, timeout=120) as response:
        players = json.loads(response.read().decode("utf-8"))

    if not isinstance(players, dict):
        raise ValueError("Unexpected response from Sleeper API.")

    return players


def to_int(value):
    """Safely convert a value to an integer."""
    if value is None or value == "":
        return None

    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def to_date(value):
    """Convert an API date string into a database date."""
    if not value:
        return None

    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (ValueError, TypeError):
        return None


def normalize_player(player_id, player, retrieved_at):
    """Convert a Sleeper player into our dim_sleeper_player schema."""

    team = player.get("team")

    return {
        "sleeper_player_id": str(player_id),
        "full_name": player.get("full_name"),
        "first_name": player.get("first_name"),
        "last_name": player.get("last_name"),
        "search_full_name": player.get("search_full_name"),
        "search_first_name": player.get("search_first_name"),
        "search_last_name": player.get("search_last_name"),
        "position": player.get("position"),
        "fantasy_positions": player.get("fantasy_positions"),
        "team_code": team,
        "team_abbr": team,
        "status": player.get("status"),
        "active": player.get("active"),
        "years_experience": to_int(player.get("years_exp")),
        "birth_date": to_date(player.get("birth_date")),
        "age": to_int(player.get("age")),
        "height": player.get("height"),
        "weight": to_int(player.get("weight")),
        "college": player.get("college"),
        "high_school": player.get("high_school"),
        "jersey_number": to_int(player.get("number")),
        "gsis_id": player.get("gsis_id"),
        "espn_id": str(player["espn_id"]) if player.get("espn_id") is not None else None,
        "yahoo_id": str(player["yahoo_id"]) if player.get("yahoo_id") is not None else None,
        "sportradar_id": player.get("sportradar_id"),
        "rotowire_id": str(player["rotowire_id"]) if player.get("rotowire_id") is not None else None,
        "stats_id": str(player["stats_id"]) if player.get("stats_id") is not None else None,
        "fantasy_data_id": str(player["fantasy_data_id"]) if player.get("fantasy_data_id") is not None else None,
        "injury_status": player.get("injury_status"),
        "injury_start_date": to_date(player.get("injury_start_date")),
        "injury_body_part": player.get("injury_body_part"),
        "injury_notes": player.get("injury_notes"),
        "practice_participation": player.get("practice_participation"),
        "practice_description": player.get("practice_description"),
        "depth_chart_position": player.get("depth_chart_position"),
        "depth_chart_order": to_int(player.get("depth_chart_order")),
        "search_rank": to_int(player.get("search_rank")),
        "player_shard": str(player["player_shard"]) if player.get("player_shard") is not None else None,
        "hashtag": player.get("hashtag"),
        "raw_payload": player,
        "source_retrieved_at": retrieved_at,
    }


def log_pipeline_start(supabase, run_id, started_at):
    """Create a pipeline run record."""
    supabase.table("pipeline_runs").insert({
        "run_id": run_id,
        "pipeline_name": PIPELINE_NAME,
        "source_system": "sleeper",
        "dataset_name": "players",
        "source_endpoint": SLEEPER_API_URL,
        "run_status": "running",
        "started_at": started_at,
        "metadata": {},
    }).execute()


def update_pipeline_run(supabase, run_id, updates):
    """Update the pipeline run record."""
    supabase.table("pipeline_runs").update(updates).eq(
        "run_id", run_id
    ).execute()


def upload_players(supabase, rows):
    """Upsert player records in manageable batches."""
    total = len(rows)

    for start in range(0, total, BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]

        supabase.table("dim_sleeper_player").upsert(
            batch,
            on_conflict="sleeper_player_id"
        ).execute()

        print(
            f"Uploaded {min(start + len(batch), total):,} "
            f"of {total:,} players"
        )


def main():
    supabase = get_supabase_client()

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    retrieved_at = started_at

    print("Starting Sleeper player ingestion...")

    log_pipeline_start(supabase, run_id, started_at)

    try:
        sleeper_players = fetch_sleeper_players()

        print(f"Retrieved {len(sleeper_players):,} Sleeper player records")

        rows = [
            normalize_player(player_id, player, retrieved_at)
            for player_id, player in sleeper_players.items()
            if isinstance(player, dict)
        ]

        print(f"Normalized {len(rows):,} player records")

        upload_players(supabase, rows)

        completed_at = datetime.now(timezone.utc).isoformat()

        update_pipeline_run(supabase, run_id, {
            "run_status": "succeeded",
            "completed_at": completed_at,
            "records_extracted": len(sleeper_players),
            "records_inserted": len(rows),
            "records_updated": 0,
            "records_rejected": len(sleeper_players) - len(rows),
            "source_retrieved_at": retrieved_at,
            "metadata": {
                "batch_size": BATCH_SIZE,
                "operation": "upsert",
            },
        })

        print("\nSleeper player ingestion completed successfully.")
        print(f"Records processed: {len(rows):,}")
        print(f"Pipeline run ID: {run_id}")

    except Exception as error:
        update_pipeline_run(supabase, run_id, {
            "run_status": "failed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "error_message": str(error)[:4000],
        })

        raise


if __name__ == "__main__":
    main()
