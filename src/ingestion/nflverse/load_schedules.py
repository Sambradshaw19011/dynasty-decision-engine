
import argparse
import os
from datetime import date, datetime
from pathlib import Path

import nflreadpy as nfl
from dotenv import load_dotenv
from supabase import create_client


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env", override=True)

BATCH_SIZE = 500

VALID_GAME_TYPES = {"REG", "PRE", "WC", "DIV", "CON", "SB"}

SEASON_TYPE_MAPPING = {
    "REG": "REG",
    "PRE": "PRE",
    "WC": "POST",
    "DIV": "POST",
    "CON": "POST",
    "SB": "POST",
}


# --------------------------------------------------
# Supabase
# --------------------------------------------------

def get_supabase_client():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not url or not key:
        raise ValueError(
            "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env"
        )

    return create_client(url, key)


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def clean_value(value):
    """Convert Polars/Python values into database-friendly values."""

    if value is None:
        return None

    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    return value


def clean_integer(value):
    if value is None:
        return None
    return int(value)


def clean_date(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    return str(value)


def get_neutral_site(location):
    if location is None:
        return None

    return str(location).strip().lower() == "neutral"


# --------------------------------------------------
# Load NFLverse data
# --------------------------------------------------

def load_source_schedules():
    print("Downloading NFLverse schedules...")

    df = nfl.load_schedules(seasons=True)

    print(f"Retrieved {df.height:,} games.")

    return df.to_dicts()


# --------------------------------------------------
# Build season records
# --------------------------------------------------

def build_season_records(source_rows):
    seasons = {}

    for row in source_rows:
        season = clean_integer(row["season"])
        game_date = clean_date(row.get("gameday"))

        if season not in seasons:
            seasons[season] = {
                "dates": []
            }

        if game_date:
            seasons[season]["dates"].append(game_date)

    if not seasons:
        raise ValueError("No season records were found.")

    current_season = max(seasons)

    season_records = []

    for season, values in sorted(seasons.items()):
        dates = values["dates"]

        season_records.append({
            "season_id": season,
            "season_type": "REG+POST",
            "start_date": min(dates) if dates else None,
            "end_date": max(dates) if dates else None,
            "is_current": season == current_season,
        })

    return season_records


# --------------------------------------------------
# Build game records
# --------------------------------------------------

def build_game_records(source_rows, valid_team_codes):
    game_records = []
    errors = []

    seen_game_ids = set()

    for row in source_rows:
        game_id = row.get("game_id")

        if not game_id:
            errors.append("Game with missing game_id")
            continue

        if game_id in seen_game_ids:
            errors.append(f"Duplicate game_id: {game_id}")
            continue

        seen_game_ids.add(game_id)

        season = clean_integer(row.get("season"))
        week = clean_integer(row.get("week"))

        game_type = row.get("game_type")

        if game_type not in VALID_GAME_TYPES:
            errors.append(
                f"{game_id}: unsupported game type {game_type}"
            )
            continue

        season_type = SEASON_TYPE_MAPPING[game_type]
        home_team = row.get("home_team")
        away_team = row.get("away_team")

        if not season or not week:
            errors.append(f"{game_id}: missing season or week")
            continue

        if not home_team or not away_team:
            errors.append(f"{game_id}: missing home or away team")
            continue

        if home_team not in valid_team_codes:
            errors.append(
                f"{game_id}: unknown home team {home_team}"
            )
            continue

        if away_team not in valid_team_codes:
            errors.append(
                f"{game_id}: unknown away team {away_team}"
            )
            continue

        if home_team == away_team:
            errors.append(
                f"{game_id}: home and away teams are identical"
            )
            continue

        home_score = clean_integer(row.get("home_score"))
        away_score = clean_integer(row.get("away_score"))

        if home_score is not None and away_score is not None:
            game_status = "FINAL"
        else:
            game_status = "SCHEDULED"

        game_records.append({
            "game_id": str(game_id),
            "season_id": season,
            "week": week,
            "season_type": season_type,
            "game_date": clean_date(row.get("gameday")),
            "kickoff_at": None,
            "home_team_code": home_team,
            "away_team_code": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "stadium_id": None,
            "game_type": game_type,
            "game_status": game_status,
            "neutral_site": get_neutral_site(row.get("location")),
            "roof": row.get("roof"),
            "surface": row.get("surface"),
        })

    if errors:
        print(f"\nValidation errors: {len(errors)}")

        for error in errors[:25]:
            print(f"  - {error}")

        if len(errors) > 25:
            print(f"  ... and {len(errors) - 25} more")

        raise ValueError(
            "Schedule validation failed. No records were loaded."
        )

    return game_records


# --------------------------------------------------
# Database validation
# --------------------------------------------------

def get_valid_team_codes(supabase):
    response = (
        supabase.table("dim_team")
        .select("nflverse_team_code")
        .execute()
    )

    return {
        row["nflverse_team_code"]
        for row in response.data
    }


# --------------------------------------------------
# Database loading
# --------------------------------------------------

def upsert_records(supabase, table_name, records, conflict_column):
    total = len(records)

    for start in range(0, total, BATCH_SIZE):
        batch = records[start:start + BATCH_SIZE]

        (
            supabase.table(table_name)
            .upsert(batch, on_conflict=conflict_column)
            .execute()
        )

        completed = min(start + len(batch), total)

        print(
            f"{table_name}: {completed:,}/{total:,} records loaded"
        )


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load NFLverse schedules into Supabase."
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preview", action="store_true")
    mode.add_argument("--load", action="store_true")

    args = parser.parse_args()

    print("\nNFLVERSE SCHEDULE PIPELINE")
    print("=" * 45)

    supabase = get_supabase_client()

    source_rows = load_source_schedules()

    valid_team_codes = get_valid_team_codes(supabase)

    season_records = build_season_records(source_rows)

    game_records = build_game_records(
        source_rows,
        valid_team_codes
    )

    print("\nVALIDATION SUMMARY")
    print("-" * 45)
    print(f"Source games:       {len(source_rows):,}")
    print(f"Unique seasons:     {len(season_records):,}")
    print(f"Validated games:    {len(game_records):,}")
    print(f"Database teams:     {len(valid_team_codes):,}")

    print(
        f"Season range:       "
        f"{season_records[0]['season_id']} - "
        f"{season_records[-1]['season_id']}"
    )

    print("\nSeason records:")
    for season in season_records:
        print(
            f"  {season['season_id']}: "
            f"{season['start_date']} - {season['end_date']}"
        )

    print("\nSample game records:")
    for game in game_records[:5]:
        print(game)

    if args.preview:
        print("\nPREVIEW COMPLETE")
        print("No database records were modified.")
        return

    print("\nLoading seasons...")
    upsert_records(
        supabase,
        "dim_season",
        season_records,
        "season_id"
    )

    print("\nLoading games...")
    upsert_records(
        supabase,
        "dim_game",
        game_records,
        "game_id"
    )

    print("\nSCHEDULE PIPELINE COMPLETE")


if __name__ == "__main__":
    main()
