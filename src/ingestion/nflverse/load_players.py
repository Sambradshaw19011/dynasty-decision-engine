
"""
Load NFLverse player reference data into Supabase public.dim_player.

Usage:
    python -m src.ingestion.nflverse.load_players --preview
    python -m src.ingestion.nflverse.load_players --load

Requires .env values for --load:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import date, datetime
from typing import Any

import nflreadpy as nfl
from dotenv import load_dotenv


BATCH_SIZE = 500

# Priority order for identifying a player when the GSIS ID is unavailable.
PLAYER_ID_COLUMNS = (
    "gsis_id",
    "esb_id",
    "nfl_id",
    "pfr_id",
    "pff_id",
    "otc_id",
    "espn_id",
    "smart_id",
)

GSIS_PATTERN = re.compile(r"^\d{2}-\d{7}$")


def clean_text(value: Any) -> str | None:
    """Convert a value to clean text."""
    if value is None:
        return None

    value = str(value).strip()
    return value if value else None


def clean_integer(value: Any) -> int | None:
    """Safely convert a value to an integer."""
    if value is None:
        return None

    try:
        return int(value)
    except (ValueError, TypeError, OverflowError):
        return None


def clean_date(value: Any) -> str | None:
    """Convert a date value into an ISO date string."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (ValueError, TypeError):
        return None


def get_player_identifier(row: dict[str, Any]) -> str | None:
    """Find the best available source identifier for a player."""
    for column in PLAYER_ID_COLUMNS:
        value = clean_text(row.get(column))
        if value:
            return value

    return None


def normalize_team_code(
    value: Any,
    known_team_codes: set[str],
) -> str | None:
    """Return a valid NFLverse team code or None."""
    team_code = clean_text(value)

    if not team_code:
        return None

    team_code = team_code.upper()

    if team_code not in known_team_codes:
        return None

    return team_code


def normalize_player(
    row: dict[str, Any],
    known_team_codes: set[str],
) -> dict[str, Any]:
    """Map NFLverse player data to the dim_player schema."""

    player_identifier = get_player_identifier(row)

    if not player_identifier:
        raise ValueError(
            "Player is missing every supported source identifier: "
            f"{row.get('display_name')}"
        )

    raw_gsis_id = clean_text(row.get("gsis_id"))

    gsis_id = (
        raw_gsis_id
        if raw_gsis_id and GSIS_PATTERN.fullmatch(raw_gsis_id)
        else None
    )

    raw_espn_id = clean_text(row.get("espn_id"))

    return {
        "nflverse_player_id": player_identifier,
        "gsis_id": gsis_id,
        "espn_id": raw_espn_id,

        "display_name": clean_text(row.get("display_name")),
        "common_first_name": clean_text(row.get("common_first_name")),
        "first_name": clean_text(row.get("first_name")),
        "last_name": clean_text(row.get("last_name")),
        "short_name": clean_text(row.get("short_name")),
        "football_name": clean_text(row.get("football_name")),
        "suffix": clean_text(row.get("suffix")),

        "position": clean_text(row.get("position")),
        "position_group": clean_text(row.get("position_group")),
        "ngs_position": clean_text(row.get("ngs_position")),
        "ngs_position_group": clean_text(row.get("ngs_position_group")),

        "birth_date": clean_date(row.get("birth_date")),
        "height": clean_integer(row.get("height")),
        "weight": clean_integer(row.get("weight")),
        "college_name": clean_text(row.get("college_name")),
        "college_conference": clean_text(row.get("college_conference")),
        "headshot_url": clean_text(row.get("headshot")),

        "jersey_number": clean_text(row.get("jersey_number")),
        "rookie_season": clean_integer(row.get("rookie_season")),
        "last_season": clean_integer(row.get("last_season")),
        "latest_team_code": normalize_team_code(
            row.get("latest_team"), known_team_codes
        ),
        "status": clean_text(row.get("status")),
        "ngs_status": clean_text(row.get("ngs_status")),
        "ngs_status_short_description": clean_text(
            row.get("ngs_status_short_description")
        ),
        "years_of_experience": clean_integer(
            row.get("years_of_experience")
        ),

        "pff_position": clean_text(row.get("pff_position")),
        "pff_status": clean_text(row.get("pff_status")),
        "draft_year": clean_integer(row.get("draft_year")),
        "draft_round": clean_integer(row.get("draft_round")),
        "draft_pick": clean_integer(row.get("draft_pick")),
        "draft_team_code": normalize_team_code(
            row.get("draft_team"), known_team_codes
        ),
    }


def validate_players(
    normalized_rows: list[dict[str, Any]],
) -> None:
    """Validate identifiers before allowing a database load."""

    player_ids = [
        row["nflverse_player_id"]
        for row in normalized_rows
    ]

    if len(player_ids) != len(set(player_ids)):
        raise ValueError(
            "Duplicate player identifiers found. "
            "Resolve duplicates before loading."
        )

    gsis_ids = [
        row["gsis_id"]
        for row in normalized_rows
        if row["gsis_id"]
    ]

    if len(gsis_ids) != len(set(gsis_ids)):
        raise ValueError(
            "Duplicate valid GSIS IDs found. "
            "Resolve duplicates before loading."
        )

    missing_names = [
        row["nflverse_player_id"]
        for row in normalized_rows
        if not row["display_name"]
    ]

    if missing_names:
        raise ValueError(
            f"{len(missing_names)} records have no display name."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ingest NFLverse player reference data."
    )

    mode = parser.add_mutually_exclusive_group(required=True)

    mode.add_argument(
        "--preview",
        action="store_true",
        help="Inspect and validate data without writing to Supabase.",
    )

    mode.add_argument(
        "--load",
        action="store_true",
        help="Upsert players into Supabase.",
    )

    args = parser.parse_args()

    print("Downloading NFLverse player reference data...")

    source_df = nfl.load_players()

    print(f"\nRows returned: {source_df.height}")
    print("\nSource columns:")
    print(source_df.columns)

    source_rows = source_df.to_dicts()

    if not source_rows:
        print("No player records returned.", file=sys.stderr)
        return 1

    # Use NFLverse's own team reference data to validate team codes.
    teams_df = nfl.load_teams()
    known_team_codes = set(teams_df["team_abbr"].to_list())

    print(f"\nKnown NFLverse team codes: {len(known_team_codes)}")

    print("\nFirst raw player row:")
    print(source_rows[0])

    normalized_rows = [
        normalize_player(row, known_team_codes)
        for row in source_rows
    ]

    validate_players(normalized_rows)

    print("\nNormalized player preview:")
    for row in normalized_rows[:10]:
        print(row)

    # Report identifier coverage.
    gsis_count = sum(
        1 for row in normalized_rows if row["gsis_id"]
    )

    missing_latest_team = sum(
        1 for row in normalized_rows if not row["latest_team_code"]
    )

    missing_draft_team = sum(
        1 for row in normalized_rows if not row["draft_team_code"]
    )

    print("\nValidation results:")
    print(f"Total player records: {len(normalized_rows)}")
    print(f"Unique player identifiers: {len(normalized_rows)}")
    print(f"Valid GSIS IDs: {gsis_count}")
    print(f"Missing GSIS IDs: {len(normalized_rows) - gsis_count}")
    print(f"Missing/unknown latest teams: {missing_latest_team}")
    print(f"Missing/unknown draft teams: {missing_draft_team}")

    if args.preview:
        print("\nPreview complete. No database changes were made.")
        return 0

    # Load credentials from the project's .env file.
    load_dotenv(override=True)

    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    if not supabase_url or not service_role_key:
        print(
            "\nMissing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env.",
            file=sys.stderr,
        )
        return 1

    from supabase import create_client

    client = create_client(supabase_url, service_role_key)

    print("\nStarting Supabase player upsert...")

    total_loaded = 0

    for start in range(0, len(normalized_rows), BATCH_SIZE):
        batch = normalized_rows[start:start + BATCH_SIZE]

        client.table("dim_player").upsert(
            batch,
            on_conflict="nflverse_player_id",
        ).execute()

        total_loaded += len(batch)

        print(
            f"Loaded {total_loaded:,} / "
            f"{len(normalized_rows):,} players"
        )

    print(f"\nSuccessfully upserted {total_loaded:,} players.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
