
"""
Load NFLverse team reference data into Supabase public.dim_team.

Usage:
    python -m src.ingestion.nflverse.load_teams --preview
    python -m src.ingestion.nflverse.load_teams --load

Required .env values for --load:
    SUPABASE_URL=https://YOUR_PROJECT.supabase.co
    SUPABASE_SERVICE_ROLE_KEY=YOUR_SERVICE_ROLE_KEY

Keep the service-role key private. Never commit it to Git.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import nflreadpy as nfl
from dotenv import load_dotenv


def clean_text(value: Any) -> str | None:
    """Convert a source value to clean text, preserving missing values."""
    if value is None:
        return None

    value = str(value).strip()
    return value if value else None


def normalize_team(row: dict[str, Any]) -> dict[str, Any]:
    """Map the exact NFLverse source columns to the dim_team schema."""

    team_code = clean_text(row.get("team_abbr"))
    if not team_code:
        raise ValueError(f"Missing team_abbr in source row: {row}")

    conference = clean_text(row.get("team_conf"))
    division = clean_text(row.get("team_division"))

    if conference:
        conference = conference.upper()

    if division:
        division = division.split()[-1].title()

    normalized = {
        "nflverse_team_code": team_code,
        "team_name": clean_text(row.get("team_name")),
        "team_nickname": clean_text(row.get("team_nick")),
        "city": None,
        "conference": conference,
        "division": division,
        "founded_year": None,
    }

    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest NFLverse teams.")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--preview",
        action="store_true",
        help="Inspect source data without writing to Supabase.",
    )
    mode.add_argument(
        "--load",
        action="store_true",
        help="Upsert teams into Supabase.",
    )

    args = parser.parse_args()

    print("Downloading NFLverse team reference data...")
    source_df = nfl.load_teams()

    print(f"\nRows returned: {source_df.height}")
    print("\nSource columns:")
    print(source_df.columns)

    # Convert the Polars DataFrame into Python dictionaries.
    source_rows = source_df.to_dicts()

    if not source_rows:
        print("No team records were returned.", file=sys.stderr)
        return 1

    # Debug the actual source values used by the normalization function.
    print("\nFirst raw source row:")
    print(source_rows[0])

    # Normalize every source record.
    normalized_rows = [
        normalize_team(row)
        for row in source_rows
    ]

    # Validate that team codes are unique before loading.
    team_codes = [
        row["nflverse_team_code"]
        for row in normalized_rows
    ]

    if len(team_codes) != len(set(team_codes)):
        raise ValueError("Duplicate NFLverse team codes found.")

    print("\nNormalized preview:")
    for row in normalized_rows[:10]:
        print(row)

    # Verify required source fields were mapped.
    missing_nicknames = [
        row["nflverse_team_code"]
        for row in normalized_rows
        if not row["team_nickname"]
    ]

    missing_divisions = [
        row["nflverse_team_code"]
        for row in normalized_rows
        if not row["division"]
    ]

    print("\nValidation:")
    print(f"Unique team codes: {len(set(team_codes))}")
    print(f"Missing nicknames: {missing_nicknames}")
    print(f"Missing divisions: {missing_divisions}")

    if args.preview:
        print("\nPreview complete. No database changes were made.")
        return 0

    # Load credentials only when the user explicitly requests a database load.
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

    response = (
        client.table("dim_team")
        .upsert(
            normalized_rows,
            on_conflict="nflverse_team_code",
        )
        .execute()
    )

    print(f"\nUpsert submitted for {len(normalized_rows)} teams.")
    print("Supabase response received successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
