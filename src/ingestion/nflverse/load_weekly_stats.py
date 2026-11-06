
"""
Load NFLverse weekly player statistics into Supabase.

Usage:
    python -m src.ingestion.nflverse.load_weekly_stats --preview
    python -m src.ingestion.nflverse.load_weekly_stats --load

Requires .env:
    SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import nflreadpy as nfl
from dotenv import load_dotenv
from supabase import create_client


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env", override=True)

BATCH_SIZE = 100
PAGE_SIZE = 1000
FIRST_SEASON_TO_LOAD = 2024

SOURCE_DATASET = "nflverse_player_stats"

STAT_COLUMNS = {
    "completions", "attempts", "passing_yards", "passing_tds",
    "passing_interceptions", "sacks_suffered", "sack_yards_lost",
    "sack_fumbles", "sack_fumbles_lost", "passing_air_yards",
    "passing_yards_after_catch", "passing_first_downs", "passing_epa",
    "passing_cpoe", "passing_2pt_conversions", "pacr", "passing_10",
    "passing_16", "passing_20", "passing_40",

    "carries", "rushing_yards", "rushing_tds", "rushing_fumbles",
    "rushing_fumbles_lost", "rushing_first_downs", "rushing_epa",
    "rushing_2pt_conversions", "rushing_10", "rushing_12",
    "rushing_20", "rushing_40",

    "receptions", "targets", "receiving_yards", "receiving_tds",
    "receiving_fumbles", "receiving_fumbles_lost", "receiving_air_yards",
    "receiving_yards_after_catch", "receiving_first_downs",
    "receiving_epa", "receiving_2pt_conversions", "receiving_10",
    "receiving_16", "receiving_20", "receiving_40", "racr",
    "target_share", "air_yards_share", "wopr",

    "fumbles_lost", "fumbles", "fumble_recovery_own",
    "fumble_recovery_yards_own", "fumble_recovery_opp",
    "fumble_recovery_yards_opp", "fumble_recovery_tds", "misc_yards",
    "penalties", "penalty_yards",

    "def_tackles_solo", "def_tackles_with_assist",
    "def_tackle_assists", "def_tackles_for_loss",
    "def_tackles_for_loss_yards", "def_fumbles_forced",
    "def_sacks", "def_sack_yards", "def_qb_hits",
    "def_interceptions", "def_interception_yards",
    "def_pass_defended", "def_tds", "def_fumbles",
    "def_safeties", "def_punt_blocks", "def_pat_blocks",
    "def_fg_blocks", "def_2pt_atts", "def_2pt_made",

    "special_teams_tds", "punt_returns", "punt_return_yards",
    "kickoff_returns", "kickoff_return_yards",
    "yards_per_punt_return", "yards_per_kickoff_return",

    "fg_made", "fg_att", "fg_missed", "fg_blocked", "fg_pct",
    "fg_made_0_19", "fg_made_20_29", "fg_made_30_39",
    "fg_made_40_49", "fg_made_50_plus", "fg_missed_0_19",
    "fg_missed_20_29", "fg_missed_30_39", "fg_missed_40_49",
    "fg_missed_50_plus", "pat_made", "pat_att", "pat_missed",

    "punts", "punt_yards", "punt_avg", "punt_net_avg",
    "punts_inside_20", "punt_touchbacks",
}

REQUIRED_SOURCE_COLUMNS = {
    "player_id",
    "game_id",
    "season",
    "week",
    "season_type",
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


def fetch_all_rows(client, table, columns):
    """Fetch a complete reference table using pagination."""
    results = []
    start = 0

    while True:
        response = (
            client.table(table)
            .select(columns)
            .range(start, start + PAGE_SIZE - 1)
            .execute()
        )

        batch = response.data or []
        results.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        start += PAGE_SIZE

    return results


# --------------------------------------------------
# Value cleaning
# --------------------------------------------------

def clean_value(value: Any):
    if value is None:
        return None

    if hasattr(value, "item"):
        value = value.item()

    if isinstance(value, float) and not math.isfinite(value):
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    return value


def clean_text(value):
    value = clean_value(value)

    if value is None:
        return None

    value = str(value).strip()

    return value if value else None


def clean_integer(value):
    value = clean_value(value)

    if value is None:
        return None

    return int(value)


def make_row_hash(source_row):
    """Create a deterministic hash of the original source record."""
    cleaned = {
        key: clean_value(value)
        for key, value in source_row.items()
    }

    serialized = json.dumps(
        cleaned,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
        allow_nan=False,
    )

    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


# --------------------------------------------------
# Database reference data
# --------------------------------------------------

def load_player_mapping(client):
    print("Loading canonical player ID mappings...")

    rows = fetch_all_rows(
        client,
        "dim_player",
        "player_id,nflverse_player_id",
    )

    mapping = {
        row["nflverse_player_id"]: row["player_id"]
        for row in rows
        if row.get("nflverse_player_id")
    }

    print(f"Player mappings retrieved: {len(mapping):,}")

    return mapping


def load_game_ids(client):
    print("Loading valid game IDs...")

    rows = fetch_all_rows(
        client,
        "dim_game",
        "game_id",
    )

    game_ids = {
        row["game_id"]
        for row in rows
        if row.get("game_id")
    }

    print(f"Valid games retrieved: {len(game_ids):,}")

    return game_ids


def load_season_ids(client):
    print("Loading valid season IDs...")

    rows = fetch_all_rows(
        client,
        "dim_season",
        "season_id",
    )

    season_ids = {
        int(row["season_id"])
        for row in rows
        if row.get("season_id") is not None
    }

    print(f"Valid seasons retrieved: {len(season_ids):,}")

    return season_ids


# --------------------------------------------------
# Source loading
# --------------------------------------------------

def load_source_stats():
    print("\nDownloading NFLverse weekly player statistics...")

    df = nfl.load_player_stats(seasons=True)

    print(f"Source records retrieved: {df.height:,}")

    missing_columns = REQUIRED_SOURCE_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required source columns: {sorted(missing_columns)}"
        )

    return df


# --------------------------------------------------
# Record normalization
# --------------------------------------------------

def normalize_record(
    source_row,
    player_mapping,
    valid_game_ids,
    valid_season_ids,
    retrieved_at,
):
    source_player_id = clean_text(source_row.get("player_id"))

    if not source_player_id or source_player_id == "0":
        raise ValueError(
            "Cannot normalize a record without a valid player ID."
        )

    game_id = clean_text(source_row.get("game_id"))
    season = clean_integer(source_row.get("season"))
    week = clean_integer(source_row.get("week"))
    season_type = clean_text(source_row.get("season_type"))

    if game_id not in valid_game_ids:
        raise ValueError(f"Unknown game ID: {game_id}")

    if season not in valid_season_ids:
        raise ValueError(f"Unknown season: {season}")

    if week is None or week <= 0:
        raise ValueError(f"Invalid week: {week}")

    if season_type not in {"REG", "POST", "PRE"}:
        raise ValueError(f"Invalid season type: {season_type}")

    canonical_player_id = player_mapping.get(source_player_id)

    record = {
        "player_id": canonical_player_id,
        "source_player_id": source_player_id,
        "game_id": game_id,
        "season_id": season,
        "week": week,
        "season_type": season_type,
        "team_code": clean_text(source_row.get("team")),
        "opponent_team_code": clean_text(
            source_row.get("opponent_team")
        ),
    }

    for column in STAT_COLUMNS:
        if column in source_row:
            record[column] = clean_value(source_row[column])

    mapped_source_columns = (
        STAT_COLUMNS
        | {
            "player_id",
            "game_id",
            "season",
            "week",
            "season_type",
            "team",
            "opponent_team",
        }
    )

    extra_stats = {
        key: clean_value(value)
        for key, value in source_row.items()
        if key not in mapped_source_columns
    }

    record.update({
        "source_dataset": SOURCE_DATASET,
        "source_retrieved_at": retrieved_at,
        "source_row_hash": make_row_hash(source_row),
        "raw_extra_stats": extra_stats,
        "updated_at": retrieved_at,
    })

    return record


# --------------------------------------------------
# Validation and preparation
# --------------------------------------------------

def prepare_records(
    source_df,
    player_mapping,
    valid_game_ids,
    valid_season_ids,
):
    source_rows = source_df.to_dicts()

    null_id_count = 0
    zero_id_count = 0
    valid_source_rows = []

    for row in source_rows:
        source_id = clean_text(row.get("player_id"))

        if source_id is None:
            null_id_count += 1
        elif source_id == "0":
            zero_id_count += 1
        else:
            valid_source_rows.append(row)

    print("\nSOURCE RECORD FILTERING")
    print("-" * 45)
    print(f"Total source rows:       {len(source_rows):,}")
    print(f"Null player IDs:         {null_id_count:,}")
    print(f"Zero player IDs:         {zero_id_count:,}")
    print(f"Usable player records:   {len(valid_source_rows):,}")

    retrieved_at = datetime.now(timezone.utc).isoformat()

    records = []
    unmatched_players = set()
    errors = []

    for index, source_row in enumerate(valid_source_rows, start=1):
        try:
            record = normalize_record(
                source_row,
                player_mapping,
                valid_game_ids,
                valid_season_ids,
                retrieved_at,
            )

            if record["player_id"] is None:
                unmatched_players.add(record["source_player_id"])

            records.append(record)

        except (ValueError, TypeError, OverflowError) as exc:
            errors.append(f"Source row {index}: {exc}")

    if errors:
        print(f"\nVALIDATION ERRORS: {len(errors):,}")

        for error in errors[:25]:
            print(f"  - {error}")

        if len(errors) > 25:
            print(f"  ... and {len(errors) - 25:,} more")

        raise ValueError(
            "Weekly stats validation failed. No records were loaded."
        )

    natural_keys = [
        (
            row["source_player_id"],
            row["game_id"],
            row["season_type"],
        )
        for row in records
    ]

    if len(natural_keys) != len(set(natural_keys)):
        raise ValueError(
            "Duplicate natural keys found in weekly stats source."
        )

    return (
        records,
        null_id_count,
        zero_id_count,
        unmatched_players,
    )


# --------------------------------------------------
# Incremental database loading
# --------------------------------------------------

def insert_new_records(client, records):
    """
    Insert records without updating existing rows.

    The unique natural-key index prevents duplicates.
    Existing rows are ignored rather than rewritten.
    """

    total = len(records)

    if total == 0:
        print("\nNo records to insert.")
        return

    print(f"\nProcessing {total:,} records...")

    batches_processed = 0

    for start in range(0, total, BATCH_SIZE):
        batch = records[start:start + BATCH_SIZE]

        (
            client.table("fact_player_weekly_stats")
            .upsert(
                batch,
                on_conflict="source_player_id,game_id,season_type",
                ignore_duplicates=True,
            )
            .execute()
        )

        batches_processed += 1

        completed = min(start + len(batch), total)

        print(
            f"Processed {completed:,} / {total:,} "
            f"records ({completed / total:.1%})"
        )

    print(f"\nCompleted {batches_processed:,} batches.")


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load NFLverse weekly player statistics."
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--preview", action="store_true")
    mode.add_argument("--load", action="store_true")

    args = parser.parse_args()

    print("\nNFLVERSE WEEKLY PLAYER STATS PIPELINE")
    print("=" * 55)

    client = get_supabase_client()

    # Load database reference data.
    player_mapping = load_player_mapping(client)
    valid_game_ids = load_game_ids(client)
    valid_season_ids = load_season_ids(client)

    # Load and normalize source statistics.
    source_df = load_source_stats()

    (
        records,
        null_count,
        zero_count,
        unmatched_players,
    ) = prepare_records(
        source_df,
        player_mapping,
        valid_game_ids,
        valid_season_ids,
    )

    matched_count = sum(
        1 for row in records
        if row["player_id"] is not None
    )

    # Filter to the seasons that need additional records.
    records_to_load = [
    record
    for record in records
    if (
        record["season_id"] > 2024
        or (
            record["season_id"] == 2024
            and record["season_type"] == "POST"
        )
    )
]

    print("\nVALIDATION SUMMARY")
    print("=" * 55)
    print(f"Source records:            {source_df.height:,}")
    print(f"Prepared records:          {len(records):,}")
    print(f"Excluded null IDs:         {null_count:,}")
    print(f"Excluded zero IDs:         {zero_count:,}")
    print(f"Matched players:           {matched_count:,}")
    print(f"Unmatched player IDs:      {len(unmatched_players):,}")
    print(f"Valid games:               {len(valid_game_ids):,}")
    print(f"Valid seasons:             {len(valid_season_ids):,}")
    print(f"Batch size:                {BATCH_SIZE:,}")

    print("\nINCREMENTAL LOAD SUMMARY")
    print("=" * 55)
    print(f"First season to load:      {FIRST_SEASON_TO_LOAD}")
    print(f"Records from 2024 onward:  {len(records_to_load):,}")

    if unmatched_players:
        print("\nUnmatched player ID samples:")

        for player_id in sorted(unmatched_players)[:20]:
            print(f"  {player_id}")

    if args.preview:
        print("\nPREVIEW SAMPLE")

        for record in records_to_load[:3]:
            print(record)

        print("\nPREVIEW COMPLETE")
        print("No database records were modified.")

        return 0

    print("\nLoading weekly player statistics...")

    insert_new_records(client, records_to_load)

    print("\nWEEKLY STATS PIPELINE COMPLETE")
    print("=" * 55)
    print(f"Source records prepared:   {len(records):,}")
    print(f"Records processed:         {len(records_to_load):,}")
    print(f"Records before 2024:       {len(records) - len(records_to_load):,}")
    print(f"Matched players:           {matched_count:,}")
    print(f"Unmatched player IDs:      {len(unmatched_players):,}")
    print(f"Excluded source rows:      {null_count + zero_count:,}")

    print(
        "\nNote: Existing records were ignored by the unique index. "
        "The processed count is not the number of newly inserted rows."
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"\nPIPELINE FAILED: {exc}", file=sys.stderr)
        raise
