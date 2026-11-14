
"""
Load NFLverse weekly player statistics into Supabase.

Routine refresh:
    python -m src.ingestion.nflverse.load_weekly_stats --preview
    python -m src.ingestion.nflverse.load_weekly_stats --load

Historical backfill:
    python -m src.ingestion.nflverse.load_weekly_stats --preview --backfill
    python -m src.ingestion.nflverse.load_weekly_stats --load --backfill

Optional:
    --weeks 5

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

# Preserve the project's existing historical load boundary.
FIRST_SEASON_TO_LOAD = 2024

# Routine refresh window: latest five available weeks.
DEFAULT_WEEKS_TO_REFRESH = 5

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

    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


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


def load_current_season(client):
    """Get the season marked as current in dim_season."""
    response = (
        client.table("dim_season")
        .select("season_id")
        .eq("is_current", True)
        .execute()
    )

    rows = response.data or []

    if len(rows) != 1:
        raise ValueError(
            "Expected exactly one current season in dim_season; "
            f"found {len(rows)}."
        )

    season = int(rows[0]["season_id"])

    print(f"Current NFL season: {season}")

    return season


# --------------------------------------------------
# Source loading
# --------------------------------------------------

def load_source_stats(seasons):
    """Download only the requested NFL season or seasons."""
    print(
        "\nDownloading NFLverse weekly player statistics "
        f"for seasons: {seasons}"
    )

    df = nfl.load_player_stats(
        seasons=seasons,
        summary_level="week",
    )

    print(f"Source records retrieved: {df.height:,}")

    missing_columns = REQUIRED_SOURCE_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required source columns: {sorted(missing_columns)}"
        )

    return df


def filter_recent_weeks(source_df, weeks_to_refresh):
    """
    Retain the latest available weeks from the current season.

    This refreshes recent records so late stat corrections
    can be picked up by the hash comparison.
    """
    if source_df.height == 0:
        print("\nNo source records returned for this season.")
        return source_df

    latest_week_value = source_df["week"].max()

    if latest_week_value is None:
        raise ValueError(
            "NFLverse returned records, but no valid week was found."
        )

    latest_week = int(latest_week_value)
    first_week = max(1, latest_week - weeks_to_refresh + 1)

    filtered_df = source_df.filter(
        source_df["week"] >= first_week
    )

    print("\nRECENT WEEK FILTER")
    print("-" * 45)
    print(f"Latest available week: {latest_week}")
    print(f"First week included:   {first_week}")
    print(f"Weeks included:        {latest_week - first_week + 1}")
    print(f"Records retained:      {filtered_df.height:,}")

    return filtered_df


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
# Existing record comparison
# --------------------------------------------------

def fetch_existing_hashes(client, season_ids):
    """
    Retrieve existing natural keys and hashes for the seasons
    currently being processed.
    """
    if not season_ids:
        return {}

    print("\nChecking existing database records...")

    existing = {}
    start = 0

    while True:
        response = (
            client.table("fact_player_weekly_stats")
            .select(
                "source_player_id,game_id,season_type,source_row_hash"
            )
            .in_("season_id", season_ids)
            .range(start, start + PAGE_SIZE - 1)
            .execute()
        )

        batch = response.data or []

        for row in batch:
            key = (
                row["source_player_id"],
                row["game_id"],
                row["season_type"],
            )

            existing[key] = row.get("source_row_hash")

        if len(batch) < PAGE_SIZE:
            break

        start += PAGE_SIZE

    print(f"Existing records retrieved: {len(existing):,}")

    return existing


def compare_records(records, existing_hashes):
    """Separate records into inserts, updates, and unchanged rows."""
    new_records = []
    updated_records = []
    unchanged_count = 0

    for record in records:
        key = (
            record["source_player_id"],
            record["game_id"],
            record["season_type"],
        )

        old_hash = existing_hashes.get(key)

        if key not in existing_hashes:
            new_records.append(record)

        elif old_hash != record["source_row_hash"]:
            updated_records.append(record)

        else:
            unchanged_count += 1

    return new_records, updated_records, unchanged_count


# --------------------------------------------------
# Incremental database loading
# --------------------------------------------------

def sync_weekly_records(client, records):
    """
    Insert new records and update changed records.

    Existing records are compared by source_row_hash.
    Unchanged records are not rewritten.
    """
    if not records:
        print("\nNo new or changed records to write.")
        return 0

    total = len(records)
    batches_processed = 0

    print(f"\nWriting {total:,} new or changed records...")

    for start in range(0, total, BATCH_SIZE):
        batch = records[start:start + BATCH_SIZE]

        (
            client.table("fact_player_weekly_stats")
            .upsert(
                batch,
                on_conflict="source_player_id,game_id,season_type",
                ignore_duplicates=False,
            )
            .execute()
        )

        batches_processed += 1

        completed = min(start + len(batch), total)

        print(
            f"Written {completed:,} / {total:,} "
            f"records ({completed / total:.1%})"
        )

    print(f"\nCompleted {batches_processed:,} batches.")

    return total


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

    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Load the configured historical seasons instead of recent weeks.",
    )

    parser.add_argument(
        "--weeks",
        type=int,
        default=DEFAULT_WEEKS_TO_REFRESH,
        help="Number of latest available weeks to refresh. Default: 5.",
    )

    args = parser.parse_args()

    if args.weeks < 1:
        parser.error("--weeks must be at least 1.")

    print("\nNFLVERSE WEEKLY PLAYER STATS PIPELINE")
    print("=" * 55)

    if args.backfill:
        print("MODE: HISTORICAL BACKFILL")
    else:
        print("MODE: RECENT SEASON REFRESH")

    client = get_supabase_client()

    # Load database reference data.
    player_mapping = load_player_mapping(client)
    valid_game_ids = load_game_ids(client)
    valid_season_ids = load_season_ids(client)
    current_season = load_current_season(client)

    # Choose the source download scope.
    if args.backfill:
        seasons_to_load = sorted(
            season
            for season in valid_season_ids
            if season >= FIRST_SEASON_TO_LOAD
        )

        if not seasons_to_load:
            raise ValueError(
                "No valid seasons found for the historical backfill."
            )

        print(f"Backfill seasons: {seasons_to_load}")

    else:
        if current_season not in valid_season_ids:
            raise ValueError(
                f"Current season {current_season} is not in valid seasons."
            )

        seasons_to_load = current_season
        print(f"Current season selected: {current_season}")

    # Download only the selected seasons.
    source_df = load_source_stats(seasons_to_load)

    # Routine refresh: keep only recent weeks.
    if not args.backfill:
        source_df = filter_recent_weeks(
            source_df,
            args.weeks,
        )

    # Normalize and validate source records.
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

    # Preserve the existing historical boundary during backfill.
    if args.backfill:
        records_to_process = [
            record
            for record in records
            if (
                record["season_id"] > FIRST_SEASON_TO_LOAD
                or (
                    record["season_id"] == FIRST_SEASON_TO_LOAD
                    and record["season_type"] == "POST"
                )
            )
        ]
    else:
        records_to_process = records

    matched_count = sum(
        1
        for row in records_to_process
        if row["player_id"] is not None
    )

    seasons_in_records = sorted({
        record["season_id"]
        for record in records_to_process
    })

    # Compare source hashes with existing database rows.
    existing_hashes = fetch_existing_hashes(
        client,
        seasons_in_records,
    )

    new_records, updated_records, unchanged_count = compare_records(
        records_to_process,
        existing_hashes,
    )

    records_to_write = new_records + updated_records

    # --------------------------------------------------
    # Validation summary
    # --------------------------------------------------

    print("\nVALIDATION SUMMARY")
    print("=" * 55)
    print(f"Source records downloaded: {source_df.height:,}")
    print(f"Prepared records:          {len(records):,}")
    print(f"Records in scope:          {len(records_to_process):,}")
    print(f"Excluded null IDs:         {null_count:,}")
    print(f"Excluded zero IDs:         {zero_count:,}")
    print(f"Matched players:           {matched_count:,}")
    print(f"Unmatched player IDs:      {len(unmatched_players):,}")
    print(f"Valid games:               {len(valid_game_ids):,}")
    print(f"Valid seasons:             {len(valid_season_ids):,}")
    print(f"Batch size:                {BATCH_SIZE:,}")

    print("\nINCREMENTAL SYNC SUMMARY")
    print("=" * 55)
    print(f"New records:               {len(new_records):,}")
    print(f"Changed records:           {len(updated_records):,}")
    print(f"Unchanged records:         {unchanged_count:,}")
    print(f"Total records to write:    {len(records_to_write):,}")

    if args.backfill:
        print(f"Historical boundary:       {FIRST_SEASON_TO_LOAD} POST onward")
    else:
        print(f"Weeks requested:           {args.weeks}")

    if unmatched_players:
        print("\nUnmatched player ID samples:")

        for player_id in sorted(unmatched_players)[:20]:
            print(f"  {player_id}")

    # --------------------------------------------------
    # Preview
    # --------------------------------------------------

    if args.preview:
        print("\nPREVIEW SAMPLE")

        for record in records_to_write[:3]:
            print(record)

        print("\nPREVIEW COMPLETE")
        print("No database records were modified.")

        return 0

    # --------------------------------------------------
    # Load
    # --------------------------------------------------

    print("\nLoading new and changed weekly player statistics...")

    written_count = sync_weekly_records(
        client,
        records_to_write,
    )

    print("\nWEEKLY STATS PIPELINE COMPLETE")
    print("=" * 55)
    print(f"Source records downloaded: {source_df.height:,}")
    print(f"Records in scope:          {len(records_to_process):,}")
    print(f"New records:               {len(new_records):,}")
    print(f"Updated records:           {len(updated_records):,}")
    print(f"Unchanged records:         {unchanged_count:,}")
    print(f"Records written:           {written_count:,}")
    print(f"Matched players:           {matched_count:,}")
    print(f"Unmatched player IDs:      {len(unmatched_players):,}")
    print(f"Excluded source rows:      {null_count + zero_count:,}")

    print(
        "\nExisting records were updated only when their "
        "NFLverse source hash changed."
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"\nPIPELINE FAILED: {exc}", file=sys.stderr)
        raise
