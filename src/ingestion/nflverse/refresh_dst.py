"""Refresh current-season PBP fumbles and recalculate all completed D/ST weeks.

Requires the deployed import-pbp-fumbles Edge Function, current scoring RPC,
and GitHub secrets documented in docs/weekly-dst-refresh.md.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import nflreadpy as nfl
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from src.ingestion.nflverse.load_play_by_play import COLUMN_MAP, INTEGER_COLUMNS, clean, integer_or_none
from supabase import create_client

BUCKET = "nflverse-data"
PAGE = 1000


def required(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required secret: {name}")
    return value


def all_rows(client, table, columns, filters=None):
    rows = []
    start = 0
    while True:
        q = client.table(table).select(columns)
        for key, value in (filters or {}).items():
            q = q.eq(key, value)
        batch = q.range(start, start + PAGE - 1).execute().data or []
        rows.extend(batch)
        if len(batch) < PAGE:
            return rows
        start += PAGE



def build_compatible_parquet(frame, existing_bytes, destination):
    """Reproduce the historical PostgreSQL-export schema without database writes."""
    import io
    historical_schema = pq.read_schema(io.BytesIO(existing_bytes))
    required_source = set(COLUMN_MAP)
    missing_source = sorted(required_source - set(frame.columns))
    if missing_source:
        raise RuntimeError(f"NFLverse missing mapped source fields: {missing_source}")
    expected_fields = set(COLUMN_MAP.values()) | {
        "raw_play_data", "source_retrieved_at", "updated_at"
    }
    if set(historical_schema.names) != expected_fields:
        raise RuntimeError(
            f"Stored schema differs from loader: "
            f"missing={sorted(expected_fields - set(historical_schema.names))}, "
            f"extra={sorted(set(historical_schema.names) - expected_fields)}"
        )
    if frame.select("game_id").null_count().item() or frame.select("play_id").null_count().item():
        raise RuntimeError("Null play identifiers")
    if frame.select(["game_id", "play_id"]).unique().height != frame.height:
        raise RuntimeError("Duplicate game_id/play_id keys")
    retrieved_at = datetime.now(timezone.utc)
    writer = pq.ParquetWriter(destination, historical_schema, compression="zstd")
    written = 0
    try:
        for offset in range(0, frame.height, 1000):
            records = []
            for row in frame.slice(offset, 1000).to_dicts():
                raw = {name: clean(value) for name, value in row.items()}
                item = {target: raw[source] for source, target in COLUMN_MAP.items()}
                for column in INTEGER_COLUMNS:
                    item[column] = integer_or_none(item[column], column)
                item["raw_play_data"] = json.dumps(raw, ensure_ascii=False, separators=(",", ":"), default=str)
                item["source_retrieved_at"] = retrieved_at
                item["updated_at"] = retrieved_at
                records.append(item)
            batch = pa.Table.from_pylist(records, schema=historical_schema)
            writer.write_table(batch)
            written += batch.num_rows
    finally:
        writer.close()
    output = pq.ParquetFile(destination)
    if written != frame.height or output.metadata.num_rows != frame.height:
        raise RuntimeError("Parquet row count mismatch")
    if not output.schema_arrow.equals(historical_schema, check_metadata=False):
        raise RuntimeError("Parquet schema mismatch")
    print(f"Verified {written:,} plays; {len(historical_schema.names)} historical columns", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='Upload and import after local verification')
    args = parser.parse_args()
    url = required("SUPABASE_URL").rstrip("/")
    key = required("SUPABASE_SERVICE_ROLE_KEY")
    secret = required("PBP_INSPECT_SECRET") if args.execute else None
    settings_id = required("DST_SCORING_SETTINGS_ID") if args.execute else None
    client = create_client(url, key)

    seasons = all_rows(client, "dim_season", "season_id", {"is_current": True})
    if len(seasons) != 1:
        raise RuntimeError(f"Expected one current season; found {len(seasons)}")
    season = int(seasons[0]["season_id"])
    print(f"Refreshing {season}", flush=True)

    games = all_rows(client, "dim_game",
                     "game_id,week,season_type,game_status,home_score,away_score",
                     {"season_id": season})
    final = [g for g in games if str(g.get("game_status", "")).upper() == "FINAL"
             and g.get("home_score") is not None and g.get("away_score") is not None
             and g.get("season_type") in ("REG", "POST")]
    if not final:
        raise RuntimeError("No completed games found; refusing to overwrite PBP")

    # NFLverse returns a full-season dataset, not only the latest week.
    frame = nfl.load_pbp(seasons=[season])
    if frame.height == 0 or not {"game_id", "play_id"}.issubset(set(frame.columns)):
        raise RuntimeError("PBP source is empty or missing game_id/play_id")
    source_games = set(frame["game_id"].drop_nulls().unique().to_list())
    missing = {g["game_id"] for g in final} - source_games
    if missing:
        raise RuntimeError(f"PBP missing {len(missing)} completed games; refusing upload: {sorted(missing)[:5]}")

    object_path = f"play_by_play/{season}.parquet"
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / f"{season}.parquet"
        existing = client.storage.from_(BUCKET).download(object_path)
        build_compatible_parquet(frame, existing, path)
        data = path.read_bytes()
        if len(data) < 1000:
            raise RuntimeError("Unexpectedly small PBP file")

        if not args.execute:
            print(f"DRY RUN PASS: compatible {object_path}; no storage, event, or score changes", flush=True)
            return

        # Upsert only after full-source validation; existing data remains if validation fails.
        endpoint = f"{url}/storage/v1/object/{BUCKET}/{object_path}"
        headers = {"apikey": key, "Authorization": f"Bearer {key}",
                   "Content-Type": "application/octet-stream", "x-upsert": "true"}
        response = requests.post(endpoint, headers=headers, data=data, timeout=600)
        response.raise_for_status()
        print(f"Uploaded {object_path}: {len(data)} bytes, {frame.height} plays", flush=True)

    endpoint = f"{url}/functions/v1/import-pbp-fumbles"
    headers = {"apikey": key, "Authorization": f"Bearer {key}",
               "x-inspection-secret": secret, "Content-Type": "application/json"}
    # Do not score until season replacement succeeds.
    for payload in (
        {"season": season, "mode": "audit"},
        {"season": season, "mode": "import", "confirm": "IMPORT_VERIFIED_EVENTS"},
    ):
        response = requests.post(endpoint, headers=headers, json=payload, timeout=600)
        response.raise_for_status()
        body = response.json()
        if body.get("success") is not True:
            raise RuntimeError(f"Fumble {payload['mode']} failed: {body}")
        print(f"Fumble {payload['mode']} succeeded", flush=True)

    for season_type in ("REG", "POST"):
        weeks = sorted({int(g["week"]) for g in final if g["season_type"] == season_type})
        for week in weeks:
            result = client.rpc("calculate_team_weekly_scores", {
                "p_scoring_settings_id": settings_id,
                "p_season": season,
                "p_week": week,
                "p_season_type": season_type,
                "p_engine_version": "def-1.4.1-weekly",
            }).execute()
            if not result.data:
                raise RuntimeError(f"No scoring run returned for {season_type} week {week}")
            print(f"Scored {season_type} week {week}: {result.data}", flush=True)

    scores = all_rows(client, "fact_calculated_team_weekly_scores",
                      "game_id,team_code,season_type,week",
                      {"season_id": season, "scoring_settings_id": settings_id})
    by_game = {}
    for row in scores:
        by_game.setdefault(row["game_id"], []).append(row["team_code"])
    problems = [(g["game_id"], by_game.get(g["game_id"], [])) for g in final
                if len(by_game.get(g["game_id"], [])) != 2
                or len(set(by_game.get(g["game_id"], []))) != 2]
    if problems:
        raise RuntimeError(f"D/ST coverage failed for {len(problems)} games: {problems[:8]}")
    print(f"PASS: {len(final)} completed games have two distinct D/ST scores", flush=True)


if __name__ == "__main__":
    main()
