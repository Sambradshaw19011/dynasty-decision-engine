"""Refresh current-season PBP fumbles and recalculate D/ST scores.

Run after load_schedules and load_weekly_stats. Requires an already deployed
import-pbp-fumbles Edge Function and an idempotent calculate_team_weekly_scores RPC.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import os
import sys
from datetime import datetime, timezone

import nflreadpy as nfl
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv
from supabase import create_client

BUCKET = "nflverse-data"
TIMEOUT = (30, 600)
PAGE_SIZE = 1000


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def pages(table, columns, *, filters=()):
    result = []
    offset = 0
    while True:
        q = table.select(columns)
        for method, column, value in filters:
            q = getattr(q, method)(column, value)
        batch = q.range(offset, offset + PAGE_SIZE - 1).execute().data or []
        result.extend(batch)
        if len(batch) < PAGE_SIZE:
            return result
        offset += PAGE_SIZE


def canonical(code):
    return {"LA": "LAR"}.get(code, code)


def season_from_database(client):
    rows = pages(client.table("dim_season"), "season_id", filters=[("eq", "is_current", True)])
    if len(rows) != 1:
        raise RuntimeError(f"Expected exactly one current season, got {len(rows)}")
    return int(rows[0]["season_id"])


def get_pbp_bytes(season):
    print(f"Downloading NFLverse PBP for {season}", flush=True)
    frame = nfl.load_pbp(seasons=[season])
    if frame.height == 0:
        raise RuntimeError("NFLverse returned no play-by-play records")
    required_cols = {"game_id", "play_id", "season", "week", "fumble_recovery_1_team"}
    missing = required_cols - set(frame.columns)
    if missing:
        raise RuntimeError(f"Missing required PBP columns: {sorted(missing)}")
    season_values = set(frame["season"].drop_nulls().unique().to_list())
    if season_values != {season}:
        raise RuntimeError(f"Unexpected PBP seasons: {season_values}")
    if frame["game_id"].null_count() or frame["play_id"].null_count():
        raise RuntimeError("PBP contains null game_id/play_id; refusing to replace Storage object")
    table = frame.to_arrow()
    output = io.BytesIO()
    pq.write_table(table, output, compression="zstd")
    data = output.getvalue()
    print(f"Downloaded {frame.height:,} plays ({len(data):,} bytes)", flush=True)
    return data, frame.height


def storage_refresh(session, base, key, season, data, rows):
    url = f"{base}/storage/v1/object/{BUCKET}/play_by_play/{season}.parquet"
    # Existing object is retained if the new source unexpectedly loses plays.
    old = session.get(url, timeout=TIMEOUT)
    if old.status_code == 200:
        old_data = old.content
        old_rows = pq.ParquetFile(io.BytesIO(old_data)).metadata.num_rows
        if rows < old_rows:
            raise RuntimeError(
                f"Refusing to replace {season} PBP: {rows} new rows < {old_rows} stored rows"
            )
        if hashlib.sha256(old_data).digest() == hashlib.sha256(data).digest():
            print("Storage PBP already matches source; no upload needed", flush=True)
            return
    elif old.status_code not in (400, 404):
        old.raise_for_status()
    response = session.post(
        url, data=data, timeout=TIMEOUT,
        headers={"Content-Type": "application/octet-stream", "x-upsert": "true"},
    )
    response.raise_for_status()
    verified = session.get(url, timeout=TIMEOUT)
    verified.raise_for_status()
    if hashlib.sha256(verified.content).digest() != hashlib.sha256(data).digest():
        raise RuntimeError("Uploaded PBP SHA256 verification failed")
    print("Current-season Parquet uploaded and SHA256 verified", flush=True)


def import_fumbles(session, base, season, inspection_secret):
    url = f"{base}/functions/v1/import-pbp-fumbles"
    response = session.post(
        url,
        json={"season": season, "mode": "import", "confirm": "IMPORT_VERIFIED_EVENTS"},
        headers={"x-inspection-secret": inspection_secret},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    result = response.json()
    if not result.get("success"):
        raise RuntimeError(f"Fumble import failed: {result}")
    print(f"Fumble import succeeded: {result}", flush=True)


def final_games(client, season):
    games = pages(
        client.table("dim_game"),
        "game_id,season_id,week,season_type,game_status,home_team_code,away_team_code,home_score,away_score",
        filters=[("eq", "season_id", season)],
    )
    return [
        g for g in games
        if str(g.get("game_status", "")).upper() == "FINAL"
        and g.get("home_score") is not None
        and g.get("away_score") is not None
        and g.get("season_type") in {"REG", "POST"}
    ]


def recalculate(client, season, settings_id, games):
    if not games:
        print("No final games to score", flush=True)
        return
    fumbles = pages(
        client.table("fact_team_fumble_events"),
        "game_id,season_type",
        filters=[("eq", "season_id", season)],
    )
    imported_types = {r["season_type"] for r in fumbles}
    scopes = sorted({(g["season_type"], int(g["week"])) for g in games})
    for kind, week in scopes:
        if kind not in imported_types:
            raise RuntimeError(f"Fumble events missing for {season} {kind}")
        result = client.rpc(
            "calculate_team_weekly_scores",
            {
                "p_scoring_settings_id": settings_id,
                "p_season": season,
                "p_week": week,
                "p_season_type": kind,
                "p_engine_version": "def-1.4.1-weekly",
            },
        ).execute()
        if not result.data:
            raise RuntimeError(f"Scoring RPC returned no run ID for {season} {kind} W{week}")
        print(f"Scored {season} {kind} W{week}: {result.data}", flush=True)


def verify(client, season, settings_id, games):
    scores = pages(
        client.table("fact_calculated_team_weekly_scores"),
        "game_id,team_code,season_type,week",
        filters=[("eq", "season_id", season), ("eq", "scoring_settings_id", settings_id)],
    )
    by_game = {}
    for s in scores:
        by_game.setdefault(s["game_id"], []).append(s["team_code"])
    failures = []
    for game in games:
        actual = by_game.get(game["game_id"], [])
        expected = {canonical(game["home_team_code"]), canonical(game["away_team_code"])}
        if len(actual) != 2 or set(map(canonical, actual)) != expected:
            failures.append({"game_id": game["game_id"], "expected": sorted(expected), "actual": actual})
    if failures:
        raise RuntimeError(f"D/ST verification failed for {len(failures)} games: {failures[:10]}")
    print(f"PASS: {len(games)} final games have exactly two matching D/ST scores", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, help="Override current season for manual runs")
    args = parser.parse_args()
    load_dotenv()
    base = required("SUPABASE_URL").rstrip("/")
    key = required("SUPABASE_SERVICE_ROLE_KEY")
    settings_id = required("SCORING_SETTINGS_ID")
    secret = required("PBP_INSPECT_SECRET")
    client = create_client(base, key)
    season = args.season or season_from_database(client)
    if season < 2005:
        raise RuntimeError(f"Unexpected season: {season}")
    print(f"Weekly D/ST refresh: season {season} at {datetime.now(timezone.utc).isoformat()}", flush=True)
    session = requests.Session()
    session.headers.update({"apikey": key, "Authorization": f"Bearer {key}"})
    data, rows = get_pbp_bytes(season)
    storage_refresh(session, base, key, season, data, rows)
    import_fumbles(session, base, season, secret)
    games = final_games(client, season)
    recalculate(client, season, settings_id, games)
    verify(client, season, settings_id, games)
    print("WEEKLY D/ST REFRESH SUCCEEDED", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"WEEKLY D/ST REFRESH FAILED: {exc}", file=sys.stderr)
        raise
