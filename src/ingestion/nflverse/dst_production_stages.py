"""Concrete production Parquet promotion for the journal-controlled D/ST refresh.

Only call inside a verified RefreshJournal 'promoting' stage. The CLI
--execute gate in refresh_dst.py remains disabled.
"""
from __future__ import annotations

import hashlib
import requests

BUCKET = "nflverse-data"


def promote_verified_parquet(client, *, season: int, staged_digest: str,
                             backup_digest: str, url: str, key: str) -> None:
    """Promote only a previously verified staged object with a matching backup."""
    if not all(isinstance(d, str) and len(d) == 64 and
               all(c in "0123456789abcdef" for c in d)
               for d in (staged_digest, backup_digest)):
        raise RuntimeError("Invalid recorded Parquet SHA256")
    storage = client.storage.from_(BUCKET)
    staging_path = f"play_by_play_staging/{season}/{staged_digest}.parquet"
    backup_path = f"play_by_play_backups/{season}/{backup_digest}.parquet"
    production_path = f"play_by_play/{season}.parquet"

    staged = storage.download(staging_path)
    backup = storage.download(backup_path)
    current = storage.download(production_path)
    sha = lambda value: hashlib.sha256(value).hexdigest()
    if sha(staged) != staged_digest:
        raise RuntimeError("Staged Parquet checksum mismatch before promotion")
    if sha(backup) != backup_digest:
        raise RuntimeError("Backup Parquet checksum mismatch before promotion")
    if sha(current) != backup_digest:
        raise RuntimeError("Production Parquet changed before promotion; refusing overwrite")

    response = requests.post(
        f"{url.rstrip('/')}/storage/v1/object/{BUCKET}/{production_path}",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/octet-stream", "x-upsert": "true"},
        data=staged, timeout=600,
    )
    response.raise_for_status()
    promoted = storage.download(production_path)
    if sha(promoted) != staged_digest:
        raise RuntimeError("Production Parquet promotion checksum mismatch")


def import_verified_fumbles(*, season: int, staged_digest: str,
                            expected_rows: int, operation_id: str, url: str, key: str,
                            inspection_secret: str) -> dict:
    """Require staging and production audits to agree before import."""
    if not isinstance(staged_digest, str) or len(staged_digest) != 64 or any(
        c not in "0123456789abcdef" for c in staged_digest
    ):
        raise RuntimeError("Invalid staged Parquet digest")
    endpoint = f"{url.rstrip('/')}/functions/v1/import-pbp-fumbles"
    headers = {"apikey": key, "Authorization": f"Bearer {key}",
               "x-inspection-secret": inspection_secret,
               "Content-Type": "application/json"}

    def call(payload):
        response = requests.post(endpoint, headers=headers, json=payload, timeout=600)
        response.raise_for_status()
        return response.json()

    staged_path = f"play_by_play_staging/{season}/{staged_digest}.parquet"
    staged = call({"season": season, "mode": "audit", "staging_path": staged_path})
    production = call({"season": season, "mode": "audit"})
    fields = (
        "classifier_version", "parquet_rows", "fumble_plays",
        "credited_recoveries", "safety_plays", "invalidated_plays",
        "events_prepared", "recovered_events", "categories_and_outcomes",
        "unknown_events", "unclassified_recoveries", "issues_count",
    )
    for report, mode, source in (
        (staged, "audit", f"{BUCKET}/{staged_path}"),
        (production, "audit", f"{BUCKET}/play_by_play/{season}.parquet"),
    ):
        if (report.get("success") is not True or report.get("mode") != mode
                or report.get("source") != source
                or report.get("parquet_rows") != expected_rows
                or report.get("issues_count") != 0
                or report.get("unclassified_recoveries") != 0
                or report.get("unknown_events") not in (None, 0)):
            raise RuntimeError(f"Fumble classifier audit rejected: {report}")
    if any(staged.get(field) != production.get(field) for field in fields):
        raise RuntimeError("Production fumble audit differs from staging")
    imported = call({"season": season, "mode": "import",
                     "operation_id": operation_id,
                     "confirm": "IMPORT_VERIFIED_EVENTS"})
    if imported.get("success") is not True or imported.get("imported") is None:
        raise RuntimeError(f"Fumble import unconfirmed: {imported}")
    expected_events = staged.get("events_prepared")
    if (not isinstance(expected_events, int) or expected_events < 0
            or type(imported["imported"]) is not int
            or imported["imported"] != expected_events):
        raise RuntimeError(
            f"Fumble import count mismatch: expected {expected_events}, "
            f"imported {imported['imported']}"
        )
    return imported


def score_completed_dst_weeks(client, *, season: int, settings_id: str,
                              completed_games: list[dict]) -> list[dict]:
    """Recalculate every completed regular/postseason week for the league."""
    if not completed_games:
        raise RuntimeError("No completed games to score")
    results = []
    for season_type in ("REG", "POST"):
        weeks = sorted({
            int(game["week"]) for game in completed_games
            if game["season_type"] == season_type
        })
        for week in weeks:
            data = client.rpc("calculate_team_weekly_scores", {
                "p_scoring_settings_id": settings_id,
                "p_season": season, "p_week": week,
                "p_season_type": season_type,
                "p_engine_version": "def-1.4.1-weekly",
            }).execute().data
            if not data:
                raise RuntimeError(f"Scoring failed: {season_type} week {week}")
            results.append({"season_type": season_type, "week": week, "result": data})
    if not results:
        raise RuntimeError("No completed D/ST weeks were scored")
    return results


def verify_completed_dst_scores(client, *, season: int, settings_id: int,
                                completed_games: list[dict]) -> int:
    """Require exactly two distinct team scores per completed game."""
    from src.ingestion.nflverse.refresh_dst import all_rows

    scores = all_rows(client, "fact_calculated_team_weekly_scores",
                      "game_id,team_code,season_type,week",
                      {"season_id": season, "scoring_settings_id": settings_id})
    expected = {game["game_id"] for game in completed_games}
    if len(expected) != len(completed_games):
        raise RuntimeError("Duplicate completed game IDs in verification input")
    by_game = {}
    for row in scores:
        game_id, team = row.get("game_id"), row.get("team_code")
        if game_id not in expected:
            raise RuntimeError(f"Unexpected D/ST score game: {game_id}")
        if not team or not isinstance(team, str):
            raise RuntimeError(f"Missing D/ST team code for game {game_id}")
        by_game.setdefault(game_id, []).append(team)
    failures = [
        game_id for game_id in sorted(expected)
        if len(by_game.get(game_id, [])) != 2
        or len(set(by_game.get(game_id, []))) != 2
    ]
    if failures:
        raise RuntimeError(f"D/ST score coverage failed for {len(failures)} games: {failures[:8]}")
    return len(expected)
