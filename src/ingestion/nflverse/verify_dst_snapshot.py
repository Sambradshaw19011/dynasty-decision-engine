"""Verify a local D/ST database snapshot without modifying Supabase.

Usage:
  python -m src.ingestion.nflverse.verify_dst_snapshot backups/dst_database/2026/<sha256>.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

TABLE_KEYS = {
    "fact_team_fumble_events": ("fumble_event_id",),
    "fact_calculated_team_weekly_scores": ("calculated_team_score_id",),
}
NATURAL_KEYS = {
    "fact_team_fumble_events": ("game_id", "play_id", "recovery_sequence"),
    "fact_calculated_team_weekly_scores": ("scoring_settings_id", "game_id", "team_code"),
}


def verify_snapshot(path: Path) -> dict:
    if not path.is_file():
        raise RuntimeError(f"Snapshot not found: {path}")
    if not re.fullmatch(r"[0-9a-f]{64}\\.json", path.name):
        raise RuntimeError("Snapshot filename must be its SHA256 digest")
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    if path.name != f"{digest}.json":
        raise RuntimeError("Snapshot SHA256 mismatch; refusing recovery")
    document = json.loads(payload)
    season = document.get("season")
    if type(season) is not int or not 2000 <= season <= 2100:
        raise RuntimeError("Invalid snapshot season")
    if set(document.get("tables", {})) != set(TABLE_KEYS):
        raise RuntimeError("Snapshot must contain exactly the two D/ST production tables")
    for name, primary_key in TABLE_KEYS.items():
        table = document["tables"][name]
        if table.get("season_column") != "season_id":
            raise RuntimeError(f"{name}: incorrect season column")
        rows = table.get("rows")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError(f"{name}: missing or empty snapshot rows")
        if table.get("row_count") != len(rows):
            raise RuntimeError(f"{name}: row count mismatch")
        for columns in (primary_key, NATURAL_KEYS[name]):
            seen = set()
            for row in rows:
                if not isinstance(row, dict) or row.get("season_id") != season:
                    raise RuntimeError(f"{name}: invalid row or season")
                key = tuple(row.get(column) for column in columns)
                if any(value is None for value in key) or key in seen:
                    raise RuntimeError(f"{name}: null or duplicate key {columns}")
                seen.add(key)
        print(f"VERIFIED: {name}: {len(rows)} rows")
    print(f"SNAPSHOT VALIDATION PASS: season={season}; SHA256={digest}")
    print("READ ONLY: no database connections or changes were made")
    print("This validates the export, not foreign keys or database restore behavior.")
    return document


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    args = parser.parse_args()
    verify_snapshot(args.snapshot)


if __name__ == "__main__":
    main()
