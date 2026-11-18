"""Load NFLverse play-by-play into public.fact_play_by_play.

Usage:
    python src/ingestion/nflverse/load_play_by_play.py --season 2005

Requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in project-root .env.
"""

import argparse
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import nflreadpy
from dotenv import load_dotenv
from supabase import create_client
from postgrest.exceptions import APIError

LOG = logging.getLogger('pbp_loader')

COLUMN_MAP = {
    'game_id': 'game_id', 'play_id': 'play_id', 'season': 'season',
    'week': 'week', 'season_type': 'season_type', 'posteam': 'posteam',
    'defteam': 'defteam', 'play_type': 'play_type', 'desc': 'play_description',
    'qtr': 'quarter', 'down': 'down', 'ydstogo': 'yards_to_go',
    'yards_gained': 'yards_gained', 'passing_yards': 'passing_yards',
    'rushing_yards': 'rushing_yards', 'receiving_yards': 'receiving_yards',
    'pass_touchdown': 'pass_touchdown', 'rush_touchdown': 'rush_touchdown',
    'sack': 'sack', 'interception': 'interception', 'fumble': 'fumble',
    'fumble_forced': 'fumble_forced', 'fumble_lost': 'fumble_lost',
    'safety': 'safety',
    'forced_fumble_player_1_team': 'forced_fumble_player_1_team',
    'forced_fumble_player_1_player_id': 'forced_fumble_player_1_id',
    'fumble_recovery_1_team': 'fumble_recovery_1_team',
    'fumble_recovery_1_player_id': 'fumble_recovery_1_player_id',
    'fumble_recovery_2_team': 'fumble_recovery_2_team',
    'fumble_recovery_2_player_id': 'fumble_recovery_2_player_id',
    'special_teams_play': 'special_teams_play', 'st_play_type': 'st_play_type',
    'punt_attempt': 'punt_attempt', 'kickoff_attempt': 'kickoff_attempt',
    'punt_blocked': 'punt_blocked', 'return_touchdown': 'return_touchdown',
    'touchdown': 'touchdown', 'td_team': 'td_team', 'td_player_id': 'td_player_id',
    'total_home_score': 'total_home_score',
    'total_away_score': 'total_away_score',
    'play_deleted': 'play_deleted', 'penalty': 'penalty',
}


def clean(value):
    """Convert Polars/Python values to JSON-safe types, preserving nulls."""
    if value is None:
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, dict):
        return {str(key): clean(item) for key, item in value.items()}
    return value


INTEGER_COLUMNS = {'season', 'week', 'quarter', 'down', 'yards_to_go',
                   'total_home_score', 'total_away_score'}


def integer_or_none(value, column):
    if value is None:
        return None
    number = float(value)
    if not math.isfinite(number) or not number.is_integer():
        raise ValueError(f'Invalid integer for {column}: {value!r}')
    return int(number)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--season', type=int, default=2005)
    parser.add_argument('--batch-size', type=int, default=50)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if not 2005 <= args.season <= datetime.now(timezone.utc).year:
        parser.error('season must be between 2005 and the current year')
    if args.batch_size < 1:
        parser.error('batch-size must be positive')

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    project_root = Path(__file__).resolve().parents[3]
    load_dotenv(project_root / '.env')

    LOG.info('Downloading NFLverse PBP for %s', args.season)
    frame = nflreadpy.load_pbp(seasons=[args.season])
    expected = frame.height
    if not expected:
        raise RuntimeError('No plays returned; refusing to import')
    required = set(COLUMN_MAP)
    missing_columns = sorted(required - set(frame.columns))
    if missing_columns:
        raise RuntimeError(f'Source missing expected columns: {missing_columns}')

    if frame.select('game_id').null_count().item() or frame.select('play_id').null_count().item():
        raise RuntimeError('Null play identifiers found')
    unique_count = frame.select(['game_id', 'play_id']).unique().height
    if unique_count != expected:
        raise RuntimeError(f'Duplicate play keys: {expected - unique_count}')

    if args.dry_run:
        LOG.info('DRY RUN: %s plays, %s source fields, no database writes', expected, len(frame.columns))
        return

    client = create_client(os.environ['SUPABASE_URL'], os.environ['SUPABASE_SERVICE_ROLE_KEY'])
    game_ids = set(frame['game_id'].unique().to_list())
    games_response = (client.table('dim_game').select('game_id')
                      .eq('season_id', args.season).limit(1000).execute())
    db_games = {r['game_id'] for r in games_response.data}
    missing_games = sorted(game_ids - db_games)
    if missing_games:
        raise RuntimeError(f'{len(missing_games)} missing dim_game IDs: {missing_games[:20]}')

    LOG.info('Validated %s plays and %s games; starting upload', expected, len(game_ids))
    uploaded = 0
    retrieved_at = datetime.now(timezone.utc).isoformat()
    for offset in range(0, expected, args.batch_size):
        rows = frame.slice(offset, args.batch_size).to_dicts()
        payload = []
        for row in rows:
            raw = {k: clean(v) for k, v in row.items()}
            item = {target: raw[source] for source, target in COLUMN_MAP.items()}
            for column in INTEGER_COLUMNS:
                item[column] = integer_or_none(item[column], column)
            item['raw_play_data'] = raw
            item['source_retrieved_at'] = retrieved_at
            item['updated_at'] = retrieved_at
            payload.append(item)
        for attempt in range(1, 5):
            try:
                (client.table('fact_play_by_play')
                 .upsert(payload, on_conflict='game_id,play_id')
                 .execute())
                break
            except APIError as exc:
                # Permanent SQL/data/permission errors cannot be fixed by retrying.
                if str(exc.code).startswith(('22', '23', '42')) or exc.code in ('42501',):
                    LOG.error('Non-retryable database error at offset %s: %s', offset, exc)
                    raise
                if attempt == 4:
                    LOG.exception('Failed batch at offset %s', offset)
                    raise
                delay = 2 ** attempt
                LOG.warning('Batch %s failed (attempt %s); retrying in %ss', offset, attempt, delay)
                time.sleep(delay)
            except Exception:
                if attempt == 4:
                    LOG.exception('Failed batch at offset %s; earlier batches remain committed', offset)
                    raise
                delay = 2 ** attempt
                LOG.warning('Batch %s failed (attempt %s); retrying in %ss', offset, attempt, delay)
                time.sleep(delay)
        uploaded += len(payload)
        if uploaded % (args.batch_size * 20) == 0 or uploaded == expected:
            LOG.info('Uploaded %s / %s plays', uploaded, expected)
    LOG.info('Complete: %s play records upserted for %s', uploaded, args.season)
    LOG.info('Verify in SQL: SELECT count(*) FROM public.fact_play_by_play WHERE season = %s;', args.season)


if __name__ == '__main__':
    main()
