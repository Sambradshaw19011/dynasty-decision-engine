"""Import NFLverse PBP seasons directly to local Parquet, matching an existing archive.

Run from repository root. Never writes to PostgreSQL or Supabase Storage.
Dependencies: nflreadpy, polars, pyarrow.

Example:
 python src/ingestion/nflverse/import_nflverse_pbp_2023_2026.py --season 2023
 python src/ingestion/nflverse/import_nflverse_pbp_2023_2026.py --start-season 2023 --end-season 2026
"""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sys
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
import nflreadpy

DEFAULT_ROOT = Path('pbp_parquet_backup/consolidated')

# Destination columns are copied from a known-good existing archive schema.
# Source aliases are explicit; missing optional columns are null, but critical ones fail.
SOURCE = {
    'game_id': ('game_id',), 'play_id': ('play_id',), 'season': ('season',),
    'week': ('week',), 'season_type': ('season_type',),
    'posteam': ('posteam',), 'defteam': ('defteam',), 'play_type': ('play_type',),
    'play_description': ('desc', 'play_description'), 'quarter': ('qtr', 'quarter'),
    'down': ('down',), 'yards_to_go': ('ydstogo', 'yards_to_go'),
    'yards_gained': ('yards_gained',), 'passing_yards': ('passing_yards',),
    'rushing_yards': ('rushing_yards',), 'receiving_yards': ('receiving_yards',),
    'pass_touchdown': ('pass_touchdown',), 'rush_touchdown': ('rush_touchdown',),
    'sack': ('sack',), 'interception': ('interception',), 'fumble': ('fumble',),
    'fumble_forced': ('fumble_forced',), 'fumble_lost': ('fumble_lost',),
    'safety': ('safety',), 'forced_fumble_player_1_team': ('forced_fumble_player_1_team',),
    'forced_fumble_player_1_id': ('forced_fumble_player_1_player_id', 'forced_fumble_player_1_id'),
    'fumble_recovery_1_team': ('fumble_recovery_1_team',),
    'fumble_recovery_1_player_id': ('fumble_recovery_1_player_id',),
    'fumble_recovery_2_team': ('fumble_recovery_2_team',),
    'fumble_recovery_2_player_id': ('fumble_recovery_2_player_id',),
    'special_teams_play': ('special_teams_play',), 'st_play_type': ('st_play_type',),
    'punt_attempt': ('punt_attempt',), 'kickoff_attempt': ('kickoff_attempt',),
    'punt_blocked': ('punt_blocked',), 'return_touchdown': ('return_touchdown',),
    'touchdown': ('touchdown',), 'td_team': ('td_team',),
    'td_player_id': ('td_player_id',), 'total_home_score': ('total_home_score',),
    'total_away_score': ('total_away_score',), 'play_deleted': ('play_deleted',),
    'penalty': ('penalty',),
}
REQUIRED = {'game_id', 'play_id', 'season', 'week', 'season_type', 'posteam', 'defteam',
            'play_type', 'fumble_recovery_1_team', 'special_teams_play', 'touchdown',
            'total_home_score', 'total_away_score'}

def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def value_for(record: dict, field: str):
    for key in SOURCE[field]:
        if key in record:
            return record[key]
    return None

def normalize_json(value):
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode('utf-8', errors='replace')
    if hasattr(value, 'item'):
        return value.item()
    return str(value)

def cast_field(value, typ: pa.DataType, field_name: str):
    if value is None:
        return None
    if pa.types.is_null(typ):
        # Template's st_play_type is null; source may now have non-null values.
        # Do not silently drop them.
        raise ValueError(f'{field_name}: non-null source value cannot fit null template type')
    if pa.types.is_string(typ) or pa.types.is_large_string(typ):
        return str(value)
    if pa.types.is_integer(typ):
        if isinstance(value, float) and (not value.is_integer()):
            raise ValueError(f'{field_name}: non-integral value {value}')
        return int(value)
    if pa.types.is_floating(typ):
        return float(value)
    if pa.types.is_timestamp(typ):
        return value
    raise TypeError(f'Unsupported template type for {field_name}: {typ}')

def run(season: int, root: Path, template_path: Path):
    target = root / f'{season}.parquet'
    temp = root / f'{season}.parquet.incomplete'
    if target.exists() or temp.exists():
        raise FileExistsError(f'Refusing to overwrite existing file: {target if target.exists() else temp}')
    schema = pq.read_schema(template_path)
    frame = nflreadpy.load_pbp([season])
    if not isinstance(frame, pl.DataFrame):
        frame = pl.from_arrow(frame.to_arrow())
    if frame.height == 0:
        raise ValueError(f'NFLverse returned no plays for {season}')
    available = set(frame.columns)
    missing = sorted(f for f in REQUIRED if not any(alias in available for alias in SOURCE[f]))
    if missing:
        raise ValueError(f'Missing critical NFLverse columns: {missing}')
    print(f'{season}: NFLverse returned {frame.height:,} rows, {len(frame.columns)} source columns', flush=True)
    root.mkdir(parents=True, exist_ok=True)
    now = dt.datetime.now(dt.timezone.utc)
    seen = set()
    written = 0
    try:
        with pq.ParquetWriter(temp, schema=schema, compression='zstd') as writer:
            for start in range(0, frame.height, 2000):
                records = frame.slice(start, 2000).to_dicts()
                columns = {field.name: [] for field in schema}
                for record in records:
                    game_id, play_id = record.get('game_id'), record.get('play_id')
                    if game_id is None or play_id is None:
                        raise ValueError('Missing game_id or play_id')
                    key = (str(game_id), float(play_id))
                    if key in seen:
                        raise ValueError(f'Duplicate play key {key}')
                    seen.add(key)
                    if record.get('season') is not None and int(record['season']) != season:
                        raise ValueError(f'Unexpected season {record["season"]}')
                    for field in schema:
                        name = field.name
                        if name == 'raw_play_data':
                            val = json.dumps(record, default=normalize_json, ensure_ascii=False, allow_nan=False)
                        elif name in ('source_retrieved_at', 'updated_at'):
                            val = now
                        else:
                            val = value_for(record, name)
                        columns[name].append(cast_field(val, field.type, name))
                batch = pa.Table.from_pydict(columns, schema=schema)
                writer.write_table(batch)
                written += batch.num_rows
                print(f'  {season}: {written:,}/{frame.height:,} rows', flush=True)
        with pq.ParquetFile(temp) as check:
            actual = check.metadata.num_rows
            cols = check.schema_arrow.names
        if actual != frame.height or cols != schema.names:
            raise ValueError(f'Output verification failed: {actual} rows, {len(cols)} columns')
        os.replace(temp, target)
        digest = sha256(target)
        manifest = {'season': season, 'rows': actual, 'columns': len(cols),
                    'sha256': digest, 'file': str(target),
                    'source': 'nflreadpy.load_pbp', 'retrieved_at_utc': now.isoformat(),
                    'season_complete': season < dt.datetime.now(dt.timezone.utc).year}
        (root / f'{season}.import_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        print(f'PASS {season}: {actual:,} rows, {len(cols)} columns, SHA-256 {digest}', flush=True)
    except Exception:
        print(f'FAILED {season}; any .incomplete file retained for inspection', file=sys.stderr)
        raise

def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--season', type=int)
    mode.add_argument('--start-season', type=int)
    parser.add_argument('--end-season', type=int)
    parser.add_argument('--output-dir', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--template', type=Path, default=DEFAULT_ROOT / '2022.parquet')
    args = parser.parse_args()
    if not args.template.is_file():
        parser.error(f'Missing schema template: {args.template}')
    if args.start_season is not None and args.end_season is None:
        parser.error('--end-season is required with --start-season')
    seasons = [args.season] if args.season is not None else range(args.start_season, args.end_season + 1)
    for season in seasons:
        if season < 2023 or season > 2026:
            parser.error('This importer is limited to seasons 2023–2026')
        run(season, args.output_dir, args.template)

if __name__ == '__main__':
    main()
