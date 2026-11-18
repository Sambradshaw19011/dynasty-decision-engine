"""Consolidate locally backed-up NFLverse Parquet parts, one file per season.

Read-only source folders. Outputs to pbp_parquet_backup/consolidated/YYYY.parquet.
Requires: pip install pyarrow
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import pyarrow as pa
import pyarrow.parquet as pq


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def consolidate(root: Path, season: int, batch_size: int) -> None:
    source_dir = root / str(season)
    if not source_dir.is_dir():
        raise ValueError(f'Missing season folder: {source_dir}')
    parts = sorted(source_dir.glob('part-*.parquet'))
    if not parts:
        raise ValueError(f'No part-*.parquet files in {source_dir}')
    # Detect gaps, rather than silently combining an incomplete season.
    part_numbers = []
    for p in parts:
        match = re.fullmatch(r'part-(\d+)\.parquet', p.name)
        if match:
            part_numbers.append(int(match.group(1)))
    if len(part_numbers) != len(parts) or part_numbers != list(range(1, len(parts) + 1)):
        raise ValueError(f'Season {season}: missing or unexpected part numbers: {part_numbers[:8]}...')

    output_dir = root / 'consolidated'
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f'{season}.parquet'
    temp = output_dir / f'{season}.parquet.incomplete'
    if target.exists() or temp.exists():
        raise FileExistsError(f'Output already exists; refusing to overwrite: {target if target.exists() else temp}')

    # Determine a common schema before writing. Null-only columns in individual
    # parts must be promoted to the actual type seen elsewhere in the season.
    schemas = [pq.read_schema(p).remove_metadata() for p in parts]
    try:
        schema = pa.unify_schemas(schemas, promote_options='permissive')
    except pa.ArrowException as exc:
        raise ValueError(f'Season {season}: incompatible source schemas: {exc}') from exc
    # Reject missing columns: the source export should have the same fields in every part.
    expected_names = set(schemas[0].names)
    for part, part_schema in zip(parts, schemas):
        if set(part_schema.names) != expected_names:
            raise ValueError(f'Season {season}: missing/extra columns in {part.name}')
    expected_rows = 0
    actual_rows = 0
    writer = None
    seen_keys = set()
    check_keys = 'game_id' in schema.names and 'play_id' in schema.names
    if not check_keys:
        print('WARNING: game_id/play_id not both present; duplicate-key check skipped')
    print(f'  {season}: unified schema across {len(parts)} parts ({len(schema.names)} columns)', flush=True)
    try:
        for index, part in enumerate(parts, 1):
            pf = pq.ParquetFile(part)
            expected_rows += pf.metadata.num_rows
            if writer is None:
                writer = pq.ParquetWriter(temp, schema, compression='zstd')
            for batch in pf.iter_batches(batch_size=batch_size):
                # Cast each batch into the unified schema, matching columns by name.
                table = pa.Table.from_batches([batch]).cast(schema, safe=True)
                if check_keys:
                    game_ids = table.column('game_id').to_pylist()
                    play_ids = table.column('play_id').to_pylist()
                    for key in zip(game_ids, play_ids):
                        if key in seen_keys:
                            raise ValueError(f'Duplicate (game_id, play_id) in {season}: {key}')
                        seen_keys.add(key)
                writer.write_table(table)
                actual_rows += batch.num_rows
            print(f'  {season}: {index}/{len(parts)} parts, {actual_rows:,} rows', flush=True)
        writer.close()
        writer = None
        merged = pq.ParquetFile(temp)

        try:
            merged_rows = merged.metadata.num_rows
            merged_schema = merged.schema_arrow

            if expected_rows != actual_rows or merged_rows != expected_rows:
                raise ValueError(
                    f'Row count mismatch: source={expected_rows}, '
                    f'written={actual_rows}, merged={merged_rows}'
                )

            if not merged_schema.equals(schema, check_metadata=False):
                raise ValueError('Merged schema differs from source')

        finally:
            merged.close()

        os.replace(temp, target)
        manifest = {
            'season': season,
            'source_part_count': len(parts),
            'row_count': expected_rows,
            'unique_game_play_keys_checked': check_keys,
            'column_count': len(schema.names),
            'output': target.name,
            'sha256': sha256(target),
            'source_files': [p.name for p in parts],
        }
        (output_dir / f'{season}.manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        print(f'PASS {season}: {expected_rows:,} rows, {len(parts)} parts -> {target} (SHA-256: {manifest["sha256"]})')
    except Exception:
        if writer is not None:
            writer.close()
        if temp.exists():
            temp.unlink()
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('pbp_parquet_backup'))
    parser.add_argument('--season', type=int)
    parser.add_argument('--start-season', type=int)
    parser.add_argument('--end-season', type=int)
    parser.add_argument('--batch-size', type=int, default=5000)
    args = parser.parse_args()
    if args.season is not None:
        seasons = [args.season]
    elif args.start_season is not None and args.end_season is not None and args.start_season <= args.end_season:
        seasons = range(args.start_season, args.end_season + 1)
    else:
        parser.error('Provide --season YEAR or --start-season YEAR --end-season YEAR')
    for season in seasons:
        consolidate(args.root, season, args.batch_size)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'ERROR: {exc}', file=sys.stderr)
        sys.exit(1)
