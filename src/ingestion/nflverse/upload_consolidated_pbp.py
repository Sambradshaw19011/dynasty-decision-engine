"""Upload and verify locally consolidated NFLverse PBP seasons in Supabase Storage.

Run from repository root, e.g.:
  python src/ingestion/nflverse/upload_consolidated_pbp.py --season 2005
  python src/ingestion/nflverse/upload_consolidated_pbp.py --start-season 2005 --end-season 2022

Requires: pip install requests python-dotenv pyarrow
Environment: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

Does not overwrite or delete existing objects; downloads uploaded files to verify SHA-256.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
from urllib.parse import quote

import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

BUCKET = 'nflverse-data'
CHUNK = 1024 * 1024


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(CHUNK), b''):
            digest.update(chunk)
    return digest.hexdigest()


def verify_local(path, season):
    if not path.is_file():
        raise ValueError(f'Missing local consolidated file: {path}')
    with pq.ParquetFile(path) as parquet:
        rows = parquet.metadata.num_rows
        names = parquet.schema_arrow.names
        if not rows or 'raw_play_data' not in names or 'game_id' not in names or 'play_id' not in names:
            raise ValueError(f'{path}: missing required data or columns')
        readable = sum(batch.num_rows for batch in parquet.iter_batches(batch_size=4000))
        if readable != rows:
            raise ValueError(f'{path}: unreadable/incomplete rows: {readable}/{rows}')
    return rows, len(names)


def download_hash(session, url):
    digest = hashlib.sha256()
    total = 0
    with session.get(url, stream=True, timeout=(30, 180)) as response:
        response.raise_for_status()
        for block in response.iter_content(chunk_size=CHUNK):
            if block:
                digest.update(block)
                total += len(block)
    return digest.hexdigest(), total


def upload_one(session, base, source, season):
    rows, cols = verify_local(source, season)
    local_sha = sha256_file(source)
    size = source.stat().st_size
    object_path = f'play_by_play/{season}.parquet'
    object_url = f'{base}/storage/v1/object/{BUCKET}/{quote(object_path, safe="/")}'
    print(f'{season}: {rows:,} rows, {cols} columns, {size / 1024 / 1024:.1f} MiB')

    # Probe for existing object. Never overwrite one silently.
    with session.get(object_url, stream=True, timeout=(30, 180)) as probe:
        if probe.status_code == 200:
            print(f'  Existing object detected; verifying without overwriting: {object_path}')
            existing = True
        elif probe.status_code in (400, 404):
            # Supabase Storage may use 400 or 404 for a missing object.
            body = probe.text[:500]
            if probe.status_code == 400 and not any(word in body.lower() for word in ('not found', 'not_found', 'object not found')):
                raise RuntimeError(f'Cannot determine object existence ({probe.status_code}): {body}')
            existing = False
        else:
            raise RuntimeError(f'Object existence check failed ({probe.status_code}): {probe.text[:500]}')

    if not existing:
        with source.open('rb') as data:
            response = session.post(
                object_url,
                data=data,
                headers={'Content-Type': 'application/octet-stream', 'x-upsert': 'false'},
                timeout=(30, 600),
            )
        if response.status_code not in (200, 201):
            raise RuntimeError(
                f'Upload failed ({response.status_code}): {response.text[:800]}\n'
                'No local files were changed. If this is an upload-size limit, '
                'we need a resumable upload for this season.'
            )
        print(f'  Uploaded: {object_path}')

    remote_sha, remote_size = download_hash(session, object_url)
    if remote_sha != local_sha or remote_size != size:
        raise RuntimeError(
            f'VERIFICATION FAILED for {object_path}: '
            f'local sha={local_sha}, remote sha={remote_sha}, '
            f'local bytes={size}, remote bytes={remote_size}. '
            'Do not delete any source files.'
        )
    print(f'  PASS: SHA-256 verified ({local_sha})')
    return {'season': season, 'object_path': object_path, 'rows': rows,
            'columns': cols, 'bytes': size, 'sha256': local_sha}


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--season', type=int)
    group.add_argument('--start-season', type=int)
    parser.add_argument('--end-season', type=int)
    parser.add_argument('--root', type=Path, default=Path('pbp_parquet_backup/consolidated'))
    args = parser.parse_args()
    if args.season is not None:
        seasons = [args.season]
    else:
        if args.end_season is None or args.end_season < args.start_season:
            parser.error('--start-season requires --end-season >= --start-season')
        seasons = range(args.start_season, args.end_season + 1)
    load_dotenv(dotenv_path=Path('.env'), override=True)
    base = os.getenv('SUPABASE_URL', '').rstrip('/')
    key = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
    if not base.startswith('https://') or not key:
        raise RuntimeError('Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in repository .env')
    session = requests.Session()
    session.headers.update({'apikey': key, 'Authorization': f'Bearer {key}'})
    manifest_path = args.root / 'storage_upload_manifest.json'
    manifest = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    for season in seasons:
        result = upload_one(session, base, args.root / f'{season}.parquet', season)
        manifest[str(season)] = result
        temp = manifest_path.with_suffix('.json.tmp')
        temp.write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
        os.replace(temp, manifest_path)
    print('\nFinished. No source files or existing Storage objects were deleted or overwritten.')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, ValueError, requests.RequestException, OSError) as exc:
        print(f'ERROR: {exc}')
        raise SystemExit(1)
