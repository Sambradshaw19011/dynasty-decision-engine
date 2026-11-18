#!/usr/bin/env python3
"""Audit and optionally delete redundant NFLverse PBP season partitions in Supabase Storage.

Install: pip install supabase duckdb
Environment: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY with delete permission).
Default is read-only. See --help for guarded deletion workflow.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile

import duckdb
from supabase import create_client

BUCKET = 'nflverse-data'
ROOT = 'play_by_play'
SEASONS = range(2005, 2023)
EXTENSIONS = {'.py', '.sql', '.yaml', '.yml', '.toml', '.json', '.md', '.sh', '.ps1'}
IGNORE_DIRS = {'.git', '.venv', 'venv', 'node_modules', '__pycache__', '.next', 'dist', 'build'}


def listing(storage, prefix):
    """List every object directly inside a prefix using pagination."""
    out, offset, limit = [], 0, 100
    while True:
        page = storage.list(prefix, {'limit': limit, 'offset': offset, 'sortBy': {'column': 'name', 'order': 'asc'}})
        if not isinstance(page, list):
            raise RuntimeError(f'Unexpected storage listing for {prefix}: {page!r}')
        out.extend(page)
        if len(page) < limit:
            return out
        offset += limit


def download(storage, object_path, destination):
    result = storage.download(object_path)
    if not isinstance(result, (bytes, bytearray)):
        raise RuntimeError(f'Unexpected download response for {object_path}')
    destination.write_bytes(result)


def dependencies(repo):
    """Conservatively flag source references to partition layout."""
    matches = []
    if not repo.exists():
        return [f'Repository not found: {repo}']
    regex = re.compile(r'play_by_play/season=|play_by_play.*season\s*=|season=\{.*\}|season=\d{4}|partition.*play.by.play', re.I)
    for base, dirs, files in os.walk(repo):
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        for name in files:
            p = Path(base) / name
            if p.name == 'audit_pbp_storage.py' or p.suffix.lower() not in EXTENSIONS or p.stat().st_size > 2_000_000:
                continue
            try:
                for i, line in enumerate(p.read_text(encoding='utf-8').splitlines(), 1):
                    if regex.search(line):
                        matches.append(f'{p.relative_to(repo)}:{i}: {line.strip()[:180]}')
            except (UnicodeError, OSError):
                continue
    return matches


def compare(consolidated, parts):
    """Exact bag comparison with EXCEPT ALL, including duplicate multiplicities."""
    conn = duckdb.connect(':memory:')
    try:
        conn.execute('SET threads=2')
        def literal(path):
            return "'" + str(path).replace("'", "''") + "'"
        p_list = '[' + ','.join(literal(p) for p in parts) + ']'
        a = f'read_parquet({literal(consolidated)})'
        b = f'read_parquet({p_list}, union_by_name=true)'
        # Compare columns and types, independent of column order.
        a_schema = {r[0]: r[1] for r in conn.execute(f'DESCRIBE SELECT * FROM {a}').fetchall()}
        b_schema = {r[0]: r[1] for r in conn.execute(f'DESCRIBE SELECT * FROM {b}').fetchall()}
        if a_schema != b_schema:
            return {'equal': False, 'reason': 'schema_mismatch', 'consolidated_schema': a_schema, 'partition_schema': b_schema}
        columns = sorted(a_schema)
        quote = lambda col: '"' + col.replace('"', '""') + '"'
        projection = ','.join(quote(c) for c in columns)
        a_count = conn.execute(f'SELECT COUNT(*) FROM {a}').fetchone()[0]
        b_count = conn.execute(f'SELECT COUNT(*) FROM {b}').fetchone()[0]
        if a_count != b_count:
            return {'equal': False, 'reason': 'row_count_mismatch', 'consolidated_rows': a_count, 'partition_rows': b_count}
        # Full-row comparison in both directions. EXCEPT ALL preserves duplicate frequencies.
        # Sorting columns avoids false mismatches caused by column ordering.
        diff_ab = conn.execute(f'SELECT EXISTS(SELECT {projection} FROM {a} EXCEPT ALL SELECT {projection} FROM {b})').fetchone()[0]
        diff_ba = conn.execute(f'SELECT EXISTS(SELECT {projection} FROM {b} EXCEPT ALL SELECT {projection} FROM {a})').fetchone()[0]
        return {'equal': not(diff_ab or diff_ba), 'reason': 'identical' if not(diff_ab or diff_ba) else 'content_mismatch',
                'consolidated_rows': a_count, 'partition_rows': b_count, 'columns': len(columns)}
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd(), help='Repository root for dependency scan')
    parser.add_argument('--report', type=Path, default=Path('pbp_cleanup_audit.json'))
    parser.add_argument('--delete', action='store_true', help='Enable guarded deletion after successful audit')
    parser.add_argument('--confirm', help='Must be exactly DELETE-VERIFIED-PBP-PARTITIONS')
    parser.add_argument('--allow-dependencies', action='store_true', help='Explicitly acknowledge and accept flagged repository dependencies')
    args = parser.parse_args()
    if args.delete and args.confirm != 'DELETE-VERIFIED-PBP-PARTITIONS':
        parser.error('--delete requires --confirm DELETE-VERIFIED-PBP-PARTITIONS')
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or os.environ.get('SUPABASE_KEY')
    if not url or not key:
        parser.error('Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY)')
    storage = create_client(url, key).storage.from_(BUCKET)
    dep = dependencies(args.repo.resolve())
    if dep:
        print(f'WARNING: {len(dep)} possible partition-path dependencies. Review the JSON report before deletion.')
    results = []
    with tempfile.TemporaryDirectory(prefix='pbp-audit-') as tmp:
        for year in SEASONS:
            consolidated_path = f'{ROOT}/{year}.parquet'
            prefix = f'{ROOT}/season={year}'
            record = {'season': year, 'consolidated_path': consolidated_path, 'partition_prefix': prefix,
                      'safe_to_delete': False, 'partition_paths': []}
            print(f'[{year}] auditing...', flush=True)
            try:
                # Validate existence by download rather than relying on parent listing.
                local = Path(tmp) / f'{year}.parquet'
                download(storage, consolidated_path, local)
                objects = listing(storage, prefix)
                files = [f'{prefix}/{o["name"]}' for o in objects if o.get('name', '').endswith('.parquet') and o.get('id')]
                unexpected = [o.get('name') for o in objects if not (o.get('name', '').endswith('.parquet') and o.get('id'))]
                record['partition_paths'] = files
                record['unexpected_entries'] = unexpected
                if not files or unexpected:
                    raise RuntimeError(f'No partitions or unexpected entries: {unexpected}')
                part_dir = Path(tmp) / f'parts_{year}'
                part_dir.mkdir()
                local_parts = []
                for idx, remote in enumerate(files):
                    target = part_dir / f'{idx:04d}.parquet'
                    download(storage, remote, target)
                    local_parts.append(target)
                outcome = compare(local, local_parts)
                record.update(outcome)
                record['safe_to_delete'] = bool(outcome['equal'])
                record['partition_bytes_downloaded'] = sum(p.stat().st_size for p in local_parts)
                print(f'[{year}] {outcome["reason"]}; {len(files)} partitions')
            except Exception as exc:
                record['error'] = str(exc)
                print(f'[{year}] FAILED: {exc}', file=sys.stderr)
            results.append(record)
            # Remove temp season data before processing next season.
            if 'part_dir' in locals() and part_dir.name == f'parts_{year}':
                for p in part_dir.glob('*'): p.unlink()
                part_dir.rmdir()
            if 'local' in locals() and local.name == f'{year}.parquet' and local.exists(): local.unlink()
    report = {'timestamp_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'bucket': BUCKET,
              'repo': str(args.repo.resolve()), 'dependency_matches': dep, 'seasons': results,
              'deletion_requested': args.delete, 'deleted_paths': [], 'deletion_errors': []}
    # Persist audit BEFORE any destructive operation.
    args.report.write_text(json.dumps(report, indent=2), encoding='utf-8')
    verified = [r for r in results if r['safe_to_delete']]
    print(f'\nVerified: {len(verified)}/18 seasons. Audit: {args.report}')
    if not args.delete:
        print('READ-ONLY MODE: nothing deleted. Review report, then rerun with --delete --confirm DELETE-VERIFIED-PBP-PARTITIONS.')
        return 0 if len(verified) == 18 else 2
    if len(verified) != 18:
        print('DELETE BLOCKED: all 18 seasons must pass verification.', file=sys.stderr)
        return 3
    if dep and not args.allow_dependencies:
        print('DELETE BLOCKED: possible repository dependencies found. Update them first, or explicitly use --allow-dependencies after review.', file=sys.stderr)
        return 4
    # Re-list and verify exact object sets before deleting to avoid deleting newly added files.
    for r in verified:
        current = listing(storage, r['partition_prefix'])
        paths = sorted(f'{r["partition_prefix"]}/{o["name"]}' for o in current if o.get('id'))
        if paths != sorted(r['partition_paths']):
            print(f'DELETE BLOCKED: objects changed since audit for {r["season"]}', file=sys.stderr)
            return 5
    print('Deleting only exact verified partition paths. Consolidated files are never deletion targets.')
    for r in verified:
        paths = r['partition_paths']
        for start in range(0, len(paths), 100):
            batch = paths[start:start+100]
            try:
                storage.remove(batch)
                report['deleted_paths'].extend(batch)
                print(f'Deleted {len(batch)} objects for {r["season"]}')
            except Exception as exc:
                report['deletion_errors'].append({'season': r['season'], 'error': str(exc), 'paths': batch})
                print(f'DELETION ERROR {r["season"]}: {exc}', file=sys.stderr)
                break
            finally:
                args.report.write_text(json.dumps(report, indent=2), encoding='utf-8')
        if report['deletion_errors']:
            break
    # Post-delete: check that all consolidated objects still download.
    report['consolidated_postcheck'] = {}
    for year in SEASONS:
        try:
            storage.download(f'{ROOT}/{year}.parquet')
            report['consolidated_postcheck'][str(year)] = 'present'
        except Exception as exc:
            report['consolidated_postcheck'][str(year)] = f'ERROR: {exc}'
    args.report.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(f'Finished: {len(report["deleted_paths"])} objects removed; report: {args.report}')
    return 0 if not report['deletion_errors'] and all(v == 'present' for v in report['consolidated_postcheck'].values()) else 6


if __name__ == '__main__':
    sys.exit(main())
