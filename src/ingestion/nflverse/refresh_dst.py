"""Refresh current-season PBP fumbles and recalculate all completed D/ST weeks.

Requires the deployed import-pbp-fumbles Edge Function, current scoring RPC,
and GitHub secrets documented in docs/weekly-dst-refresh.md.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import nflreadpy as nfl
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv
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



FUMBLE_FIELDS = (
    "game_id", "play_id", "season", "week", "season_type",
    "posteam", "defteam", "play_type", "play_description",
    "fumble", "fumble_forced", "fumble_lost", "safety",
    "forced_fumble_player_1_team", "forced_fumble_player_1_id",
    "fumble_recovery_1_team", "fumble_recovery_1_player_id",
    "fumble_recovery_2_team", "fumble_recovery_2_player_id",
    "special_teams_play", "st_play_type", "punt_attempt",
    "kickoff_attempt", "punt_blocked", "return_touchdown",
    "touchdown", "td_team", "play_deleted", "penalty",
)
RAW_FUMBLE_FIELDS = (
    "fumbled_1_player_id", "fumbled_1_team", "fumbled_2_player_id",
    "fumbled_2_team", "fumble_recovery_1_team", "fumble_recovery_2_team",
    "fumble_recovery_1_player_id", "fumble_recovery_2_player_id",
    "forced_fumble_player_1_player_id", "forced_fumble_player_1_team",
    "forced_fumble_player_2_player_id", "forced_fumble_player_2_team",
    "fumble_not_forced", "fumble_out_of_bounds", "lateral_recovery",
    "play_type_nfl", "desc", "st_play_type", "special_teams_play",
    "fumble", "fumble_forced", "fumble_lost", "safety", "play_deleted",
)


def compare_fumble_inputs(existing_bytes, generated_path):
    """Read-only comparison of fumble classifier inputs by unique play key."""
    import io

    old_file = pq.ParquetFile(io.BytesIO(existing_bytes))
    new_file = pq.ParquetFile(generated_path)
    fields = list(FUMBLE_FIELDS) + ["raw_play_data"]
    for label, parquet in (("existing", old_file), ("generated", new_file)):
        missing = set(fields) - set(parquet.schema_arrow.names)
        if missing:
            raise RuntimeError(f"{label} Parquet missing comparison fields: {sorted(missing)}")

    def normalize(value):
        if value is None:
            return None
        if isinstance(value, float):
            if value != value:
                return None
            if value == 0:
                return 0
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, default=str)
        return value

    def collect(parquet):
        indexed = {}
        for batch in parquet.iter_batches(batch_size=1000, columns=fields):
            for row in batch.to_pylist():
                key = (row["game_id"], row["play_id"])
                if key in indexed:
                    raise RuntimeError(f"Duplicate play key in comparison: {key}")
                raw = json.loads(row.get("raw_play_data") or "{}")
                relevant = {field: normalize(row.get(field)) for field in FUMBLE_FIELDS}
                relevant["raw_fumble_fields"] = {
                    field: normalize(raw.get(field)) for field in RAW_FUMBLE_FIELDS
                }
                indexed[key] = relevant
        return indexed

    try:
        old, new = collect(old_file), collect(new_file)
    finally:
        old_file.close()
        new_file.close()
    added, removed = set(new) - set(old), set(old) - set(new)
    changed = [key for key in old.keys() & new.keys() if old[key] != new[key]]
    sensitive = re.compile(r"fumb|muff|recover|loose ball|safety|blocked|turnover", re.I)
    description_only = []
    blocking = []
    for key in changed:
        differing = {name for name in old[key] if old[key][name] != new[key][name]}
        raw_old = old[key]["raw_fumble_fields"]
        raw_new = new[key]["raw_fumble_fields"]
        raw_diff = {name for name in RAW_FUMBLE_FIELDS if raw_old[name] != raw_new[name]}
        if (
            differing <= {"play_description", "raw_fumble_fields"}
            and raw_diff <= {"desc"}
            and not sensitive.search(str(old[key]["play_description"] or ""))
            and not sensitive.search(str(new[key]["play_description"] or ""))
        ):
            description_only.append(key)
        else:
            blocking.append(key)
    print(
        f"FUMBLE INPUT COMPARISON: old={len(old):,} new={len(new):,} "
        f"added={len(added)} removed={len(removed)} changed={len(changed)}",
        flush=True,
    )
    for key in sorted(changed)[:10]:
        differences = [
            field for field in old[key] if old[key][field] != new[key][field]
        ]
        print(f"  Changed {key}: {', '.join(differences)}", flush=True)
        if "raw_fumble_fields" in differences:
            old_raw = old[key]["raw_fumble_fields"]
            new_raw = new[key]["raw_fumble_fields"]
            raw_differences = [
                name for name in RAW_FUMBLE_FIELDS
                if old_raw[name] != new_raw[name]
            ]
            print(f"    Raw fields changed: {', '.join(raw_differences)}", flush=True)
            for name in raw_differences[:8]:
                print(
                    f"    {name}: existing={old_raw[name]!r}, generated={new_raw[name]!r}",
                    flush=True,
                )
        if "play_description" in differences:
            old_desc = old[key]["play_description"]
            new_desc = new[key]["play_description"]
            print(f"    Description: existing={old_desc!r}", flush=True)
            print(f"                 generated={new_desc!r}", flush=True)
    for key in sorted(added)[:5]:
        print(f"  Added {key}", flush=True)
    for key in sorted(removed)[:5]:
        print(f"  Removed {key}", flush=True)
    print(f"Description-only non-fumble updates: {len(description_only)}; blocking changes: {len(blocking)}", flush=True)
    # New weeks naturally add play IDs. Their contents must be validated by
    # the staging classifier before any production promotion.
    if removed or blocking:
        raise RuntimeError(
            "Existing plays were removed or fumble-sensitive inputs changed; "
            "do not promote until validated. "
            f"removed={len(removed)} blocking={len(blocking)}"
        )
    print(
        f"FUMBLE INPUT COMPARISON PASS: {len(added)} new plays; "
        "no removed plays or blocking changes",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='Upload and import after local verification')
    parser.add_argument('--stage', action='store_true', help='Upload generated Parquet to staging only; no import or scoring')
    parser.add_argument('--audit-staging', action='store_true', help='Stage and audit generated Parquet using the deployed Edge Function')
    parser.add_argument('--verify-backup', action='store_true', help='Verify immutable backup of current production Parquet; do not promote or score')
    parser.add_argument('--verify-recovery', action='store_true', help='Simulate restoring verified production backup to an isolated staging object; no production writes')
    parser.add_argument('--snapshot-db', action='store_true', help='Export and verify current-season D/ST event and score rows locally; no database writes')
    args = parser.parse_args()
    if sum((args.stage, args.audit_staging, args.verify_backup, args.verify_recovery, args.snapshot_db, args.execute)) > 1:
        parser.error('Choose only one of --stage, --audit-staging, --verify-backup, --verify-recovery, --snapshot-db, or --execute')
    if args.execute:
        raise RuntimeError(
            "Production --execute remains disabled: coordinated recovery and "
            "failure-injection testing are not complete. No production changes made."
        )
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[3] / '.env', override=False)
    url = required("SUPABASE_URL").rstrip("/")
    key = required("SUPABASE_SERVICE_ROLE_KEY")
    secret = required("PBP_INSPECT_SECRET") if (args.execute or args.audit_staging) else None
    settings_id = required("DST_SCORING_SETTINGS_ID") if args.execute else None
    client = create_client(url, key)

    seasons = all_rows(client, "dim_season", "season_id", {"is_current": True})
    if len(seasons) != 1:
        raise RuntimeError(f"Expected one current season; found {len(seasons)}")
    season = int(seasons[0]["season_id"])
    print(f"Refreshing {season}", flush=True)
    # Fail closed before any future production promotion if an earlier run
    # was interrupted. This check is read-only and also runs in dry-run mode.
    unfinished = (
        client.table("dst_refresh_operations")
        .select("operation_id,status,created_at")
        .eq("season_id", season)
        .not_.in_("status", ["recovered", "completed"])
        .execute().data or []
    )
    if unfinished:
        print("UNFINISHED D/ST REFRESH:", json.dumps(unfinished, indent=2), flush=True)
        if args.execute:
            raise RuntimeError("Unfinished D/ST operation: recovery required before promotion")
        print("WARNING: production refresh is blocked pending recovery", flush=True)
    else:
        print("REFRESH JOURNAL CHECK PASS: no unfinished operations", flush=True)
    if args.snapshot_db:
        # The fumble event table must be configured explicitly; guessing its
        # name could silently omit the data we need to preserve.
        event_table = required("DST_FUMBLE_EVENTS_TABLE")
        if not re.fullmatch(r"[a-z][a-z0-9_]*", event_table):
            raise RuntimeError("DST_FUMBLE_EVENTS_TABLE must be a simple table identifier")
        if event_table == "fact_calculated_team_weekly_scores":
            raise RuntimeError("Event and scoring tables must be distinct")
        snapshot_tables = (
            (event_table, "season_id"),
            ("fact_calculated_team_weekly_scores", "season_id"),
        )
        snapshot = {
            "season": season,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tables": {},
        }
        for table_name, season_column in snapshot_tables:
            rows = all_rows(client, table_name, "*", {season_column: season})
            # Sort canonically for reproducible digest; preserve every column.
            rows.sort(key=lambda row: json.dumps(row, sort_keys=True, default=str))
            snapshot["tables"][table_name] = {
                "season_column": season_column,
                "row_count": len(rows),
                "rows": rows,
            }
            print(f"SNAPSHOT READ: {table_name} ({len(rows):,} rows)", flush=True)
        snapshot_bytes = json.dumps(
            snapshot, ensure_ascii=False, sort_keys=True, indent=2, default=str,
        ).encode("utf-8")
        snapshot_dir = Path("backups") / "dst_database" / str(season)
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_digest = hashlib.sha256(snapshot_bytes).hexdigest()
        snapshot_path = snapshot_dir / f"{snapshot_digest}.json"
        if snapshot_path.exists():
            if hashlib.sha256(snapshot_path.read_bytes()).hexdigest() != snapshot_digest:
                raise RuntimeError("Existing database snapshot checksum mismatch")
        else:
            with snapshot_path.open("xb") as output:
                output.write(snapshot_bytes)
        stored = snapshot_path.read_bytes()
        if hashlib.sha256(stored).hexdigest() != snapshot_digest:
            raise RuntimeError("Database snapshot checksum mismatch")
        verified = json.loads(stored)
        for table_name, _ in snapshot_tables:
            table = verified["tables"][table_name]
            if len(table["rows"]) != table["row_count"]:
                raise RuntimeError(f"Snapshot row-count mismatch: {table_name}")
        print(f"DATABASE SNAPSHOT VERIFIED: {snapshot_path}", flush=True)
        print(f"SHA256: {snapshot_digest}", flush=True)
        print("READ-ONLY DATABASE SNAPSHOT PASS: no production changes", flush=True)
        print("This is an export, not an implemented database rollback.", flush=True)
        return

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
        compare_fumble_inputs(existing, path)
        if args.verify_backup or args.verify_recovery:
            original_digest = hashlib.sha256(existing).hexdigest()
            backup_path = f"play_by_play_backups/{season}/{original_digest}.parquet"
            backup_url = f"{url}/storage/v1/object/{BUCKET}/{backup_path}"
            backup_headers = {
                "apikey": key, "Authorization": f"Bearer {key}",
                "Content-Type": "application/octet-stream", "x-upsert": "false",
            }
            backup_response = requests.post(
                backup_url, headers=backup_headers, data=existing, timeout=600,
            )
            if not backup_response.ok:
                # Supabase Storage can report an existing object as HTTP 400
                # (Duplicate) rather than 409. Only accept that exact case
                # after verifying the bytes already stored.
                try:
                    error_payload = backup_response.json()
                except ValueError:
                    error_payload = {}
                error_text = str(error_payload).lower()
                is_duplicate = (
                    backup_response.status_code in (400, 409)
                    and ("duplicate" in error_text or "already exists" in error_text
                         or "resourcealreadyexists" in error_text)
                )
                if not is_duplicate:
                    backup_response.raise_for_status()
            backup_bytes = client.storage.from_(BUCKET).download(backup_path)
            if hashlib.sha256(backup_bytes).hexdigest() != original_digest:
                raise RuntimeError("Production backup checksum mismatch")
            print(f"BACKUP VERIFIED: {BUCKET}/{backup_path}", flush=True)
            print(f"SHA256: {original_digest}", flush=True)
            if args.verify_recovery:
                # Exercise the recovery upload using an isolated object, not the
                # production path. Verify byte-for-byte integrity after upload.
                recovery_path = f"play_by_play_recovery_tests/{season}/{original_digest}.parquet"
                recovery_response = requests.post(
                    f"{url}/storage/v1/object/{BUCKET}/{recovery_path}",
                    headers=backup_headers, data=backup_bytes, timeout=600,
                )
                if not recovery_response.ok:
                    try:
                        recovery_error = str(recovery_response.json()).lower()
                    except ValueError:
                        recovery_error = ""
                    duplicate = (
                        recovery_response.status_code in (400, 409)
                        and ("duplicate" in recovery_error or "already exists" in recovery_error
                             or "resourcealreadyexists" in recovery_error)
                    )
                    if not duplicate:
                        recovery_response.raise_for_status()
                restored = client.storage.from_(BUCKET).download(recovery_path)
                if hashlib.sha256(restored).hexdigest() != original_digest:
                    raise RuntimeError("Isolated recovery checksum mismatch")
                print(f"RECOVERY DRILL PASS: {BUCKET}/{recovery_path}", flush=True)
                print("Recovery source verified and isolated restore bytes match production backup", flush=True)
            print("No production Parquet, fumble events, or D/ST scores changed", flush=True)
            return
        data = path.read_bytes()
        if len(data) < 1000:
            raise RuntimeError("Unexpectedly small PBP file")

        if not args.execute and not args.stage and not args.audit_staging:
            print(f"DRY RUN PASS: compatible {object_path}; no storage, event, or score changes", flush=True)
            return
        if args.stage or args.audit_staging:
            digest = hashlib.sha256(data).hexdigest()
            staging_path = f"play_by_play_staging/{season}/{digest}.parquet"
            # Content-addressed and never overwrites the production Parquet.
            endpoint = f"{url}/storage/v1/object/{BUCKET}/{staging_path}"
            headers = {"apikey": key, "Authorization": f"Bearer {key}",
                       "Content-Type": "application/octet-stream", "x-upsert": "false"}
            response = requests.post(endpoint, headers=headers, data=data, timeout=600)
            if not response.ok:
                try:
                    error_text = str(response.json()).lower()
                except ValueError:
                    error_text = ""
                duplicate = (
                    response.status_code in (400, 409)
                    and ("duplicate" in error_text or "already exists" in error_text
                         or "resourcealreadyexists" in error_text)
                )
                if not duplicate:
                    response.raise_for_status()
                print("Staging object already exists; verifying bytes", flush=True)
            uploaded = client.storage.from_(BUCKET).download(staging_path)
            if hashlib.sha256(uploaded).hexdigest() != digest:
                raise RuntimeError("Staging Parquet checksum mismatch")
            print(f"STAGING PASS: nflverse-data/{staging_path}", flush=True)
            print(f"SHA256: {digest}", flush=True)
            print("No production Parquet, fumble events, or D/ST scores changed", flush=True)
            if args.audit_staging:
                audit_url = f"{url}/functions/v1/import-pbp-fumbles"
                audit_headers = {
                    "apikey": key, "Authorization": f"Bearer {key}",
                    "x-inspection-secret": secret, "Content-Type": "application/json",
                }
                audit = requests.post(
                    audit_url, headers=audit_headers,
                    json={"season": season, "mode": "audit", "staging_path": staging_path},
                    timeout=600,
                )
                audit.raise_for_status()
                report = audit.json()
                print(json.dumps(report, indent=2), flush=True)
                if (report.get("success") is not True
                        or report.get("mode") != "audit"
                        or report.get("source") != f"{BUCKET}/{staging_path}"
                        or report.get("parquet_rows") != frame.height
                        or report.get("issues_count") != 0
                        or report.get("unclassified_recoveries") != 0):
                    raise RuntimeError("Staging classifier audit did not pass")
                print("STAGING CLASSIFIER AUDIT PASS: no production changes", flush=True)
            else:
                print("Classifier staging audit requires deployed Edge Function support for an allowlisted staging path", flush=True)
            return

        # Production promotion is explicitly gated by a verified staging audit.
        # Preserve the original production bytes in immutable content-addressed
        # backup storage before changing the production object.
        digest = hashlib.sha256(data).hexdigest()
        staging_path = f"play_by_play_staging/{season}/{digest}.parquet"
        backup_digest = hashlib.sha256(existing).hexdigest()
        backup_path = f"play_by_play_backups/{season}/{backup_digest}.parquet"
        storage_headers = {"apikey": key, "Authorization": f"Bearer {key}",
                           "Content-Type": "application/octet-stream"}
        def put_immutable(object_name, payload, expected_digest):
            response = requests.post(
                f"{url}/storage/v1/object/{BUCKET}/{object_name}",
                headers={**storage_headers, "x-upsert": "false"},
                data=payload, timeout=600,
            )
            if response.status_code != 409:
                response.raise_for_status()
            stored = client.storage.from_(BUCKET).download(object_name)
            if hashlib.sha256(stored).hexdigest() != expected_digest:
                raise RuntimeError(f"Immutable storage checksum mismatch: {object_name}")

        put_immutable(staging_path, data, digest)
        endpoint = f"{url}/functions/v1/import-pbp-fumbles"
        audit_headers = {**storage_headers, "x-inspection-secret": secret}
        def audit_file(object_name):
            response = requests.post(
                endpoint, headers=audit_headers,
                json={"season": season, "mode": "audit", "staging_path": object_name},
                timeout=600,
            )
            response.raise_for_status()
            result = response.json()
            if (result.get("success") is not True
                    or result.get("mode") != "audit"
                    or result.get("source") != f"{BUCKET}/{object_name}"
                    or result.get("parquet_rows") != frame.height
                    or result.get("issues_count") != 0
                    or result.get("unclassified_recoveries") != 0):
                raise RuntimeError(f"Staging audit failed: {result}")
            return result

        staged_report = audit_file(staging_path)
        print(f"STAGING AUDIT PASS: {staged_report['events_prepared']} events", flush=True)
        put_immutable(backup_path, existing, backup_digest)
        print(f"PRODUCTION BACKUP VERIFIED: {BUCKET}/{backup_path}", flush=True)
        # Re-read immediately before replacement to detect concurrent changes.
        current = client.storage.from_(BUCKET).download(object_path)
        if hashlib.sha256(current).hexdigest() != backup_digest:
            raise RuntimeError("Production Parquet changed during refresh; refusing promotion")
        response = requests.post(
            f"{url}/storage/v1/object/{BUCKET}/{object_path}",
            headers={**storage_headers, "x-upsert": "true"},
            data=data, timeout=600,
        )
        response.raise_for_status()
        promoted = client.storage.from_(BUCKET).download(object_path)
        if hashlib.sha256(promoted).hexdigest() != digest:
            raise RuntimeError(
                f"Production promotion checksum mismatch. Verified backup: {backup_path}"
            )
        print(f"PROMOTION PASS: {object_path}; rollback source: {backup_path}", flush=True)

    # Production classification must match the exact staged event distribution.
    response = requests.post(
        endpoint, headers=audit_headers,
        json={"season": season, "mode": "audit"}, timeout=600,
    )
    response.raise_for_status()
    production_report = response.json()
    compare_keys = (
        "classifier_version", "parquet_rows", "fumble_plays",
        "credited_recoveries", "safety_plays", "invalidated_plays",
        "events_prepared", "recovered_events", "categories_and_outcomes",
        "unknown_events", "unclassified_recoveries", "issues_count",
    )
    if (production_report.get("success") is not True
            or any(production_report.get(k) != staged_report.get(k) for k in compare_keys)):
        raise RuntimeError(
            "Production audit does not match staging audit; fumble import not attempted. "
            f"Verified backup: {backup_path}"
        )
    response = requests.post(
        endpoint, headers=audit_headers,
        json={"season": season, "mode": "import", "confirm": "IMPORT_VERIFIED_EVENTS"},
        timeout=600,
    )
    response.raise_for_status()
    import_result = response.json()
    if (import_result.get("success") is not True
            or import_result.get("imported") is None):
        raise RuntimeError(f"Fumble import failed: {import_result}")
    print(f"Fumble import succeeded: {import_result.get('imported')}", flush=True)

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
