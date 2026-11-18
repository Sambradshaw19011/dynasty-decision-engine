"""Copy Supabase PostgreSQL play-by-play rows into verified Supabase Storage Parquet parts.

Reads PostgreSQL only. Does not delete, update, or insert database rows.
Install: pip install psycopg2-binary pandas pyarrow supabase python-dotenv
Run: python src/ingestion/nflverse/export_pbp_to_supabase_storage.py --season 2005
"""
import argparse
import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from supabase import create_client

BUCKET = "nflverse-data"
CHUNK_ROWS = 2000
OUT = Path("pbp_parquet_backup")


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    load_dotenv(dotenv_path=Path.cwd() / ".env", override=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int)
    parser.add_argument("--start", type=int, default=2005)
    parser.add_argument("--end", type=int, default=2020)
    args = parser.parse_args()
    seasons = [args.season] if args.season is not None else range(args.start, args.end + 1)
    db_url = os.environ.get("DATABASE_URL")
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not all((db_url, url, key)):
        raise SystemExit("Missing DATABASE_URL, SUPABASE_URL, or SUPABASE_SERVICE_ROLE_KEY in .env")

    storage = create_client(url, key).storage.from_(BUCKET)
    OUT.mkdir(exist_ok=True)

    for season in seasons:
        print(f"\n=== Exporting {season} ===", flush=True)
        season_dir = OUT / str(season)
        season_dir.mkdir(parents=True, exist_ok=True)
        # A short read-only transaction per batch, no server-side cursor.
        with psycopg2.connect(db_url, connect_timeout=30, sslmode="require") as conn:
            conn.set_session(readonly=True, autocommit=True)
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM public.fact_play_by_play WHERE season = %s", (season,))
                expected = cur.fetchone()[0]
        if not expected:
            print(f"SKIP {season}: no rows", flush=True)
            continue

        exported = 0
        part = 0
        last_game_id = None
        last_play_id = None
        manifest = []
        while True:
            # Reconnect per batch to avoid long-lived sessions and recover from idle disconnects.
            with psycopg2.connect(db_url, connect_timeout=30, sslmode="require") as conn:
                conn.set_session(readonly=True, autocommit=True)
                with conn.cursor() as cur:
                    if last_game_id is None:
                        cur.execute("""
                            SELECT * FROM public.fact_play_by_play
                            WHERE season = %s
                            ORDER BY game_id, play_id LIMIT %s
                        """, (season, CHUNK_ROWS))
                    else:
                        cur.execute("""
                            SELECT * FROM public.fact_play_by_play
                            WHERE season = %s AND (game_id, play_id) > (%s, %s)
                            ORDER BY game_id, play_id LIMIT %s
                        """, (season, last_game_id, last_play_id, CHUNK_ROWS))
                    columns = [desc[0] for desc in cur.description]
                    rows = cur.fetchall()
            if not rows:
                break

            part += 1
            records = [dict(zip(columns, row)) for row in rows]
            for record in records:
                raw = record.get("raw_play_data")
                if raw is not None and not isinstance(raw, str):
                    record["raw_play_data"] = json.dumps(raw, ensure_ascii=False, separators=(",", ":"), default=str)
            path = season_dir / f"part-{part:04d}.parquet"
            pd.DataFrame.from_records(records, columns=columns).to_parquet(
                path, engine="pyarrow", compression="zstd", index=False
            )
            local_bytes = path.read_bytes()
            object_name = f"play_by_play/season={season}/{path.name}"
            try:
                storage.upload(
                    path=object_name,
                    file=local_bytes,
                    file_options={"content-type": "application/octet-stream", "upsert": "false"},
                )
            except Exception as exc:
                # Safe rerun: existing objects must match exactly.
                try:
                    existing = storage.download(object_name)
                except Exception:
                    raise RuntimeError(f"Upload failed for {object_name}: {exc}") from exc
                if digest(existing) != digest(local_bytes):
                    raise RuntimeError(f"Existing object differs: {object_name}") from exc
            remote_bytes = storage.download(object_name)
            if digest(remote_bytes) != digest(local_bytes):
                raise RuntimeError(f"SHA256 mismatch: {object_name}")

            last_game_id, last_play_id = records[-1]["game_id"], records[-1]["play_id"]
            exported += len(rows)
            manifest.append({"object": object_name, "rows": len(rows), "sha256": digest(local_bytes)})
            print(f"  {exported:,}/{expected:,} verified: {object_name}", flush=True)

        if exported != expected:
            raise RuntimeError(f"Season {season}: exported {exported} of {expected} database rows")
        (season_dir / "manifest.json").write_text(
            json.dumps({"season": season, "expected_rows": expected, "parts": manifest}, indent=2),
            encoding="utf-8",
        )
        print(f"PASS {season}: {exported:,} rows exported and uploaded bytes verified", flush=True)
    print("\nFinished. PostgreSQL data was not modified.")


if __name__ == "__main__":
    main()
