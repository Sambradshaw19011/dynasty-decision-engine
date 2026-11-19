"""Patch the deployed import-pbp-fumbles source export without changing classification.

Usage:
  python scripts/patch_dst_edge_function.py path/to/index.ts
The input is overwritten only if the exact expected call occurs once.
"""
from pathlib import Path
import sys

OLD = "admin.rpc('replace_fumble_events_season',{p_season:season,p_events:events})"
NEW = "admin.rpc('replace_fumble_events_season',{p_season:season,p_events:events,p_operation_id:body.operation_id??null})"

def patch(source: str) -> str:
    if source.count(NEW) == 1:
        return source
    if source.count(OLD) != 1:
        raise ValueError("Expected exactly one known fumble replacement RPC; refusing unsafe patch")
    return source.replace(OLD, NEW)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/patch_dst_edge_function.py path/to/index.ts")
    path = Path(sys.argv[1])
    old = path.read_text(encoding="utf-8")
    updated = patch(old)
    if updated != old:
        path.write_text(updated, encoding="utf-8")
        print(f"Patched {path}")
    else:
        print("Already patched")
