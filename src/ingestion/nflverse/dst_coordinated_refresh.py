"""Wire concrete D/ST stages to the verified journal coordinator.

This module is deliberately not a standalone production entry point.
refresh_dst.py retains its disabled --execute gate until live safety review.
"""
from __future__ import annotations

from src.ingestion.nflverse.dst_refresh_coordinator import run_verified_refresh
from src.ingestion.nflverse.dst_production_stages import (
    promote_verified_parquet, import_verified_fumbles,
    score_completed_dst_weeks, verify_completed_dst_scores,
)
from src.ingestion.nflverse.recover_dst_operation import recover_operation


def run_coordinated_dst_refresh(client, *, season: int, staged_digest: str,
                                backup_parquet_digest: str,
                                backup_database_digest: str,
                                expected_pbp_rows: int, settings_id: str,
                                completed_games: list[dict], url: str,
                                key: str, inspection_secret: str) -> str:
    """Execute four verified stages, with full recovery on handled failure.

    Call only when no other writer is running and all durable backups have
    been prepared. The journal RPC serializes participating refresh workers;
    unrelated writers are not covered by that lease.
    """
    if not completed_games or expected_pbp_rows <= 0:
        raise RuntimeError("Refusing refresh without completed games and PBP rows")
    if not all((url, key, inspection_secret)):
        raise RuntimeError("Missing refresh credentials")

    return run_verified_refresh(
        client, season=season, staged_digest=staged_digest,
        backup_parquet_digest=backup_parquet_digest,
        backup_database_digest=backup_database_digest,
        promote=lambda: promote_verified_parquet(
            client, season=season, staged_digest=staged_digest,
            backup_digest=backup_parquet_digest, url=url, key=key),
        import_events=lambda operation_id: import_verified_fumbles(
            season=season, staged_digest=staged_digest,
            expected_rows=expected_pbp_rows, operation_id=operation_id, url=url, key=key,
            inspection_secret=inspection_secret),
        score=lambda: score_completed_dst_weeks(
            client, season=season, settings_id=settings_id,
            completed_games=completed_games),
        verify=lambda: verify_completed_dst_scores(
            client, season=season, settings_id=settings_id,
            completed_games=completed_games),
        recover=lambda operation_id: recover_operation(
            client, operation_id=operation_id, execute=True, url=url, key=key),
    )
