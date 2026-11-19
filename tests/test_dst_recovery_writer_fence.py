"""Offline regression tests for operation-owned D/ST database recovery."""
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.ingestion.nflverse.recover_dst_operation import recover_operation


def test_recovery_passes_operation_id_to_fenced_restore_rpc():
    season = 2026
    operation_id = "00000000-0000-0000-0000-000000000001"
    op = {
        "operation_id": operation_id, "status": "recovering",
        "season_id": season,
        "backup_parquet_sha256": "a" * 64,
        "staged_parquet_sha256": "b" * 64,
        "backup_parquet_path": f"play_by_play_backups/{season}/{'a' * 64}.parquet",
        "staged_parquet_path": f"play_by_play_staging/{season}/{'b' * 64}.parquet",
        "backup_database_path": f"dst_database_backups/{season}/{'c' * 64}.json",
        "backup_database_sha256": "c" * 64,
    }
    client = Mock()
    client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [op]
    client.storage.from_.return_value.download.side_effect = [
        b"backup", b"staged", b"production", b"production", b"restored",
    ]
    client.rpc.return_value.execute.return_value = SimpleNamespace(data={
        "success": True, "changes_committed": True,
    })
    snapshot = {"tables": {
        "fact_team_fumble_events": {"row_count": 1},
        "fact_calculated_team_weekly_scores": {"row_count": 1},
    }}
    with patch(
        "src.ingestion.nflverse.recover_dst_operation.verify_remote_snapshot",
        return_value=snapshot,
    ), patch(
        "src.ingestion.nflverse.recover_dst_operation.sha256",
        side_effect=["a" * 64, "b" * 64, "a" * 64, "a" * 64, "a" * 64],
    ), patch(
        "src.ingestion.nflverse.recover_dst_operation.count_rows",
        return_value=1,
    ), patch(
        "src.ingestion.nflverse.recover_dst_operation.advance",
    ) as advance:
        recover_operation(
            client, operation_id=operation_id, execute=True,
            url="https://example.supabase.co", key="test-key",
        )
    args = client.rpc.call_args
    assert args.args[0] == "restore_dst_database_snapshot"
    assert args.args[1]["p_operation_id"] == operation_id
    advance.assert_called_once()
    assert advance.call_args.args[2:4] == ("recovering", "recovered")
    assert advance.call_args.args[4] == {"recovery": "verified_database_and_parquet"}
