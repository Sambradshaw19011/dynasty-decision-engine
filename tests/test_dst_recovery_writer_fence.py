"""Fail-closed regression tests for D/ST rollback after database mutations."""
from unittest.mock import Mock, patch

import pytest

from src.ingestion.nflverse.recover_dst_operation import recover_operation


@pytest.mark.parametrize("status", [
    "importing", "scoring", "verifying", "recovery_failed", "recovering",
])
def test_automatic_rollback_refuses_unfenced_database_writers(status):
    season = 2026
    op = {
        "operation_id": "test-operation", "status": status,
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
    storage = client.storage.from_.return_value
    storage.download.side_effect = [b"backup", b"staged", b"production"]
    with patch(
        "src.ingestion.nflverse.recover_dst_operation.verify_remote_snapshot",
        return_value={"tables": {}},
    ), patch(
        "src.ingestion.nflverse.recover_dst_operation.sha256",
        side_effect=["a" * 64, "b" * 64, "a" * 64],
    ):
        with pytest.raises(RuntimeError, match="unrelated writers are not fenced"):
            recover_operation(client, operation_id="test-operation",
                              execute=True, url="https://example.supabase.co",
                              key="test-key")
    client.rpc.assert_not_called()
