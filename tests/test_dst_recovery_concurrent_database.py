"""Refuse recovery when a concurrent writer changes database rows during promotion."""
import hashlib
import unittest
from unittest.mock import Mock, patch

from src.ingestion.nflverse.recover_dst_operation import recover_operation


class ConcurrentDatabaseRecoveryTests(unittest.TestCase):
    def setUp(self):
        backup, staged = b"old", b"new"
        old_sha = hashlib.sha256(backup).hexdigest()
        new_sha = hashlib.sha256(staged).hexdigest()
        self.op = {
            "operation_id": "test-op", "status": "promoting", "season_id": 2026,
            "backup_parquet_path": f"play_by_play_backups/2026/{old_sha}.parquet",
            "backup_parquet_sha256": old_sha,
            "staged_parquet_path": f"play_by_play_staging/2026/{new_sha}.parquet",
            "staged_parquet_sha256": new_sha,
            "backup_database_path": f"dst_database_backups/2026/{'a'*64}.json",
            "backup_database_sha256": "a"*64,
        }
        self.snapshot = {"season": 2026, "tables": {
            "fact_team_fumble_events": {"row_count": 1, "rows": [
                {"season_id": 2026, "id": 1, "value": "original"}]},
            "fact_calculated_team_weekly_scores": {"row_count": 1, "rows": [
                {"season_id": 2026, "id": 1, "points": 10}]},
        }}
        self.client = Mock()
        self.client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [self.op]
        self.client.storage.from_.return_value.download.side_effect = lambda path: (
            backup if path == self.op["backup_parquet_path"] else
            staged if path == self.op["staged_parquet_path"] else backup
        )

    def test_same_row_count_changed_contents_refuses_restore(self):
        changed = [{"season_id": 2026, "id": 1, "value": "concurrent edit"}]
        with patch("src.ingestion.nflverse.recover_dst_operation.verify_remote_snapshot",
                   return_value=self.snapshot), patch(
                   "src.ingestion.nflverse.recover_dst_operation.count_rows",
                   side_effect=[1, 1]), patch(
                   "src.ingestion.nflverse.refresh_dst.all_rows", return_value=changed):
            with self.assertRaisesRegex(RuntimeError, "Concurrent database change"):
                recover_operation(self.client, operation_id="test-op", execute=True,
                                  url="https://example.supabase.co", key="key")
        self.client.rpc.assert_not_called()

    def test_unchanged_database_reaches_restore(self):
        original = [
            self.snapshot["tables"]["fact_team_fumble_events"]["rows"],
            self.snapshot["tables"]["fact_calculated_team_weekly_scores"]["rows"],
        ]
        with patch("src.ingestion.nflverse.recover_dst_operation.verify_remote_snapshot",
                   return_value=self.snapshot), patch(
                   "src.ingestion.nflverse.recover_dst_operation.count_rows",
                   side_effect=[1, 1]), patch(
                   "src.ingestion.nflverse.refresh_dst.all_rows", side_effect=original):
            with self.assertRaisesRegex(RuntimeError, "restore unconfirmed"):
                self.client.rpc.return_value.execute.return_value.data = {}
                recover_operation(self.client, operation_id="test-op", execute=True,
                                  url="https://example.supabase.co", key="key")
        self.client.rpc.assert_called()


if __name__ == "__main__":
    unittest.main()
