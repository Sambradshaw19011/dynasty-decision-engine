"""Offline recovery preflight tests; never restore production."""
import hashlib
import json
import unittest
from unittest.mock import Mock, patch

from src.ingestion.nflverse.recover_dst_operation import recover_operation


class RecoveryPreflightTests(unittest.TestCase):
    def setUp(self):
        self.season = 2026
        self.backup = b"original parquet"
        self.staged = b"new parquet"
        self.backup_sha = hashlib.sha256(self.backup).hexdigest()
        self.staged_sha = hashlib.sha256(self.staged).hexdigest()
        self.db_sha = "a" * 64
        self.op = {
            "operation_id": "test-id", "status": "importing", "season_id": 2026,
            "backup_parquet_path": f"play_by_play_backups/2026/{self.backup_sha}.parquet",
            "backup_parquet_sha256": self.backup_sha,
            "staged_parquet_path": f"play_by_play_staging/2026/{self.staged_sha}.parquet",
            "staged_parquet_sha256": self.staged_sha,
            "backup_database_path": f"dst_database_backups/2026/{self.db_sha}.json",
            "backup_database_sha256": self.db_sha,
        }
        self.client = Mock()
        self.client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [self.op]
        self.client.storage.from_.return_value.download.side_effect = self.download
        self.snapshot = {"season": 2026, "tables": {
            "fact_team_fumble_events": {"row_count": 371},
            "fact_calculated_team_weekly_scores": {"row_count": 242},
        }}

    def download(self, path):
        return {
            self.op["backup_parquet_path"]: self.backup,
            self.op["staged_parquet_path"]: self.staged,
            "play_by_play/2026.parquet": self.staged,
        }[path]

    def run_preflight(self):
        with patch("src.ingestion.nflverse.recover_dst_operation.verify_remote_snapshot",
                   return_value=self.snapshot), patch(
                   "src.ingestion.nflverse.recover_dst_operation.count_rows",
                   side_effect=[371, 242]):
            recover_operation(self.client, operation_id="test-id")

    def test_interrupted_import_preflight(self):
        self.run_preflight()
        self.client.rpc.assert_not_called()

    def test_recovering_status_can_retry_preflight(self):
        self.op["status"] = "recovering"
        self.run_preflight()
        self.client.rpc.assert_not_called()

    def test_completed_operation_rejected(self):
        self.op["status"] = "completed"
        with self.assertRaisesRegex(RuntimeError, "cannot be recovered"):
            self.run_preflight()

    def test_missing_durable_backup_rejected(self):
        self.op["backup_database_path"] = None
        with self.assertRaisesRegex(RuntimeError, "no durable database backup"):
            self.run_preflight()

    def test_unexpected_production_parquet_rejected(self):
        original = self.download
        self.client.storage.from_.return_value.download.side_effect = (
            lambda path: b"other" if path == "play_by_play/2026.parquet"
            else original(path)
        )
        with self.assertRaisesRegex(RuntimeError, "differs from both"):
            self.run_preflight()

    def test_corrupt_staged_parquet_rejected(self):
        original = self.download
        self.client.storage.from_.return_value.download.side_effect = (
            lambda path: b"bad" if path == self.op["staged_parquet_path"]
            else original(path)
        )
        with self.assertRaisesRegex(RuntimeError, "Staged Parquet"):
            self.run_preflight()

    def test_execution_without_credentials_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "requires Supabase URL"):
            recover_operation(self.client, operation_id="test-id", execute=True)


if __name__ == "__main__":
    unittest.main()
