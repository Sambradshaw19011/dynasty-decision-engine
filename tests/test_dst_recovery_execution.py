"""Offline tests for D/ST recovery execution and journal state transitions.

All storage, RPC, and database operations are mocked. No Supabase writes occur.
"""
import hashlib
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.ingestion.nflverse.recover_dst_operation import recover_operation


class RecoveryExecutionTests(unittest.TestCase):
    def setUp(self):
        self.backup = b"original parquet"
        self.staged = b"staged parquet"
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
        self.snapshot = {"season": 2026, "tables": {
            "fact_team_fumble_events": {"row_count": 371},
            "fact_calculated_team_weekly_scores": {"row_count": 242},
        }}
        self.client = Mock()
        self.client.table.return_value.select.return_value.eq.return_value.execute.return_value.data = [self.op]
        self.production = self.staged
        self.transitions = []
        self.fail_restore = False
        self.client.storage.from_.return_value.download.side_effect = self.download
        self.client.rpc.side_effect = self.rpc

    def download(self, path):
        if path == "play_by_play/2026.parquet":
            return self.production
        return {
            self.op["backup_parquet_path"]: self.backup,
            self.op["staged_parquet_path"]: self.staged,
        }[path]

    def rpc(self, name, params):
        if name == "advance_dst_refresh_operation":
            self.transitions.append((params["p_expected_status"], params["p_next_status"]))
            data = {"status": params["p_next_status"]}
        elif name == "restore_dst_database_snapshot":
            if self.fail_restore:
                raise RuntimeError("injected database failure")
            data = {"success": True, "changes_committed": True}
        else:
            raise AssertionError(name)
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=data))

    def run_recovery(self):
        with patch("src.ingestion.nflverse.recover_dst_operation.verify_remote_snapshot",
                   return_value=self.snapshot), patch(
                   "src.ingestion.nflverse.recover_dst_operation.count_rows",
                   side_effect=[371, 242, 371, 242]), patch(
                   "src.ingestion.nflverse.recover_dst_operation.requests.post"
                   ) as post:
            def uploaded(*args, **kwargs):
                self.production = kwargs["data"]
                response = Mock()
                response.raise_for_status.return_value = None
                return response
            post.side_effect = uploaded
            recover_operation(self.client, operation_id="test-id", execute=True,
                              url="https://example.supabase.co", key="test-key")
            return post

    def test_successful_restore_and_journal_completion(self):
        post = self.run_recovery()
        self.assertEqual(self.production, self.backup)
        self.assertEqual(self.transitions, [
            ("importing", "recovering"), ("recovering", "recovered")
        ])
        self.assertEqual(post.call_count, 1)

    def test_restore_failure_marks_recovery_failed(self):
        self.fail_restore = True
        with self.assertRaisesRegex(RuntimeError, "injected database failure"):
            self.run_recovery()
        self.assertEqual(self.production, self.staged)
        self.assertEqual(self.transitions, [
            ("importing", "recovering"), ("recovering", "recovery_failed")
        ])

    def test_retry_from_recovering(self):
        self.op["status"] = "recovering"
        self.run_recovery()
        self.assertEqual(self.transitions, [("recovering", "recovered")])

    def test_original_parquet_needs_no_upload(self):
        self.production = self.backup
        post = self.run_recovery()
        self.assertEqual(post.call_count, 0)
        self.assertEqual(self.transitions[-1], ("recovering", "recovered"))

    def test_post_restore_verification_failure_blocks_completion(self):
        with patch("src.ingestion.nflverse.recover_dst_operation.verify_remote_snapshot",
                   return_value=self.snapshot), patch(
                   "src.ingestion.nflverse.recover_dst_operation.count_rows",
                   side_effect=[371, 242, 370]), patch(
                   "src.ingestion.nflverse.recover_dst_operation.requests.post"
                   ) as post:
            post.return_value.raise_for_status.return_value = None
            self.production = self.backup
            with self.assertRaisesRegex(RuntimeError, "row count mismatch"):
                recover_operation(self.client, operation_id="test-id", execute=True,
                                  url="https://example.supabase.co", key="test-key")
        self.assertEqual(self.transitions[-1], ("recovering", "recovery_failed"))
        self.assertNotIn(("recovering", "recovered"), self.transitions)


if __name__ == "__main__":
    unittest.main()
