"""Verify the coordinator never opens a journal before artifact preflight."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.ingestion.nflverse.dst_refresh_coordinator import begin_verified_refresh


class VerifiedJournalTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.client.rpc.return_value.execute.return_value = SimpleNamespace(
            data="00000000-0000-0000-0000-000000000001"
        )
        self.kwargs = dict(season=2026, staged_digest="a" * 64,
                           backup_parquet_digest="b" * 64,
                           backup_database_digest="c" * 64)
        self.paths = dict(
            staging=f"play_by_play_staging/2026/{'a'*64}.parquet",
            backup_parquet=f"play_by_play_backups/2026/{'b'*64}.parquet",
            production="play_by_play/2026.parquet",
            backup_database=f"dst_database_backups/2026/{'c'*64}.json",
        )

    def test_verified_artifacts_start_v2_journal(self):
        with patch("src.ingestion.nflverse.verify_dst_refresh_artifacts.verify_artifacts",
                   return_value={"paths": self.paths}):
            journal = begin_verified_refresh(self.client, **self.kwargs)
        self.assertEqual(journal.status, "prepared")
        name, params = self.client.rpc.call_args.args
        self.assertEqual(name, "start_dst_refresh_operation_v2")
        self.assertEqual(params["p_backup_database_path"], self.paths["backup_database"])

    def test_preflight_failure_does_not_start_journal(self):
        with patch("src.ingestion.nflverse.verify_dst_refresh_artifacts.verify_artifacts",
                   side_effect=RuntimeError("corrupt backup")):
            with self.assertRaisesRegex(RuntimeError, "corrupt backup"):
                begin_verified_refresh(self.client, **self.kwargs)
        self.client.rpc.assert_not_called()


if __name__ == "__main__":
    unittest.main()
