"""Offline verification tests for the pre-journal D/ST artifact gate."""
import hashlib
import json
import unittest
from unittest.mock import Mock, patch

from src.ingestion.nflverse.verify_dst_refresh_artifacts import verify_artifacts
def make_document():
    return {"season": 2026, "tables": {
        "fact_team_fumble_events": {"season_column": "season_id", "row_count": 1,
            "rows": [{"season_id": 2026, "fumble_event_id": 1, "game_id": "g",
                      "play_id": 1, "recovery_sequence": 1}]},
        "fact_calculated_team_weekly_scores": {"season_column": "season_id",
            "row_count": 1, "rows": [{"season_id": 2026,
            "calculated_team_score_id": 1, "scoring_settings_id": 1,
            "game_id": "g", "team_code": "SF"}]},
    }}


class ArtifactGateTests(unittest.TestCase):
    def setUp(self):
        self.season = 2026
        self.staged = b"staged parquet"
        self.backup = b"original production parquet"
        self.snapshot = json.dumps(make_document()).encode()
        self.stage_sha = hashlib.sha256(self.staged).hexdigest()
        self.backup_sha = hashlib.sha256(self.backup).hexdigest()
        self.db_sha = hashlib.sha256(self.snapshot).hexdigest()
        self.client = Mock()
        self.client.storage.from_.return_value.download.side_effect = self.download

    def download(self, path):
        return {
            f"play_by_play_staging/2026/{self.stage_sha}.parquet": self.staged,
            f"play_by_play_backups/2026/{self.backup_sha}.parquet": self.backup,
            "play_by_play/2026.parquet": self.backup,
            f"dst_database_backups/2026/{self.db_sha}.json": self.snapshot,
        }[path]

    def check(self):
        return verify_artifacts(
            self.client, season=self.season, staged_digest=self.stage_sha,
            backup_parquet_digest=self.backup_sha,
            backup_database_digest=self.db_sha,
        )

    def test_valid_artifacts(self):
        result = self.check()
        self.assertEqual(result["snapshot_counts"]["fact_team_fumble_events"], 1)
        self.client.rpc.assert_not_called()

    def test_modified_production_rejected(self):
        original = self.download
        self.client.storage.from_.return_value.download.side_effect = (
            lambda path: b"unexpected" if path == "play_by_play/2026.parquet"
            else original(path)
        )
        with self.assertRaisesRegex(RuntimeError, "production"):
            self.check()

    def test_corrupted_staging_rejected(self):
        original = self.download
        self.client.storage.from_.return_value.download.side_effect = (
            lambda path: b"corrupt" if path.startswith("play_by_play_staging/")
            else original(path)
        )
        with self.assertRaisesRegex(RuntimeError, "staging"):
            self.check()

    def test_corrupted_database_snapshot_rejected(self):
        original = self.download
        self.client.storage.from_.return_value.download.side_effect = (
            lambda path: b"corrupt" if path.startswith("dst_database_backups/")
            else original(path)
        )
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            self.check()

    def test_invalid_digest_rejected_before_download(self):
        self.stage_sha = "../bad"
        with self.assertRaisesRegex(RuntimeError, "digests"):
            self.check()
        self.client.storage.from_.return_value.download.assert_not_called()


if __name__ == "__main__":
    unittest.main()
