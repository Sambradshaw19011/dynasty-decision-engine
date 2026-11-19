"""Offline tests for the concrete D/ST Parquet promotion stage."""
import hashlib
import unittest
from unittest.mock import Mock, patch

from src.ingestion.nflverse.dst_production_stages import promote_verified_parquet


class PromotionTests(unittest.TestCase):
    def setUp(self):
        self.backup = b"original"
        self.staged = b"new version"
        self.backup_sha = hashlib.sha256(self.backup).hexdigest()
        self.staged_sha = hashlib.sha256(self.staged).hexdigest()
        self.client = Mock()
        self.storage = self.client.storage.from_.return_value
        self.current = self.backup
        self.storage.download.side_effect = self.download
        self.kwargs = dict(season=2026, staged_digest=self.staged_sha,
                           backup_digest=self.backup_sha,
                           url="https://example.supabase.co", key="test")

    def download(self, path):
        if path == "play_by_play/2026.parquet":
            return self.current
        if path == f"play_by_play_staging/2026/{self.staged_sha}.parquet":
            return self.staged
        if path == f"play_by_play_backups/2026/{self.backup_sha}.parquet":
            return self.backup
        raise AssertionError(path)

    def test_verified_promotion(self):
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post") as post:
            post.return_value.raise_for_status.return_value = None
            def upload(*args, **kwargs):
                self.current = kwargs["data"]
                return post.return_value
            post.side_effect = upload
            promote_verified_parquet(self.client, **self.kwargs)
            self.assertEqual(self.current, self.staged)
            self.assertEqual(post.call_count, 1)

    def test_concurrent_production_change_blocks_upload(self):
        self.current = b"unrelated writer"
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post") as post:
            with self.assertRaisesRegex(RuntimeError, "changed before promotion"):
                promote_verified_parquet(self.client, **self.kwargs)
            post.assert_not_called()

    def test_corrupted_staging_blocks_upload(self):
        self.staged = b"corrupted"
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post") as post:
            with self.assertRaisesRegex(RuntimeError, "Staged Parquet checksum"):
                promote_verified_parquet(self.client, **self.kwargs)
            post.assert_not_called()

    def test_corrupted_backup_blocks_upload(self):
        self.backup = b"corrupted"
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post") as post:
            with self.assertRaisesRegex(RuntimeError, "Backup Parquet checksum"):
                promote_verified_parquet(self.client, **self.kwargs)
            post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
