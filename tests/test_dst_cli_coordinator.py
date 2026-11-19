"""Tests for the fail-closed CLI-to-coordinator integration seam."""
import unittest
from unittest.mock import Mock, patch

from src.ingestion.nflverse.refresh_dst import execute_coordinated_refresh


class CliCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.kwargs = dict(
            season=2026, staged_digest="a" * 64,
            backup_parquet_digest="b" * 64,
            backup_database_digest="c" * 64,
            expected_pbp_rows=26246, settings_id=1,
            completed_games=[{"game_id": "g1", "week": 1, "season_type": "REG"}],
            url="https://example.supabase.co", key="key",
            inspection_secret="secret",
        )

    def test_default_rejects_before_any_stage(self):
        with patch("src.ingestion.nflverse.dst_coordinated_refresh.run_coordinated_dst_refresh") as run:
            with self.assertRaisesRegex(RuntimeError, "disabled pending safety validation"):
                execute_coordinated_refresh(self.client, **self.kwargs)
            run.assert_not_called()

    def test_explicit_authorization_delegates_all_inputs(self):
        with patch("src.ingestion.nflverse.dst_coordinated_refresh.run_coordinated_dst_refresh",
                   return_value="operation-123") as run:
            result = execute_coordinated_refresh(
                self.client, **self.kwargs, production_authorized=True)
            self.assertEqual(result, "operation-123")
            run.assert_called_once_with(self.client, **self.kwargs)


if __name__ == "__main__":
    unittest.main()
