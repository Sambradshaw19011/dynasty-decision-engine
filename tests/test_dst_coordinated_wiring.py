"""Offline tests that all concrete D/ST stage callbacks are connected."""
import unittest
from unittest.mock import Mock, patch

from src.ingestion.nflverse.dst_coordinated_refresh import run_coordinated_dst_refresh


class CoordinatedWiringTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.kwargs = dict(
            season=2026, staged_digest="a" * 64,
            backup_parquet_digest="b" * 64,
            backup_database_digest="c" * 64,
            expected_pbp_rows=26246, settings_id=1,
            completed_games=[{"game_id": "g", "season_type": "REG", "week": 1}],
            url="https://example.supabase.co", key="key",
            inspection_secret="secret",
        )

    def test_concrete_stages_and_recovery_are_wired(self):
        calls = []
        def runner(client, **kwargs):
            self.assertIs(client, self.client)
            self.assertEqual(kwargs["season"], 2026)
            kwargs["promote"]()
            kwargs["import_events"]()
            kwargs["score"]()
            kwargs["verify"]()
            kwargs["recover"]("test-operation")
            return "test-operation"
        with patch("src.ingestion.nflverse.dst_coordinated_refresh.run_verified_refresh",
                   side_effect=runner), patch(
                   "src.ingestion.nflverse.dst_coordinated_refresh.promote_verified_parquet",
                   side_effect=lambda *a, **k: calls.append("promote")), patch(
                   "src.ingestion.nflverse.dst_coordinated_refresh.import_verified_fumbles",
                   side_effect=lambda *a, **k: calls.append("import")), patch(
                   "src.ingestion.nflverse.dst_coordinated_refresh.score_completed_dst_weeks",
                   side_effect=lambda *a, **k: calls.append("score")), patch(
                   "src.ingestion.nflverse.dst_coordinated_refresh.verify_completed_dst_scores",
                   side_effect=lambda *a, **k: calls.append("verify")), patch(
                   "src.ingestion.nflverse.dst_coordinated_refresh.recover_operation",
                   side_effect=lambda *a, **k: calls.append("recover")):
            result = run_coordinated_dst_refresh(self.client, **self.kwargs)
        self.assertEqual(result, "test-operation")
        self.assertEqual(calls, ["promote", "import", "score", "verify", "recover"])

    def test_missing_credentials_never_start_journal(self):
        self.kwargs["inspection_secret"] = ""
        with patch("src.ingestion.nflverse.dst_coordinated_refresh.run_verified_refresh") as run:
            with self.assertRaisesRegex(RuntimeError, "Missing refresh credentials"):
                run_coordinated_dst_refresh(self.client, **self.kwargs)
            run.assert_not_called()

    def test_missing_games_never_start_journal(self):
        self.kwargs["completed_games"] = []
        with patch("src.ingestion.nflverse.dst_coordinated_refresh.run_verified_refresh") as run:
            with self.assertRaisesRegex(RuntimeError, "Refusing refresh"):
                run_coordinated_dst_refresh(self.client, **self.kwargs)
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
