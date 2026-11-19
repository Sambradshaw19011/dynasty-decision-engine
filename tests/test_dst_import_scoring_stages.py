"""Offline tests for fumble import and D/ST scoring production stages."""
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from src.ingestion.nflverse.dst_production_stages import (
    import_verified_fumbles, score_completed_dst_weeks, verify_completed_dst_scores,
)


class ImportAndScoringTests(unittest.TestCase):
    def setUp(self):
        self.season = 2026
        self.staged_digest = "a" * 64
        self.kwargs = dict(season=2026, staged_digest=self.staged_digest,
                           expected_rows=26246, url="https://example.supabase.co",
                           key="test", inspection_secret="secret")
        self.audit = dict(success=True, mode="audit", parquet_rows=26246,
                          issues_count=0, unclassified_recoveries=0,
                          unknown_events=0, events_prepared=371,
                          classifier_version="pbp-fumble-deterministic-5.2.0")
        self.staging_path = f"nflverse-data/play_by_play_staging/2026/{self.staged_digest}.parquet"
        self.production_path = "nflverse-data/play_by_play/2026.parquet"

    def responses(self, *, production_changes=None):
        staged = {**self.audit, "source": self.staging_path}
        production = {**self.audit, "source": self.production_path,
                      **(production_changes or {})}
        reports = [staged, production, {"success": True, "imported": 371}]
        calls = []
        def post(*args, **kwargs):
            calls.append(kwargs["json"])
            result = reports.pop(0)
            return SimpleNamespace(raise_for_status=lambda: None,
                                   json=lambda: result)
        return post, calls

    def test_matching_audits_allow_import(self):
        post, calls = self.responses()
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post",
                   side_effect=post):
            result = import_verified_fumbles(**self.kwargs)
        self.assertEqual(result["imported"], 371)
        self.assertEqual([x["mode"] for x in calls], ["audit", "audit", "import"])

    def test_import_count_mismatch_blocks_completion(self):
        post, calls = self.responses()
        def wrong_count(*args, **kwargs):
            response = post(*args, **kwargs)
            if kwargs["json"]["mode"] == "import":
                return SimpleNamespace(raise_for_status=lambda: None,
                                       json=lambda: {"success": True, "imported": 370})
            return response
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post",
                   side_effect=wrong_count):
            with self.assertRaisesRegex(RuntimeError, "Fumble import count mismatch"):
                import_verified_fumbles(**self.kwargs)
        self.assertEqual(len(calls), 3)

    def test_audit_difference_prevents_import(self):
        post, calls = self.responses(production_changes={"events_prepared": 370})
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post",
                   side_effect=post):
            with self.assertRaisesRegex(RuntimeError, "differs from staging"):
                import_verified_fumbles(**self.kwargs)
        self.assertEqual(len(calls), 2)

    def test_classifier_issues_prevent_import(self):
        post, calls = self.responses(production_changes={"issues_count": 1})
        with patch("src.ingestion.nflverse.dst_production_stages.requests.post",
                   side_effect=post):
            with self.assertRaisesRegex(RuntimeError, "audit rejected"):
                import_verified_fumbles(**self.kwargs)
        self.assertEqual(len(calls), 2)

    def test_scoring_runs_all_completed_weeks(self):
        client = Mock()
        client.rpc.return_value.execute.return_value.data = {"success": True}
        games = [
            {"game_id": "g1", "week": 1, "season_type": "REG"},
            {"game_id": "g2", "week": 1, "season_type": "REG"},
            {"game_id": "g3", "week": 2, "season_type": "REG"},
            {"game_id": "g4", "week": 1, "season_type": "POST"},
        ]
        results = score_completed_dst_weeks(client, season=2026,
                                            settings_id=1, completed_games=games)
        self.assertEqual(len(results), 3)
        self.assertEqual(client.rpc.call_count, 3)

    def test_failed_week_blocks_scoring(self):
        client = Mock()
        client.rpc.return_value.execute.return_value.data = None
        with self.assertRaisesRegex(RuntimeError, "Scoring failed"):
            score_completed_dst_weeks(
                client, season=2026, settings_id=1,
                completed_games=[{"game_id": "g1", "week": 1, "season_type": "REG"}])

    def test_unexpected_score_game_fails_closed(self):
        client = Mock()
        games = [{"game_id": "g1", "week": 1, "season_type": "REG"}]
        rows = [{"game_id": "g1", "team_code": "SF"},
                {"game_id": "g1", "team_code": "LAR"},
                {"game_id": "other", "team_code": "NYG"}]
        with patch("src.ingestion.nflverse.refresh_dst.all_rows", return_value=rows):
            with self.assertRaisesRegex(RuntimeError, "Unexpected D/ST score game"):
                verify_completed_dst_scores(client, season=2026,
                                            settings_id=1, completed_games=games)

    def test_missing_team_code_fails_closed(self):
        client = Mock()
        games = [{"game_id": "g1", "week": 1, "season_type": "REG"}]
        rows = [{"game_id": "g1", "team_code": "SF"},
                {"game_id": "g1", "team_code": None}]
        with patch("src.ingestion.nflverse.refresh_dst.all_rows", return_value=rows):
            with self.assertRaisesRegex(RuntimeError, "Missing D/ST team code"):
                verify_completed_dst_scores(client, season=2026,
                                            settings_id=1, completed_games=games)

    def test_duplicate_completed_games_fail_closed(self):
        client = Mock()
        games = [{"game_id": "g1", "week": 1, "season_type": "REG"}] * 2
        with patch("src.ingestion.nflverse.refresh_dst.all_rows", return_value=[]):
            with self.assertRaisesRegex(RuntimeError, "Duplicate completed game IDs"):
                verify_completed_dst_scores(client, season=2026,
                                            settings_id=1, completed_games=games)

    def test_score_coverage_requires_two_distinct_teams(self):
        client = Mock()
        games = [{"game_id": "g1", "week": 1, "season_type": "REG"}]
        with patch("src.ingestion.nflverse.refresh_dst.all_rows",
                   return_value=[{"game_id": "g1", "team_code": "SF"},
                                 {"game_id": "g1", "team_code": "LAR"}]):
            self.assertEqual(verify_completed_dst_scores(
                client, season=2026, settings_id=1, completed_games=games), 1)
        with patch("src.ingestion.nflverse.refresh_dst.all_rows",
                   return_value=[{"game_id": "g1", "team_code": "SF"},
                                 {"game_id": "g1", "team_code": "SF"}]):
            with self.assertRaisesRegex(RuntimeError, "coverage failed"):
                verify_completed_dst_scores(
                    client, season=2026, settings_id=1, completed_games=games)


if __name__ == "__main__":
    unittest.main()
