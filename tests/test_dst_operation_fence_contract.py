"""Offline operation-fence integration contract tests (no Supabase writes)."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from scripts.patch_dst_edge_function import patch as patch_edge
from src.ingestion.nflverse.dst_production_stages import score_completed_dst_weeks
from src.ingestion.nflverse.dst_refresh_coordinator import RefreshJournal


OP = "00000000-0000-0000-0000-000000000001"


class FakeClient:
    def __init__(self):
        self.calls = []

    def rpc(self, name, params):
        self.calls.append((name, params))
        if name == "advance_dst_refresh_operation":
            data = {"status": params["p_next_status"]}
        elif name == "calculate_team_weekly_scores":
            data = OP
        else:
            raise AssertionError(name)
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=data))


class FenceTests(unittest.TestCase):
    def test_journal_passes_operation_to_both_writers(self):
        client = FakeClient()
        journal = RefreshJournal(client, OP)
        observed = []
        journal.run(
            promote=lambda: None,
            import_events=lambda op: observed.append(("import", op)),
            score=lambda op: observed.append(("score", op)),
            verify=lambda: None,
            recover=lambda op: self.fail("Unexpected recovery"),
        )
        self.assertEqual(observed, [("import", OP), ("score", OP)])

    def test_scoring_rpc_includes_operation_id(self):
        client = FakeClient()
        score_completed_dst_weeks(
            client, season=2026, settings_id=OP, operation_id=OP,
            completed_games=[{"week": 7, "season_type": "REG"}],
        )
        name, params = client.calls[0]
        self.assertEqual(name, "calculate_team_weekly_scores")
        self.assertEqual(params["p_operation_id"], OP)
        self.assertEqual(params["p_scoring_settings_id"], OP)

    def test_edge_patch_is_idempotent_and_exact(self):
        old = "const {data,error}=await admin.rpc('replace_fumble_events_season',{p_season:season,p_events:events});"
        updated = patch_edge(old)
        self.assertIn("p_operation_id:body.operation_id??null", updated)
        self.assertEqual(patch_edge(updated), updated)
        with self.assertRaises(ValueError):
            patch_edge("different implementation")

    def test_import_payload_contains_operation_id(self):
        from src.ingestion.nflverse.dst_production_stages import import_verified_fumbles
        # Only test the contract statically: no HTTP or Supabase credentials.
        import inspect
        source = inspect.getsource(import_verified_fumbles)
        self.assertIn('"operation_id": operation_id', source)


if __name__ == "__main__":
    unittest.main()
