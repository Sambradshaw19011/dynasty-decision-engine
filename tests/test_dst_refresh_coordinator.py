"""Offline tests for D/ST refresh stage ordering and recovery behavior."""
import unittest
from types import SimpleNamespace

from src.ingestion.nflverse.dst_refresh_coordinator import RefreshJournal


class FakeClient:
    def __init__(self, fail_transition=None):
        self.transitions = []
        self.fail_transition = fail_transition

    def rpc(self, name, params):
        if name == "start_dst_refresh_operation":
            data = "00000000-0000-0000-0000-000000000001"
        elif name == "advance_dst_refresh_operation":
            self.transitions.append((params["p_expected_status"], params["p_next_status"]))
            if params["p_next_status"] == self.fail_transition:
                data = {"status": "incorrect"}
            else:
                data = {"status": params["p_next_status"]}
        else:
            raise AssertionError(name)
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=data))


class CoordinatorTests(unittest.TestCase):
    def journal(self, client):
        return RefreshJournal.begin(
            client, season=2026, backup_parquet_path="backup",
            backup_parquet_sha256="a" * 64,
            backup_database_sha256="b" * 64,
            staged_parquet_path="staging",
            staged_parquet_sha256="c" * 64,
        )

    def test_successful_stage_order(self):
        client = FakeClient()
        journal = self.journal(client)
        actions = []
        journal.run(
            promote=lambda: actions.append("promote"),
            import_events=lambda op: actions.append("import"),
            score=lambda op: actions.append("score"),
            verify=lambda: actions.append("verify"),
            recover=lambda _: self.fail("unexpected recovery"),
        )
        self.assertEqual(actions, ["promote", "import", "score", "verify"])
        self.assertEqual(journal.status, "completed")
        self.assertEqual(
            client.transitions,
            list(zip(("prepared", "promoting", "importing", "scoring", "verifying"),
                     ("promoting", "importing", "scoring", "verifying", "completed"))),
        )

    def test_each_failure_invokes_recovery_and_never_completes(self):
        for failed in ("promote", "import", "score", "verify"):
            with self.subTest(failed=failed):
                journal = self.journal(FakeClient())
                recovered = []
                def action(name):
                    if name == failed:
                        raise ValueError("injected failure")
                with self.assertRaisesRegex(RuntimeError, "recovery completed"):
                    journal.run(
                        promote=lambda: action("promote"),
                        import_events=lambda op: action("import"),
                        score=lambda op: action("score"),
                        verify=lambda: action("verify"),
                        recover=lambda op: recovered.append(op),
                    )
                self.assertEqual(recovered, [journal.operation_id])
                self.assertNotEqual(journal.status, "completed")

    def test_failed_recovery_blocks_completion(self):
        journal = self.journal(FakeClient())
        def fail():
            raise ValueError("injected")
        with self.assertRaisesRegex(RuntimeError, "recovery also failed"):
            journal.run(promote=fail, import_events=lambda: None,
                        score=lambda: None, verify=lambda: None,
                        recover=lambda _: fail())
        self.assertEqual(journal.status, "promoting")

    def test_failed_transition_does_not_run_stage(self):
        journal = self.journal(FakeClient(fail_transition="promoting"))
        called = []
        with self.assertRaisesRegex(RuntimeError, "remains prepared and blocked"):
            journal.run(promote=lambda: called.append("promote"),
                        import_events=lambda op: None, score=lambda op: None,
                        verify=lambda: None,
                        recover=lambda _: called.append("recover"))
        self.assertEqual(called, [])
        self.assertEqual(journal.status, "prepared")


if __name__ == "__main__":
    unittest.main()
