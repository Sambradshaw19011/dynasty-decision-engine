"""Offline integration tests for the verified D/ST stage runner."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.ingestion.nflverse.dst_refresh_coordinator import run_verified_refresh


class Client:
    def __init__(self):
        self.transitions = []
        self.operation_id = "00000000-0000-0000-0000-000000000001"

    def rpc(self, name, params):
        if name == "start_dst_refresh_operation_v2":
            data = self.operation_id
        elif name == "advance_dst_refresh_operation":
            self.transitions.append((params["p_expected_status"], params["p_next_status"]))
            data = {"status": params["p_next_status"]}
        else:
            raise AssertionError(f"Unexpected RPC: {name}")
        return SimpleNamespace(execute=lambda: SimpleNamespace(data=data))


class VerifiedRunnerTests(unittest.TestCase):
    def setUp(self):
        self.client = Client()
        self.kwargs = dict(season=2026, staged_digest="a" * 64,
                           backup_parquet_digest="b" * 64,
                           backup_database_digest="c" * 64)
        self.paths = dict(
            staging=f"play_by_play_staging/2026/{'a'*64}.parquet",
            backup_parquet=f"play_by_play_backups/2026/{'b'*64}.parquet",
            backup_database=f"dst_database_backups/2026/{'c'*64}.json",
        )

    def run_stages(self, failure=None, recover_failure=False):
        actions = []
        def stage(name):
            def call(*args):
                if name in ("import", "score"):
                    self.assertEqual(args, (self.client.operation_id,))
                else:
                    self.assertEqual(args, ())
                actions.append(name)
                if failure == name:
                    raise RuntimeError(f"failed {name}")
            return call
        def recover(op):
            actions.append("recover")
            self.assertEqual(op, self.client.operation_id)
            if recover_failure:
                raise RuntimeError("recovery failed")
        with patch("src.ingestion.nflverse.verify_dst_refresh_artifacts.verify_artifacts",
                   return_value={"paths": self.paths}):
            try:
                result = run_verified_refresh(
                    self.client, **self.kwargs,
                    promote=stage("promote"), import_events=stage("import"),
                    score=stage("score"), verify=stage("verify"), recover=recover)
                return actions, result
            except RuntimeError:
                self.actions = actions
                raise

    def test_successful_order_and_completion(self):
        actions, operation_id = self.run_stages()
        self.assertEqual(actions, ["promote", "import", "score", "verify"])
        self.assertEqual(operation_id, self.client.operation_id)
        self.assertEqual(self.client.transitions, [
            ("prepared", "promoting"), ("promoting", "importing"),
            ("importing", "scoring"), ("scoring", "verifying"),
            ("verifying", "completed"),
        ])

    def test_each_stage_failure_recovers_and_does_not_complete(self):
        for stage in ("promote", "import", "score", "verify"):
            with self.subTest(stage=stage):
                self.client = Client()
                with self.assertRaisesRegex(RuntimeError, "recovery completed"):
                    self.run_stages(failure=stage)
                self.assertEqual(self.actions[-1], "recover")
                self.assertNotIn(("verifying", "completed"), self.client.transitions)

    def test_recovery_failure_does_not_complete(self):
        with self.assertRaisesRegex(RuntimeError, "recovery also failed"):
            self.run_stages(failure="import", recover_failure=True)
        self.assertNotIn(("verifying", "completed"), self.client.transitions)

    def test_invalid_artifacts_never_start_or_run(self):
        with patch("src.ingestion.nflverse.verify_dst_refresh_artifacts.verify_artifacts",
                   side_effect=RuntimeError("invalid artifact")):
            with self.assertRaisesRegex(RuntimeError, "invalid artifact"):
                run_verified_refresh(
                    self.client, **self.kwargs,
                    promote=lambda: self.fail("promoted"),
                    import_events=lambda: self.fail("imported"),
                    score=lambda: self.fail("scored"),
                    verify=lambda: self.fail("verified"),
                    recover=lambda _: self.fail("recovered"))
        self.assertEqual(self.client.transitions, [])


if __name__ == "__main__":
    unittest.main()
