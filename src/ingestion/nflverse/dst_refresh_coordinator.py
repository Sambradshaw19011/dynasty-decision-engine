"""D/ST refresh journal orchestration primitives.

No production writes occur on import. The production refresh must call begin()
only after verified, durable backups exist. This module never marks an operation
completed without an explicit caller verification step.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

STAGES = ("prepared", "promoting", "importing", "scoring", "verifying", "completed")
RECOVERABLE = frozenset(STAGES[:-1]) | {"recovery_failed"}


@dataclass
class RefreshJournal:
    client: object
    operation_id: str
    status: str = "prepared"

    @classmethod
    def begin(cls, client, *, season: int, backup_parquet_path: str,
              backup_parquet_sha256: str, backup_database_sha256: str,
              staged_parquet_path: str, staged_parquet_sha256: str,
              backup_database_path: str | None = None):
        params = {
            "p_season": season,
            "p_backup_parquet_path": backup_parquet_path,
            "p_backup_parquet_sha256": backup_parquet_sha256,
            "p_backup_database_sha256": backup_database_sha256,
            "p_staged_parquet_path": staged_parquet_path,
            "p_staged_parquet_sha256": staged_parquet_sha256,
        }
        if backup_database_path is not None:
            params["p_backup_database_path"] = backup_database_path
        operation_id = client.rpc(
            "start_dst_refresh_operation_v2" if backup_database_path else "start_dst_refresh_operation",
            params,
        ).execute().data
        if not isinstance(operation_id, str) or not operation_id:
            raise RuntimeError("D/ST refresh journal start not confirmed")
        return cls(client, operation_id)

    def advance(self, next_status: str, details: dict | None = None):
        if self.status not in STAGES or next_status not in STAGES:
            raise RuntimeError("Invalid forward journal transition")
        if STAGES.index(next_status) != STAGES.index(self.status) + 1:
            raise RuntimeError(f"Cannot advance from {self.status} to {next_status}")
        result = self.client.rpc("advance_dst_refresh_operation", {
            "p_operation_id": self.operation_id,
            "p_expected_status": self.status,
            "p_next_status": next_status,
            "p_details": details or {},
        }).execute().data
        if not isinstance(result, dict) or result.get("status") != next_status:
            raise RuntimeError(f"Journal did not confirm {next_status}")
        self.status = next_status

    def run(self, *, promote: Callable, import_events: Callable,
            score: Callable, verify: Callable, recover: Callable):
        """Execute stages; never swallow failed recovery.

        recover(operation_id) must restore both database and Parquet, verify
        them, and mark the journal recovered. An interruption before recovery
        leaves a journal entry for later inspection.
        """
        try:
            for status, action in (
                ("promoting", promote),
                ("importing", import_events),
                ("scoring", score),
                ("verifying", verify),
            ):
                self.advance(status)
                action()
            self.advance("completed")
        except Exception as original:
            try:
                recover(self.operation_id)
            except Exception as recovery_error:
                raise RuntimeError(
                    f"Refresh failed at {self.status}; recovery also failed. "
                    f"Operation {self.operation_id} requires intervention."
                ) from recovery_error
            raise RuntimeError(
                f"Refresh failed at {self.status}; recovery completed. "
                f"Operation {self.operation_id}."
            ) from original


def begin_verified_refresh(client, *, season: int, staged_digest: str,
                           backup_parquet_digest: str,
                           backup_database_digest: str) -> RefreshJournal:
    """Verify all immutable artifacts before acquiring the season journal lease.

    This is the only supported entry point for a production coordinator.
    A separate runner must supply stage actions and recovery behavior.
    """
    from src.ingestion.nflverse.verify_dst_refresh_artifacts import verify_artifacts

    report = verify_artifacts(
        client, season=season, staged_digest=staged_digest,
        backup_parquet_digest=backup_parquet_digest,
        backup_database_digest=backup_database_digest,
    )
    paths = report["paths"]
    return RefreshJournal.begin(
        client, season=season,
        backup_parquet_path=paths["backup_parquet"],
        backup_parquet_sha256=backup_parquet_digest,
        backup_database_sha256=backup_database_digest,
        backup_database_path=paths["backup_database"],
        staged_parquet_path=paths["staging"],
        staged_parquet_sha256=staged_digest,
    )


def run_verified_refresh(client, *, season: int, staged_digest: str,
                         backup_parquet_digest: str, backup_database_digest: str,
                         promote: Callable, import_events: Callable,
                         score: Callable, verify: Callable,
                         recover: Callable) -> str:
    """Run the production stages under one durable, verified journal.

    The caller owns each concrete action and must supply a recovery function
    that fully restores database and Parquet and closes the journal. This
    function does not bypass refresh_dst.py's production execution gate.
    """
    journal = begin_verified_refresh(
        client, season=season, staged_digest=staged_digest,
        backup_parquet_digest=backup_parquet_digest,
        backup_database_digest=backup_database_digest,
    )
    journal.run(promote=promote, import_events=import_events, score=score,
                verify=verify, recover=recover)
    return journal.operation_id
