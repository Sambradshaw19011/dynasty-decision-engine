"""Offline tests for immutable D/ST database backup validation."""
import hashlib
import json
import unittest
from unittest.mock import Mock, patch

from src.ingestion.nflverse.dst_durable_backup import verify_remote_snapshot


def make_document():
    return {
        "season": 2026,
        "tables": {
            "fact_team_fumble_events": {
                "season_column": "season_id", "row_count": 1,
                "rows": [{"season_id": 2026, "fumble_event_id": 1,
                          "game_id": "g", "play_id": 1, "recovery_sequence": 1}],
            },
            "fact_calculated_team_weekly_scores": {
                "season_column": "season_id", "row_count": 1,
                "rows": [{"season_id": 2026, "calculated_team_score_id": 1,
                          "scoring_settings_id": 1, "game_id": "g", "team_code": "SF"}],
            },
        },
    }


class DurableBackupTests(unittest.TestCase):
    def verify(self, document, *, corrupt=False):
        payload = json.dumps(document).encode()
        digest = hashlib.sha256(payload).hexdigest()
        client = Mock()
        client.storage.from_.return_value.download.return_value = (
            payload + b"corrupt" if corrupt else payload
        )
        path = f"dst_database_backups/2026/{digest}.json"
        return verify_remote_snapshot(
            client, bucket="nflverse-data", season=2026,
            object_path=path, digest=digest,
        )

    def test_valid_snapshot(self):
        self.assertEqual(self.verify(make_document())["season"], 2026)

    def test_corrupted_bytes_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "checksum"):
            self.verify(make_document(), corrupt=True)

    def test_missing_table_rejected(self):
        document = make_document()
        del document["tables"]["fact_team_fumble_events"]
        with self.assertRaisesRegex(RuntimeError, "table set"):
            self.verify(document)

    def test_duplicate_natural_keys_rejected(self):
        document = make_document()
        table = document["tables"]["fact_team_fumble_events"]
        row = dict(table["rows"][0])
        row["fumble_event_id"] = 2
        table["rows"].append(row)
        table["row_count"] = 2
        with self.assertRaisesRegex(RuntimeError, "Duplicate"):
            self.verify(document)

    def test_wrong_season_rejected(self):
        document = make_document()
        document["tables"]["fact_team_fumble_events"]["rows"][0]["season_id"] = 2025
        with self.assertRaisesRegex(RuntimeError, "season"):
            self.verify(document)


if __name__ == "__main__":
    unittest.main()
