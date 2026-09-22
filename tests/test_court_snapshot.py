import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bot


class CourtSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        db_patch = patch.object(bot, "DB_PATH", Path(self.temp.name) / "test.sqlite3")
        db_patch.start()
        self.addCleanup(db_patch.stop)
        bot.init_db()
        groups_patch = patch.object(bot, "moved_groups", return_value={})
        groups_patch.start()
        self.addCleanup(groups_patch.stop)

    def test_history_selects_only_same_deals_and_survives_refresh(self):
        court = bot.JUDICIAL_PIPELINE_ID

        class Client:
            events_enabled = True
            leads = {
                1: {"id": 1, "pipeline_id": 42, "status_id": 142},
                2: {"id": 2, "pipeline_id": court, "status_id": 10},
                3: {"id": 3, "pipeline_id": 43, "status_id": 143},
                4: {"id": 4, "pipeline_id": court, "status_id": 10},
                # An unrelated deal must not appear, even with matching contacts.
                5: {"id": 5, "pipeline_id": 42, "status_id": 142},
            }

            def get_pipeline(self, pipeline_id):
                return {"_embedded": {"statuses": [{"id": 10}]}}

            def get(self, path, params):
                assert path == "/api/v4/events"
                if not self.events_enabled:
                    return None
                page = dict(params)["page"]
                ids = [1, 2] if page == 1 else [3, 6]
                return {
                    "_embedded": {"events": [
                        {"entity_id": lead_id, "created_at": 100,
                         "value_before": [{"lead_status": {"pipeline_id": court, "id": 10}}]}
                        for lead_id in ids
                    ]},
                    "_links": {"next": {"href": "page2"}} if page == 1 else {},
                }

            def list_pipeline_leads(self, pipeline_id):
                return [lead for lead in self.leads.values() if lead["pipeline_id"] == pipeline_id]

            def get_leads_by_ids(self, ids):
                return [self.leads[lead_id] for lead_id in ids if lead_id in self.leads]

        client = Client()
        bot.remember_source_history_lead(1, bot.REFERRAL_PIPELINE_IDS[0], 10, "test")
        self.assertEqual(bot.refresh_court_snapshot(client)["ids"], [3, 1])
        self.assertEqual(bot.source_history_ids(bot.REFERRAL_PIPELINE_IDS), [1])
        # Previously observed membership remains available without new events.
        client.events_enabled = False
        client.leads[4]["pipeline_id"] = 44
        self.assertEqual(bot.refresh_court_snapshot(client)["ids"], [4, 3, 1])
        # A returned deal is hidden while it is back in the source pipeline.
        client.leads[1]["pipeline_id"] = court
        self.assertEqual(bot.refresh_court_snapshot(client)["ids"], [4, 3])


if __name__ == "__main__":
    unittest.main()
