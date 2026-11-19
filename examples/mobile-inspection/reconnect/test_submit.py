import unittest

from submit import provision_inspection, read_inspection


RUN = "2c6ef7d8-67c0-47aa-a1a4-714e8baa445b"
SERVER = "2c6ef7d8-67c0-47aa-a1a4-714e8baa445c"
SEQUENCE = "2c6ef7d8-67c0-47aa-a1a4-714e8baa445d"
RECORD = {"instance_id": RUN, "site_id": "A-12", "inspection_result": "passed"}


class ReconnectTest(unittest.TestCase):
    def test_export_excludes_unrecognized_or_non_boolean_evidence(self):
        record = read_inspection({**RECORD, "site_tag_verified": True})
        self.assertEqual(record["site_tag_verified"], True)
        for record in [
            {**RECORD, "photo": "private-bytes"},
            {**RECORD, "site_tag_verified": "1"},
            {**RECORD, "inspection_result": "unrecognized"},
        ]:
            with self.assertRaises(ValueError):
                read_inspection(record)

    def test_reconnect_returns_a_stable_server_acknowledgement(self):
        calls = []
        stored = {}

        def request(path, body=None):
            calls.append((path, body))
            if path == "/instances":
                duplicate = bool(stored)
                if not duplicate:
                    stored.update(body)
                return {"id": SERVER, "deduplicated": duplicate}
            return {"sequence_id": stored["sequence_id"], "context": stored["context"]}

        first = provision_inspection(request, SEQUENCE, "demo", RECORD)
        stored["context"]["data"]["supervisor_decision"] = "approved"
        second = provision_inspection(request, SEQUENCE, "demo", RECORD)
        self.assertEqual(first, second)
        self.assertEqual(first["server_execution_id"], SERVER)
        self.assertEqual(calls[0][1]["idempotency_key"], f"field-inspection:{RUN}")
        self.assertEqual(calls[0][1]["context"]["data"]["instance_id"], RUN)
        with self.assertRaises(ValueError):
            provision_inspection(request, SEQUENCE, "demo", {**RECORD, "inspection_result": "repair_needed"})
