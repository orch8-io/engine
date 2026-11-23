#!/usr/bin/env python3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from runpy import run_path
import unittest


AcknowledgementGate = run_path(
    str(Path(__file__).with_name("fixtures") / "upgrade-tls-proxy.py")
)["AcknowledgementGate"]


class AcknowledgementGateTests(unittest.TestCase):
    def test_only_successful_submissions_consume_the_gate(self):
        gate = AcknowledgementGate()
        for method, path, status in [
            ("GET", "/api/v1/instances/id", 200),
            ("POST", "/api/v1/instances/id", 201),
            ("POST", "/api/v1/instances", 422),
        ]:
            self.assertEqual(gate.response_to_client(method, path, status, b"original"), (status, b"original"))
        self.assertEqual(gate.response_to_client("POST", "/api/v1/instances", 201, b"first")[0], 503)
        self.assertEqual(gate.response_to_client("POST", "/api/v1/instances", 200, b"receipt"), (200, b"receipt"))

    def test_concurrent_responses_withhold_exactly_one_acknowledgement(self):
        gate = AcknowledgementGate()
        with ThreadPoolExecutor(max_workers=8) as threads:
            responses = list(threads.map(
                lambda _: gate.response_to_client("POST", "/api/v1/instances", 201, b"receipt"),
                range(24),
            ))
        self.assertEqual([status for status, _ in responses].count(503), 1)
        self.assertEqual(responses.count((201, b"receipt")), 23)


if __name__ == "__main__":
    unittest.main()
