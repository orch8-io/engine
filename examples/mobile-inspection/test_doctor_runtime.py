"""Behavior tests for the offline mobile integration report."""

import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from doctor_runtime import runtime_checks


DOCTOR = Path(__file__).resolve().with_name("doctor.py")
NOW = datetime(2026, 1, 2, tzinfo=timezone.utc)
KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def report(config, platform="all"):
    return {item.label: item for item in runtime_checks(config, platform, NOW)}


class RuntimeDoctorTests(unittest.TestCase):
    def test_signed_catalog_and_push_keep_network_and_device_evidence_unverified(self):
        backend = "https://catalog.example.invalid/mobile/catalog.json"
        config = {
            "handlers": {"required": ["capture", "submit"], "registered": ["submit", "capture", "extra"]},
            "manifest": {"mode": "signed", "url": backend, "root_public_key": KEY},
            "auth": {
                "mode": "device_session", "provider_configured": True,
                "credential_available": True, "expires_at": "2026-01-02T01:00:00Z",
            },
            "push": {
                "ios": {
                    "enabled": True, "apns_entitlement_present": True,
                    "background_remote_notifications_enabled": True, "push_token_available": True,
                    "wake_callback_configured": True,
                },
                "android": {"enabled": False},
            },
        }
        with patch("socket.create_connection", side_effect=AssertionError("runtime report attempted network I/O")):
            results = report(config)
        expected = {
            "Handler coverage": "DECLARED",
            "Manifest URL": "CHECKED",
            "Manifest root": "CHECKED",
            "Manifest access and signature": "UNVERIFIED",
            "Device session provider": "DECLARED",
            "Token readiness": "DECLARED",
            "Credential freshness": "DECLARED",
            "Credential acceptance": "UNVERIFIED",
            "APNs entitlement": "DECLARED",
            "APNs background wake": "DECLARED",
            "APNs token readiness": "DECLARED",
            "APNs wake callback": "DECLARED",
            "APNs delivery": "UNVERIFIED",
            "FCM push": "NOT REQUIRED",
        }
        self.assertEqual({label: check.status for label, check in results.items()}, expected)
        self.assertNotIn(backend, repr(results))
        self.assertNotIn(KEY, repr(results))

    def test_reports_missing_handler_count_and_never_echoes_handler_names(self):
        results = report({
            "handlers": {
                "required": ["private-handler", "private-handler", "installed-handler"],
                "registered": ["installed-handler"],
            },
        }, "ios")
        self.assertEqual(results["Handler coverage"].status, "FAIL")
        self.assertIn("1 required handler", results["Handler coverage"].summary)
        self.assertNotIn("private-handler", repr(results))
        self.assertNotIn("installed-handler", repr(results))
        self.assertEqual(results["Catalog manifest"].status, "UNVERIFIED")
        self.assertEqual(results["Token readiness"].status, "UNVERIFIED")
        self.assertEqual(results["APNs push"].status, "UNVERIFIED")
        self.assertNotIn("FCM push", results)

    def test_rejects_insecure_or_credential_bearing_urls_and_malformed_roots(self):
        for url in [
            "http://catalog.example.invalid",
            "https://someone:secret@catalog.example.invalid",
            "https://catalog.example.invalid?signature=secret",
            "https://catalog.example.invalid/#secret",
            "https://catalog.example.invalid:999999",
            "https://foo%40bar",
            "https://catalog.example.invalid/white space",
            "https:///no-host",
        ]:
            with self.subTest(url=url):
                results = report({
                    "manifest": {"mode": "signed", "url": url, "root_public_key": "invalid-root"},
                    "auth": {"mode": "public"},
                }, "android")
                self.assertEqual(results["Manifest URL"].status, "FAIL")
                self.assertEqual(results["Manifest root"].status, "FAIL")
                self.assertEqual(results["Token readiness"].status, "NOT REQUIRED")
                self.assertNotIn(url, repr(results))

    def test_token_expiry_and_missing_platform_hooks_are_distinct(self):
        results = report({
            "auth": {
                "mode": "device_session", "provider_configured": False,
                "credential_available": True, "expires_at": "2026-01-02T01:00:00+01:00",
            },
            "push": {
                "android": {
                    "enabled": True, "fcm_configuration_present": True,
                    "messaging_service_configured": False, "push_token_available": False,
                },
            },
        }, "android")
        self.assertEqual(results["Device session provider"].status, "FAIL")
        self.assertEqual(results["Token readiness"].status, "DECLARED")
        self.assertEqual(results["Credential freshness"].status, "FAIL")
        self.assertEqual(results["FCM messaging service"].status, "FAIL")
        self.assertEqual(results["FCM token readiness"].status, "FAIL")
        self.assertEqual(results["FCM wake callback"].status, "UNVERIFIED")
        self.assertNotIn("APNs push", results)
        self.assertEqual(report({"auth": {"mode": "signed_url", "expires_at": "2030-01-02"}})
                         ["Credential freshness"].status, "FAIL")
        self.assertEqual(report({"auth": {"mode": "signed_url", "credential_available": True}})
                         ["Credential freshness"].status, "UNVERIFIED")

    def test_bundled_report_runs_without_build_tools(self):
        config = {
            "handlers": {"required": [], "registered": []},
            "manifest": {"mode": "bundled"},
            "auth": {"mode": "none"},
            "push": {"ios": {"enabled": False}, "android": {"enabled": False}},
        }
        result = self.invoke(json.dumps(config))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Bundled mode uses local definitions", result.stdout)
        self.assertIn("NOT REQUIRED", result.stdout)
        self.assertNotIn("Local build prerequisites", result.stdout)

    def test_invalid_and_secret_bearing_config_never_prints_raw_values_or_paths(self):
        marker = "private-value-do-not-print"
        for config in [
            '{"auth": {"mode": "device_session", "' + marker + '": "' + marker + '"}}',
            '{"auth": {"mode": "device_session", "mode": "' + marker + '"}}',
            '{"manifest": {"mode": "' + marker + '"}}',
            '{"push": {"android": {"enabled": "' + marker + '"}}}',
            '{"auth":' + marker,
        ]:
            with self.subTest(config=config):
                result = self.invoke(config)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("FAIL", result.stdout)
                self.assertNotIn(marker, result.stdout + result.stderr)
        result = subprocess.run([sys.executable, str(DOCTOR), "android", "--token", marker],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertNotIn(marker, result.stdout + result.stderr)

    @staticmethod
    def invoke(config):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "private-value-do-not-print.json"
            file.write_text(config, encoding="utf-8")
            return subprocess.run([sys.executable, str(DOCTOR), "all", "--runtime-only", "--runtime-config", str(file)],
                                  capture_output=True, text=True)


if __name__ == "__main__":
    unittest.main()
