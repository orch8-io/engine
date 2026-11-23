#!/usr/bin/env python3
"""Unit tests for preparation of the previous PostgreSQL connection pool."""

import importlib.util
from pathlib import Path
import unittest
from urllib.parse import parse_qsl, urlsplit


spec = importlib.util.spec_from_file_location(
    "rolling_postgres", Path(__file__).with_name("test-rolling-postgres.py")
)
rolling = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rolling)


class PreviousDatabaseUrlTest(unittest.TestCase):
    def test_adds_the_option_to_a_url_without_settings(self):
        self.assertEqual(
            rolling.previous_database_url("postgresql://db/database"),
            "postgresql://db/database?statement-cache-capacity=0",
        )

    def test_replaces_old_capacities_and_preserves_other_options(self):
        source = ("postgresql://alice:p%40ss@db/database?"
                  "options=-c%20search_path%3Dtenant&statement-cache-capacity=100&"
                  "sslmode=require&application_name=&statement-cache-capacity=3")
        result = urlsplit(rolling.previous_database_url(source))
        self.assertEqual(result.netloc, "alice:p%40ss@db")
        self.assertEqual(result.path, "/database")
        self.assertEqual(parse_qsl(result.query, keep_blank_values=True), [
            ("options", "-c search_path=tenant"),
            ("sslmode", "require"),
            ("application_name", ""),
            ("statement-cache-capacity", "0"),
        ])


if __name__ == "__main__":
    unittest.main()
