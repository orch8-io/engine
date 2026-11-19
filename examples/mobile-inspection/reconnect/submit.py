#!/usr/bin/env python3
"""Submit a completed offline inspection to a running Orch8 engine."""

import argparse
import json
import os
import re
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID


def read_inspection(data):
    required = {"instance_id", "site_id", "inspection_result"}
    allowed = required | {"site_tag_verified"}
    if not isinstance(data, dict) or not required <= data.keys() <= allowed:
        raise ValueError("expected only the fields in the inspection export")
    site_id = data["site_id"]
    if not isinstance(site_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", site_id):
        raise ValueError("invalid site ID")
    result = data["inspection_result"]
    if result not in ("passed", "repair_needed"):
        raise ValueError("invalid inspection result")
    fields = dict(data)
    fields["instance_id"] = str(UUID(str(data["instance_id"])))
    if "site_tag_verified" in fields and not isinstance(fields["site_tag_verified"], bool):
        raise ValueError("verification flag must be boolean")
    return fields


def provision_inspection(request, sequence_id, tenant, incoming):
    record = read_inspection(incoming)
    sequence_id = str(UUID(sequence_id))
    response = request("/instances", {
        "sequence_id": sequence_id,
        "tenant_id": tenant,
        "namespace": "default",
        "idempotency_key": f"field-inspection:{record['instance_id']}",
        "context": {"data": record},
    })
    execution = str(UUID(response["id"]))
    if response.get("deduplicated") is not False:
        stored = request(f"/instances/{execution}")
        data = stored["context"]["data"]
        original = {key: data[key] for key in (
            "instance_id", "site_id", "inspection_result", "site_tag_verified"
        ) if key in data}
        if stored.get("sequence_id") != sequence_id or original != record:
            raise ValueError("the inspection ID was previously submitted with different evidence")
    return {
        "submission_id": record["instance_id"],
        "server_execution_id": execution,
        "outcome": "acknowledged",
    }


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def engine_client(url, tenant, api_key):
    base = url.rstrip("/")
    parsed = urlsplit(base)
    loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if (parsed.scheme != "https" and not (parsed.scheme == "http" and loopback)
            or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment):
        raise ValueError("use HTTPS or an HTTP loopback URL without embedded credentials")
    if not base.endswith("/api/v1"):
        base += "/api/v1"
    opener = build_opener(NoRedirect())

    def request(path, body=None):
        headers = {"X-Tenant-Id": tenant, "Content-Type": "application/json"}
        if api_key:
            headers["X-API-Key"] = api_key
        payload = json.dumps(body).encode() if body is not None else None
        try:
            with opener.open(Request(base + path, data=payload, headers=headers), timeout=10) as response:
                data = response.read(1024 * 1024 + 1)
                if len(data) > 1024 * 1024:
                    raise ValueError("engine response exceeded the inspection limit")
                return json.loads(data)
        except HTTPError as error:
            error.close()
            raise ValueError(f"engine returned HTTP {error.code}") from None
    return request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("export_file")
    parser.add_argument("--sequence-id", required=True)
    parser.add_argument("--url", default=os.getenv("ORCH8_URL", "http://127.0.0.1:8080/api/v1"))
    parser.add_argument("--tenant", default=os.getenv("ORCH8_TENANT_ID", "demo"))
    args = parser.parse_args()
    try:
        with open(args.export_file, "rb") as source:
            data = source.read(4097)
        if len(data) > 4096:
            raise ValueError("inspection export exceeded 4096 bytes")
        request = engine_client(args.url, args.tenant, os.getenv("ORCH8_API_KEY"))
        print(json.dumps(provision_inspection(request, args.sequence_id, args.tenant, json.loads(data))))
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"{error}\n")


if __name__ == "__main__":
    main()
