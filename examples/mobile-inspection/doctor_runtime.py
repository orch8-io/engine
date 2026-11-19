"""Offline checks for the mobile integration doctor's optional declarations."""

import base64
import binascii
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class RuntimeCheck:
    label: str
    status: str
    summary: str


def load_runtime_config(path: Path) -> object:
    def unique_object(pairs):
        result = dict(pairs)
        if len(result) != len(pairs):
            raise ValueError("Duplicate configuration field.")
        return result

    try:
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    except (OSError, ValueError, RecursionError):
        raise ValueError("Read a valid UTF-8 runtime configuration JSON file.") from None


def fields(value: object, allowed: set[str], label: str) -> dict:
    if not isinstance(value, dict) or value.keys() - allowed:
        raise ValueError(f"{label} contains an unsupported field or has an invalid shape.")
    return value


def flag(config: dict, key: str, label: str, configured: str, missing: str) -> RuntimeCheck:
    if key not in config:
        return RuntimeCheck(label, "UNVERIFIED", "No declaration provided.")
    value = config[key]
    if type(value) is not bool:
        raise ValueError(f"{label} must be declared as a boolean.")
    if value:
        return RuntimeCheck(label, "DECLARED", configured)
    return RuntimeCheck(label, "FAIL", missing)


def handlers(config: dict) -> RuntimeCheck:
    label = "Handler coverage"
    required, registered = config.get("required"), config.get("registered")
    for key in ("required", "registered"):
        if key in config and (
            not isinstance(config[key], list)
            or any(not isinstance(name, str) or not name.strip() for name in config[key])
        ):
            raise ValueError("Handler inventories must be lists of nonempty strings.")
    if required is None or registered is None:
        return RuntimeCheck(label, "UNVERIFIED", "Declare the required and app-registered inventories.")
    missing = len(set(required) - set(registered))
    if missing:
        return RuntimeCheck(label, "FAIL", f"{missing} required handler(s) missing from the declared registration inventory.")
    return RuntimeCheck(label, "DECLARED", "All required handlers are listed in the app registration inventory.")


def valid_https_descriptor(value: object) -> bool:
    if not isinstance(value, str) or not value or any(char.isspace() or ord(char) < 32 for char in value):
        return False
    if any(char in value for char in "?#\\"):
        return False
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except (ValueError, UnicodeError):
        return False
    return (
        parsed.scheme == "https"
        and bool(parsed.hostname)
        and "%" not in parsed.netloc
        and parsed.username is None
        and parsed.password is None
        and (port is None or port > 0)
    )


def valid_root_key(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return len(base64.b64decode(value, validate=True)) == 32
    except (binascii.Error, ValueError):
        return False


def manifest_checks(config: dict, mode: str | None) -> list[RuntimeCheck]:
    if mode is None:
        return [RuntimeCheck("Catalog manifest", "UNVERIFIED", "Declare bundled or signed catalog mode.")]
    if mode == "bundled":
        return [RuntimeCheck("Catalog manifest", "NOT REQUIRED", "Bundled mode uses local definitions.")]
    if mode != "signed":
        raise ValueError("Catalog manifest mode must be bundled or signed.")
    return [
        RuntimeCheck("Manifest URL", "CHECKED", "HTTPS URL has no user info, query or fragment.")
        if valid_https_descriptor(config.get("url"))
        else RuntimeCheck("Manifest URL", "FAIL", "Supply a token-free HTTPS manifest descriptor."),
        RuntimeCheck("Manifest root", "CHECKED", "Public key has the required 32-byte Base64 encoding.")
        if valid_root_key(config.get("root_public_key"))
        else RuntimeCheck("Manifest root", "FAIL", "Supply the Base64-encoded Ed25519 public root key."),
        RuntimeCheck("Manifest access and signature", "UNVERIFIED", "Run signed catalog sync on a configured device."),
    ]


def credential_freshness(config: dict, now: datetime) -> RuntimeCheck:
    label = "Credential freshness"
    if "expires_at" not in config:
        return RuntimeCheck(label, "UNVERIFIED", "No expiry timestamp supplied.")
    value = config["expires_at"]
    try:
        if not isinstance(value, str):
            raise ValueError
        expiry = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
        if expiry.utcoffset() is None:
            raise ValueError
    except ValueError:
        return RuntimeCheck(label, "FAIL", "Supply an ISO 8601 expiry timestamp with a timezone.")
    if expiry <= now:
        return RuntimeCheck(label, "FAIL", "The declared credential has expired.")
    return RuntimeCheck(label, "DECLARED", "The declared expiry is in the future.")


def auth_checks(config: dict, manifest_mode: str | None, now: datetime) -> list[RuntimeCheck]:
    mode = config.get("mode")
    label = "Token readiness"
    if mode is None:
        return [RuntimeCheck(label, "UNVERIFIED", "Declare how the app authenticates.")]
    if mode == "none" and manifest_mode == "signed":
        return [RuntimeCheck(label, "FAIL", "Declare public, device-session or signed-URL access for the signed catalog.")]
    if mode in ("none", "public"):
        return [RuntimeCheck(label, "NOT REQUIRED", "The declared access mode uses no device credential.")]
    if mode not in ("device_session", "signed_url"):
        raise ValueError("Authentication mode must be none, public, device_session or signed_url.")
    checks = []
    if mode == "device_session":
        checks.append(flag(config, "provider_configured", "Device session provider",
                           "App declares a device session provider.", "Configure a device session provider."))
    checks.append(flag(config, "credential_available", label,
                       "App reports a credential is available.", "Obtain a credential in the app."))
    checks.append(credential_freshness(config, now))
    checks.append(RuntimeCheck("Credential acceptance", "UNVERIFIED", "Check authentication in a device integration test."))
    return checks


def push_checks(config: dict, platform: str) -> list[RuntimeCheck]:
    provider = "APNs" if platform == "ios" else "FCM"
    requirements = (
        [
            ("apns_entitlement_present", "APNs entitlement", "Enable Push Notifications for the signed app."),
            ("background_remote_notifications_enabled", "APNs background wake", "Enable the remote notification background mode."),
        ] if platform == "ios" else [
            ("fcm_configuration_present", "FCM app configuration", "Configure Firebase Cloud Messaging for the app."),
            ("messaging_service_configured", "FCM messaging service", "Connect the messaging service in the app."),
        ]
    ) + [
        ("push_token_available", f"{provider} token readiness", "Obtain a push token on the device."),
        ("wake_callback_configured", f"{provider} wake callback", "Forward wake hints to the Orch8 SDK."),
    ]
    config = fields(config, {"enabled", *(key for key, _, _ in requirements)}, f"{provider} configuration")
    if "enabled" not in config:
        return [RuntimeCheck(f"{provider} push", "UNVERIFIED", "Declare whether push wake is enabled.")]
    if type(config["enabled"]) is not bool:
        raise ValueError(f"{provider} enablement must be declared as a boolean.")
    if not config["enabled"]:
        return [RuntimeCheck(f"{provider} push", "NOT REQUIRED", "Push wake is disabled for this integration.")]
    checks = [flag(config, key, label, "App declares this requirement is met.", fix)
              for key, label, fix in requirements]
    checks.append(RuntimeCheck(f"{provider} delivery", "UNVERIFIED", "Verify delivery and wake on a configured device."))
    return checks


def runtime_checks(config: object, platform: str, now: datetime | None = None) -> list[RuntimeCheck]:
    config = fields(config, {"handlers", "manifest", "auth", "push"}, "Runtime configuration")
    handler_config = fields(config.get("handlers", {}), {"required", "registered"}, "Handler declaration")
    manifest = fields(config.get("manifest", {}), {"mode", "url", "root_public_key"}, "Catalog manifest")
    auth = fields(config.get("auth", {}), {"mode", "provider_configured", "credential_available", "expires_at"}, "Authentication")
    push = fields(config.get("push", {}), {"ios", "android"}, "Push configuration")
    manifest_mode = manifest.get("mode")
    checks = [handlers(handler_config), *manifest_checks(manifest, manifest_mode),
              *auth_checks(auth, manifest_mode, now if now is not None else datetime.now(timezone.utc))]
    for selected in ("ios", "android") if platform == "all" else (platform,):
        checks.extend(push_checks(push.get(selected, {}), selected))
    return checks
