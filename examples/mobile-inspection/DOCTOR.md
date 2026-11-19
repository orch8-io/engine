# Mobile integration doctor

The doctor checks the starter’s SDK pins and build prerequisites by default.
Add `--runtime-config` to report on the app’s declared runtime configuration.
Use `--runtime-only` to run the report without native build tools.

```sh
python3 doctor.py all --runtime-only --runtime-config ./runtime-check.json
python3 doctor.py ios --test --runtime-config ./runtime-check.json
```

Select `ios`, `android` or `all` to report the corresponding push
configuration. Start with a declaration like this for the offline starter.

```json
{
  "handlers": {
    "required": ["save_inspection"],
    "registered": ["save_inspection"]
  },
  "manifest": {"mode": "bundled"},
  "auth": {"mode": "none"},
  "push": {
    "ios": {"enabled": false},
    "android": {"enabled": false}
  }
}
```

## Connected integrations

- List all handlers the target app needs under `handlers.required`. Use
  `handlers.registered` for the app’s registration inventory. The report
  prints the missing count and keeps handler names out of its output.
- For signed catalogs, set `manifest.mode` to `signed`. Set `manifest.url` to
  a token-free HTTPS descriptor without credentials, query or fragment.
  Supply the 32-byte Base64-encoded Ed25519 public authority key under
  `manifest.root_public_key`. The doctor checks the URL’s shape and the
  key’s encoding and length. Live access, key matching and signature
  verification remain unverified.
- Set `auth.mode` to the actual mode, such as `public`, `device_session`
  or `signed_url`. For device sessions, declare `provider_configured`.
  For device sessions or signed URLs, declare `credential_available` and
  optionally supply the credential’s `expires_at` as an ISO 8601
  timestamp with timezone. Credential acceptance remains unverified.
- For APNs wake, set `push.ios.enabled` and declare
  `apns_entitlement_present`, `background_remote_notifications_enabled`,
  `push_token_available` and `wake_callback_configured` as booleans.
  For FCM wake, set `push.android.enabled` and declare
  `fcm_configuration_present`, `messaging_service_configured`,
  `push_token_available` and `wake_callback_configured`. Delivery and
  the resulting device wake remain unverified.

Keep bearer tokens, signed URLs, push tokens and device or account
identifiers out of the configuration. The runtime report uses local
inputs and makes no network requests. Omit flags you have not confirmed to get
`UNVERIFIED`. Use `false` to report a known missing requirement.

`CHECKED` indicates a local format check. `DECLARED` indicates evidence
supplied by the app operator. A malformed configuration or a `FAIL`
returns exit code 1. `UNVERIFIED` is informational and requires
device-side confirmation.
