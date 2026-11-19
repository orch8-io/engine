# Mobile catalog release rings

Use `orch8_publisher::catalog_rollout` in the backend that serves an
authenticated mobile catalog. It selects a stable or canary snapshot and
signs the existing `orch8.catalog.v1` envelope. Existing scoped mobile
clients verify the envelope with their configured catalog authority.

## Serve and acknowledge

1. Authenticate the caller and look up its enrolled device, app version,
   SDK version, and authorized catalog audience. Use the audience pinned
   by the client. The app and SDK versions are numeric, with one to three
   components. The stable and canary snapshots each set a minimum app
   version. Sequence entries carry their existing minimum SDK versions.
2. Load the trusted `CatalogRollout` policy and the persistent
   `CatalogDeviceCursor` for this audience and enrolled device. Allocate
   the cursor once. For an existing catalog, start from current published
   workflow versions and seed the signer with the highest previously
   served manifest version via `observe_published_version`. Preserve
   stored cursors through restarts and deployments.
3. Call `cursor.issue_idempotent(&policy, &signer, app_version, sdk_version, key)`.
   Keep the request key stable across a lost response or a compare-and-swap
   retry. The cursor retains the exact latest signed response and also
   returns it to concurrent requests selecting the same snapshot generation.
   Start a new key for a new generation or compatible snapshot selection.
   Commit the changed cursor under a transaction or compare-and-swap
   before returning the result. Retry from the committed cursor after a
   failed compare-and-swap. Serve `delivery.wire_bytes()` with
   `Cache-Control: private, no-store`.
4. Pass the returned `CatalogDeliveryReceipt` to the app through its
   authenticated integration. The app reports success when the intended
   revision is installed. Load the cursor for that authenticated device,
   then call `cursor.acknowledge(device_id, &receipt)` and commit the
   result. Duplicate acknowledgements are accepted. Superseded receipts
   and receipts issued to other devices are rejected.

The cohort hashes the enrolled device, catalog and cohort key to a bucket
out of 10,000. Keep the cohort key fixed and raise the basis-point
threshold to expand a canary while retaining its earlier devices. Each
policy change needs a newer generation. Delivery and acknowledgement
records describe app-reported progress. Eligibility is distribution policy
in the authenticated backend. Catalog signature verification and audience
checks remain in the scoped SDK.

## Roll back behavior

Use a known compatible, complete snapshot of historical sequence
definitions. Under the host's publication lock, reserve revisions above
the highest version ever published for each name. Pass those version
floors and the historical definitions to `publish_catalog_rollback`.
The helper uploads signed assets with fresh IDs and higher versions. It
updates a pinned child reference to the new revision of the same child
from the historical snapshot. Include all referenced child definitions.

Publish a newer rollout policy generation using the returned entries as
the stable snapshot and disable the canary. Use the known compatible app
and SDK minima for the restored behavior. Each enrolled device receives a
new signed manifest version. Devices already given the canary are protected
against receiving an older workflow version, including when their app
version changes or a workflow name was withdrawn.

The host backend owns policy, device enrollment, cursor persistence and
acknowledgement transport. The helper does not mount HTTP routes or change
the mobile SDK's wire format.
