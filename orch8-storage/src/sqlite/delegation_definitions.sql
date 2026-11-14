CREATE TABLE IF NOT EXISTS delegation_definitions (
 tenant_id TEXT NOT NULL,
 sequence_sha256 TEXT NOT NULL CHECK(length(sequence_sha256)=64 AND sequence_sha256 NOT GLOB '*[^0-9a-f]*'),
 sequence_id TEXT NOT NULL,
 version INTEGER NOT NULL CHECK(version=1),
 definition TEXT NOT NULL CHECK(length(CAST(definition AS BLOB))<=4194304),
 PRIMARY KEY(tenant_id,sequence_sha256)
);
CREATE TRIGGER IF NOT EXISTS delegation_definition_update BEFORE UPDATE ON delegation_definitions
 BEGIN SELECT RAISE(ABORT,'frozen delegation definitions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS delegation_definition_delete BEFORE DELETE ON delegation_definitions
 BEGIN SELECT RAISE(ABORT,'frozen delegation definitions are immutable'); END;
CREATE TABLE IF NOT EXISTS delegation_definition_manifests (
 tenant_id TEXT NOT NULL,
 task_id TEXT NOT NULL,
 bundle_sha256 TEXT NOT NULL CHECK(length(bundle_sha256)=64 AND bundle_sha256 NOT GLOB '*[^0-9a-f]*'),
 manifest TEXT NOT NULL CHECK(length(CAST(manifest AS BLOB))<=4194304),
 wire_version INTEGER NOT NULL CHECK(wire_version=1),
 wire_sha256 TEXT NOT NULL CHECK(length(wire_sha256)=64 AND wire_sha256 NOT GLOB '*[^0-9a-f]*'),
 wire_bytes INTEGER NOT NULL CHECK(wire_bytes>0 AND wire_bytes<=4194304),
 wire_definitions INTEGER NOT NULL CHECK(wire_definitions>0 AND wire_definitions<=256),
 wire_selectors INTEGER NOT NULL CHECK(wire_selectors>=0 AND wire_selectors<=1024),
 PRIMARY KEY(tenant_id,task_id)
);
CREATE TRIGGER IF NOT EXISTS delegation_definition_manifest_update BEFORE UPDATE ON delegation_definition_manifests
 BEGIN SELECT RAISE(ABORT,'frozen delegation manifests are immutable'); END;
CREATE TRIGGER IF NOT EXISTS delegation_definition_manifest_delete BEFORE DELETE ON delegation_definition_manifests
 BEGIN SELECT RAISE(ABORT,'frozen delegation manifests are immutable'); END;

-- Local downloaded graph cache. Installation alone never authorizes execution.
CREATE TABLE IF NOT EXISTS mobile_claim_definition_receipts (
 id TEXT PRIMARY KEY,
 account_id TEXT NOT NULL,
 catalog_id TEXT NOT NULL,
 owner_epoch INTEGER NOT NULL CHECK(owner_epoch>0),
 server TEXT NOT NULL,
 remote_tenant TEXT NOT NULL,
 task_id TEXT NOT NULL,
 worker_id TEXT NOT NULL,
 claim_epoch TEXT NOT NULL CHECK(length(claim_epoch) BETWEEN 1 AND 20 AND claim_epoch NOT GLOB '*[^0-9]*' AND substr(claim_epoch,1,1)<>'0'),
 reference_json TEXT NOT NULL,
 namespace TEXT NOT NULL UNIQUE,
 root_id TEXT NOT NULL REFERENCES sequences(id),
 definition_bytes INTEGER NOT NULL CHECK(definition_bytes>0 AND definition_bytes<=8388608),
 UNIQUE(account_id,catalog_id,owner_epoch,server,remote_tenant,task_id,worker_id,claim_epoch),
 FOREIGN KEY(account_id,catalog_id) REFERENCES mobile_catalogs(account_id,id)
);
CREATE TABLE IF NOT EXISTS mobile_claim_definition_entries (
 receipt_id TEXT NOT NULL REFERENCES mobile_claim_definition_receipts(id),
 source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64),
 sequence_id TEXT NOT NULL UNIQUE REFERENCES sequences(id),
 local_sha256 TEXT NOT NULL CHECK(length(local_sha256)=64),
 PRIMARY KEY(receipt_id,source_sha256)
);
CREATE INDEX IF NOT EXISTS idx_mobile_claim_definition_usage
 ON mobile_claim_definition_receipts(account_id,catalog_id,definition_bytes);

-- Retain the authenticated manifest so restart can validate cached source maps
-- without downloading the exact-byte envelope again.
CREATE TABLE IF NOT EXISTS mobile_claim_definition_manifests (
 receipt_id TEXT PRIMARY KEY REFERENCES mobile_claim_definition_receipts(id),
 manifest TEXT NOT NULL CHECK(length(CAST(manifest AS BLOB))<=4194304)
);
-- Stable logical identity deliberately excludes both local and remote epochs.
-- No task_instances row is created; this is not scheduler admission.
CREATE TABLE IF NOT EXISTS mobile_remote_run_bindings (
 local_run_id TEXT PRIMARY KEY,
 account_id TEXT NOT NULL,
 catalog_id TEXT NOT NULL,
 server TEXT NOT NULL,
 remote_tenant TEXT NOT NULL,
 task_id TEXT NOT NULL,
 owner_epoch INTEGER NOT NULL CHECK(owner_epoch>0),
 worker_id TEXT NOT NULL,
 claim_epoch TEXT NOT NULL CHECK(length(claim_epoch) BETWEEN 1 AND 20 AND claim_epoch NOT GLOB '*[^0-9]*' AND substr(claim_epoch,1,1)<>'0'),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64 AND request_sha256 NOT GLOB '*[^0-9a-f]*'),
 initial_receipt_id TEXT NOT NULL UNIQUE REFERENCES mobile_claim_definition_receipts(id),
 current_receipt_id TEXT NOT NULL REFERENCES mobile_claim_definition_receipts(id),
 UNIQUE(account_id,catalog_id,server,remote_tenant,task_id),
 FOREIGN KEY(account_id,catalog_id) REFERENCES mobile_catalogs(account_id,id)
);

-- Explicit native cache fingerprint codec. Missing/unknown codecs are not
-- inferred by reserializing mutable rows or changing their stored fingerprints.
CREATE TABLE IF NOT EXISTS mobile_claim_definition_codecs (
 receipt_id TEXT PRIMARY KEY REFERENCES mobile_claim_definition_receipts(id),
 version INTEGER NOT NULL CHECK(version>0)
);

-- One bounded latest observation per logical run; graph receipts remain immutable.
-- Claim observation is not permission to execute or a substitute for outcome history.
CREATE TABLE IF NOT EXISTS mobile_remote_run_observations (
 local_run_id TEXT PRIMARY KEY REFERENCES mobile_remote_run_bindings(local_run_id),
 record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=16384)
);

-- Measured logical resource use and exact input bytes, separate from the v1 receipt.
-- Missing historical rows are not reconstructed from mutable mailbox/catalog data.
CREATE TABLE IF NOT EXISTS delegation_publication_inputs (
 tenant_id TEXT NOT NULL,
 task_id TEXT NOT NULL,
 version INTEGER NOT NULL CHECK(version=1),
 request_sha256 TEXT NOT NULL CHECK(length(request_sha256)=64 AND request_sha256 NOT GLOB '*[^0-9a-f]*'),
 definition_expanded_bytes INTEGER NOT NULL CHECK(definition_expanded_bytes>0 AND definition_expanded_bytes<=16777216),
 input_wire_version INTEGER NOT NULL CHECK(input_wire_version=1),
 input_sha256 TEXT NOT NULL CHECK(length(input_sha256)=64 AND input_sha256 NOT GLOB '*[^0-9a-f]*'),
 input_bytes INTEGER NOT NULL CHECK(input_bytes>0 AND input_bytes<=4194304),
 input_expanded_bytes INTEGER NOT NULL CHECK(input_expanded_bytes>0 AND input_expanded_bytes<=16777216),
 stored_input TEXT NOT NULL CHECK(length(CAST(stored_input AS BLOB))<=8388608),
 PRIMARY KEY(tenant_id,task_id),
 FOREIGN KEY(tenant_id,task_id) REFERENCES delegation_definition_manifests(tenant_id,task_id)
);
CREATE TRIGGER IF NOT EXISTS delegation_publication_input_update BEFORE UPDATE ON delegation_publication_inputs
 BEGIN SELECT RAISE(ABORT,'frozen delegation inputs are immutable'); END;
CREATE TRIGGER IF NOT EXISTS delegation_publication_input_delete BEFORE DELETE ON delegation_publication_inputs
 BEGIN SELECT RAISE(ABORT,'frozen delegation inputs are immutable'); END;

-- Compact references are immutable per issued generation, independent of hot work.
CREATE TABLE IF NOT EXISTS worker_issued_delegation_contexts (
 task_id TEXT NOT NULL,
 claim_epoch BIGINT NOT NULL CHECK(claim_epoch>0),
 tenant_id TEXT NOT NULL,
 worker_id TEXT NOT NULL,
 record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=16384),
 PRIMARY KEY(task_id,claim_epoch),
 FOREIGN KEY(task_id,claim_epoch) REFERENCES worker_issued_claims(task_id,claim_epoch)
);
CREATE TRIGGER IF NOT EXISTS worker_issued_delegation_context_update BEFORE UPDATE ON worker_issued_delegation_contexts
 BEGIN SELECT RAISE(ABORT,'issued delegation references are immutable'); END;
CREATE TRIGGER IF NOT EXISTS worker_issued_delegation_context_delete BEFORE DELETE ON worker_issued_delegation_contexts
 BEGIN SELECT RAISE(ABORT,'issued delegation references are immutable'); END;


-- Explicit host-selected worker ownership, never inferred from remote task data.
-- Older SDKs initialize this identity table separately; preserve its exact shape.
CREATE TABLE IF NOT EXISTS mobile_node_identity (
 key TEXT PRIMARY KEY,
 value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mobile_scoped_worker_peers (
 account_id TEXT NOT NULL,
 catalog_id TEXT NOT NULL,
 peer_key TEXT NOT NULL,
 api_base TEXT NOT NULL,
 remote_tenant TEXT NOT NULL,
 runtime_id TEXT NOT NULL,
 record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=8192),
 PRIMARY KEY(account_id,catalog_id,peer_key),
 UNIQUE(api_base,remote_tenant,runtime_id),
 FOREIGN KEY(account_id,catalog_id) REFERENCES mobile_catalogs(account_id,id)
);
CREATE TRIGGER IF NOT EXISTS mobile_worker_peer_update BEFORE UPDATE ON mobile_scoped_worker_peers
 BEGIN SELECT RAISE(ABORT,'scoped worker peer ownership is immutable'); END;
CREATE TRIGGER IF NOT EXISTS mobile_worker_peer_delete BEFORE DELETE ON mobile_scoped_worker_peers
 BEGIN SELECT RAISE(ABORT,'scoped worker peer ownership is immutable'); END;
CREATE TRIGGER IF NOT EXISTS mobile_worker_peer_global_identity BEFORE INSERT ON mobile_scoped_worker_peers
 WHEN EXISTS(SELECT 1 FROM mobile_node_identity WHERE key='runtime_id' AND lower(value)=NEW.runtime_id)
 BEGIN SELECT RAISE(ABORT,'scoped worker cannot adopt legacy node identity'); END;
CREATE TRIGGER IF NOT EXISTS mobile_worker_peer_legacy_identity_insert BEFORE INSERT ON mobile_node_identity
 WHEN NEW.key='runtime_id' AND EXISTS(SELECT 1 FROM mobile_scoped_worker_peers WHERE runtime_id=lower(NEW.value))
 BEGIN SELECT RAISE(ABORT,'legacy node cannot adopt scoped worker identity'); END;
CREATE TRIGGER IF NOT EXISTS mobile_worker_peer_legacy_identity_update BEFORE UPDATE ON mobile_node_identity
 WHEN NEW.key='runtime_id' AND EXISTS(SELECT 1 FROM mobile_scoped_worker_peers WHERE runtime_id=lower(NEW.value))
 BEGIN SELECT RAISE(ABORT,'legacy node cannot adopt scoped worker identity'); END;

CREATE TABLE IF NOT EXISTS mobile_scoped_worker_devices (
 account_id TEXT NOT NULL,
 catalog_id TEXT NOT NULL,
 peer_key TEXT NOT NULL,
 device_id TEXT NOT NULL CHECK(length(CAST(device_id AS BLOB)) BETWEEN 1 AND 256),
 PRIMARY KEY(account_id,catalog_id,peer_key),
 FOREIGN KEY(account_id,catalog_id,peer_key) REFERENCES mobile_scoped_worker_peers(account_id,catalog_id,peer_key)
);
CREATE TRIGGER IF NOT EXISTS mobile_worker_device_update BEFORE UPDATE ON mobile_scoped_worker_devices
 BEGIN SELECT RAISE(ABORT,'scoped worker device is immutable'); END;
CREATE TRIGGER IF NOT EXISTS mobile_worker_device_delete BEFORE DELETE ON mobile_scoped_worker_devices
 BEGIN SELECT RAISE(ABORT,'scoped worker device is immutable'); END;

-- One exact input per logical run, independent of claim and sign-in generations.
CREATE TABLE IF NOT EXISTS mobile_remote_run_inputs (
 local_run_id TEXT PRIMARY KEY REFERENCES mobile_remote_run_bindings(local_run_id),
 version INTEGER NOT NULL CHECK(version>0),
 reference_json TEXT NOT NULL CHECK(length(CAST(reference_json AS BLOB))<=1024),
 payload_base64 TEXT NOT NULL CHECK(length(CAST(payload_base64 AS BLOB)) BETWEEN 4 AND 5592408),
 expanded_bytes INTEGER NOT NULL CHECK(expanded_bytes BETWEEN 1 AND 16777216)
);
CREATE TRIGGER IF NOT EXISTS mobile_remote_input_update BEFORE UPDATE ON mobile_remote_run_inputs
 BEGIN SELECT RAISE(ABORT,'remote logical input is immutable'); END;
CREATE TRIGGER IF NOT EXISTS mobile_remote_input_delete BEFORE DELETE ON mobile_remote_run_inputs
 BEGIN SELECT RAISE(ABORT,'remote logical input requires acknowledged archive'); END;

-- Remote native roots cannot be adopted by legacy scheduling or mobile views.
-- The immutable receipt survives mutable instance identity/state changes.
CREATE TABLE IF NOT EXISTS mobile_remote_run_materializations (
 instance_id TEXT PRIMARY KEY REFERENCES mobile_remote_run_bindings(local_run_id),
 record TEXT NOT NULL CHECK(length(CAST(record AS BLOB))<=16384),
 FOREIGN KEY(instance_id) REFERENCES task_instances(id)
);
CREATE TRIGGER IF NOT EXISTS mobile_remote_materialization_update BEFORE UPDATE ON mobile_remote_run_materializations
 BEGIN SELECT RAISE(ABORT,'remote materialization ownership is immutable'); END;
CREATE TRIGGER IF NOT EXISTS mobile_remote_materialization_delete BEFORE DELETE ON mobile_remote_run_materializations
 BEGIN SELECT RAISE(ABORT,'remote materialization requires acknowledged archive'); END;

-- One physical root per configured backend/task, across account/catalog handoff.
CREATE TABLE IF NOT EXISTS mobile_remote_physical_roots (
 api_base TEXT NOT NULL,
 task_id TEXT NOT NULL,
 remote_tenant TEXT NOT NULL,
 instance_id TEXT NOT NULL UNIQUE REFERENCES mobile_remote_run_materializations(instance_id),
 PRIMARY KEY(api_base,task_id)
);
CREATE TRIGGER IF NOT EXISTS mobile_remote_physical_update BEFORE UPDATE ON mobile_remote_physical_roots
 BEGIN SELECT RAISE(ABORT,'remote physical ownership is immutable'); END;
CREATE TRIGGER IF NOT EXISTS mobile_remote_physical_delete BEFORE DELETE ON mobile_remote_physical_roots
 BEGIN SELECT RAISE(ABORT,'remote physical ownership requires acknowledged archive'); END;
