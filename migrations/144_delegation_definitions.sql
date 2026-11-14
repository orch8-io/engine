-- No historical body is inferred from a mutable catalog entry.
CREATE TABLE delegation_definitions (
 tenant_id TEXT NOT NULL,
 sequence_sha256 TEXT NOT NULL CHECK(sequence_sha256 ~ '^[0-9a-f]{64}$'),
 sequence_id UUID NOT NULL,
 version INTEGER NOT NULL CHECK(version=1),
 definition TEXT NOT NULL CHECK(octet_length(definition)<=4194304),
 PRIMARY KEY(tenant_id,sequence_sha256)
);
CREATE FUNCTION protect_delegation_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'frozen delegation definitions are immutable'; END $$;
CREATE TRIGGER delegation_definition_immutable BEFORE UPDATE OR DELETE ON delegation_definitions
 FOR EACH ROW EXECUTE FUNCTION protect_delegation_definition();
-- Separate from the root hash: latest child versions may differ per publication.
CREATE TABLE delegation_definition_manifests (
 tenant_id TEXT NOT NULL,
 task_id UUID NOT NULL,
 bundle_sha256 TEXT NOT NULL CHECK(bundle_sha256 ~ '^[0-9a-f]{64}$'),
 manifest TEXT NOT NULL CHECK(octet_length(manifest)<=4194304),
 wire_version INTEGER NOT NULL CHECK(wire_version=1),
 wire_sha256 TEXT NOT NULL CHECK(wire_sha256 ~ '^[0-9a-f]{64}$'),
 wire_bytes INTEGER NOT NULL CHECK(wire_bytes>0 AND wire_bytes<=4194304),
 wire_definitions INTEGER NOT NULL CHECK(wire_definitions>0 AND wire_definitions<=256),
 wire_selectors INTEGER NOT NULL CHECK(wire_selectors>=0 AND wire_selectors<=1024),
 PRIMARY KEY(tenant_id,task_id)
);
CREATE TRIGGER delegation_definition_manifest_immutable BEFORE UPDATE OR DELETE ON delegation_definition_manifests
 FOR EACH ROW EXECUTE FUNCTION protect_delegation_definition();

-- Measured logical resource use and exact input bytes, separate from the v1 receipt.
-- Missing historical rows are not reconstructed from mutable mailbox/catalog data.
CREATE TABLE delegation_publication_inputs (
 tenant_id TEXT NOT NULL,
 task_id UUID NOT NULL,
 version INTEGER NOT NULL CHECK(version=1),
 request_sha256 TEXT NOT NULL CHECK(request_sha256 ~ '^[0-9a-f]{64}$'),
 definition_expanded_bytes INTEGER NOT NULL CHECK(definition_expanded_bytes>0 AND definition_expanded_bytes<=16777216),
 input_wire_version INTEGER NOT NULL CHECK(input_wire_version=1),
 input_sha256 TEXT NOT NULL CHECK(input_sha256 ~ '^[0-9a-f]{64}$'),
 input_bytes INTEGER NOT NULL CHECK(input_bytes>0 AND input_bytes<=4194304),
 input_expanded_bytes INTEGER NOT NULL CHECK(input_expanded_bytes>0 AND input_expanded_bytes<=16777216),
 stored_input TEXT NOT NULL CHECK(octet_length(stored_input)<=8388608),
 PRIMARY KEY(tenant_id,task_id),
 FOREIGN KEY(tenant_id,task_id) REFERENCES delegation_definition_manifests(tenant_id,task_id)
);
CREATE TRIGGER delegation_publication_input_immutable BEFORE UPDATE OR DELETE ON delegation_publication_inputs
 FOR EACH ROW EXECUTE FUNCTION protect_delegation_definition();

-- Compact references are immutable per issued generation, independent of hot work.
CREATE TABLE worker_issued_delegation_contexts (
 task_id UUID NOT NULL,
 claim_epoch BIGINT NOT NULL CHECK(claim_epoch>0),
 tenant_id TEXT NOT NULL,
 worker_id TEXT NOT NULL,
 record TEXT NOT NULL CHECK(octet_length(record)<=16384),
 PRIMARY KEY(task_id,claim_epoch),
 FOREIGN KEY(task_id,claim_epoch) REFERENCES worker_issued_claims(task_id,claim_epoch)
);
CREATE TRIGGER worker_issued_delegation_context_immutable BEFORE UPDATE OR DELETE ON worker_issued_delegation_contexts
 FOR EACH ROW EXECUTE FUNCTION protect_delegation_definition();
