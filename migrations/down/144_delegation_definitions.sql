-- Remove the frozen-definition reader only when no retained body depends on it.
LOCK TABLE worker_issued_delegation_contexts, delegation_definitions, delegation_definition_manifests, delegation_publication_inputs IN ACCESS EXCLUSIVE MODE NOWAIT;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM worker_issued_delegation_contexts) OR current_setting('transaction_isolation')<>'read committed' OR EXISTS(SELECT 1 FROM delegation_definitions) OR EXISTS(SELECT 1 FROM delegation_definition_manifests) OR EXISTS(SELECT 1 FROM delegation_publication_inputs) THEN
  RAISE EXCEPTION 'retained frozen delegation definitions require the current reader';
 END IF;
END $$;
DROP TRIGGER delegation_definition_immutable ON delegation_definitions;
DROP TRIGGER delegation_definition_manifest_immutable ON delegation_definition_manifests;
DROP TRIGGER delegation_publication_input_immutable ON delegation_publication_inputs;
DROP TABLE delegation_publication_inputs;
DROP TRIGGER worker_issued_delegation_context_immutable ON worker_issued_delegation_contexts;
DROP TABLE worker_issued_delegation_contexts;
DROP FUNCTION protect_delegation_definition();
DROP TABLE delegation_definition_manifests;
DROP TABLE delegation_definitions;
