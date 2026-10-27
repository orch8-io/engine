-- A node id survives loop resets; timestamps also survive. The activation token
-- changes when a node is reset for a new invocation, never on activation/resume.
ALTER TABLE execution_tree ADD COLUMN activation_id UUID NOT NULL DEFAULT gen_random_uuid();
CREATE FUNCTION rotate_execution_activation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.state<>'pending' AND NEW.state='pending' THEN
  NEW.activation_id := gen_random_uuid();
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER execution_activation BEFORE UPDATE OF state ON execution_tree
 FOR EACH ROW EXECUTE FUNCTION rotate_execution_activation();

-- No native/task FK: dispatch evidence must survive their independent hot GC.
CREATE TABLE worker_task_invocations (
 task_id UUID PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 instance_id UUID NOT NULL,
 record JSONB NOT NULL
);
CREATE INDEX worker_invocation_native_node ON execution_tree(instance_id,block_id) WHERE state IN ('pending','running','waiting');
CREATE INDEX worker_task_invocations_instance ON worker_task_invocations(tenant_id,instance_id);
CREATE FUNCTION preserve_worker_invocation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'worker invocation bindings are immutable'; END $$;
CREATE TRIGGER worker_invocation_update BEFORE UPDATE ON worker_task_invocations
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_invocation();
CREATE TRIGGER worker_invocation_delete BEFORE DELETE ON worker_task_invocations
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_invocation();
