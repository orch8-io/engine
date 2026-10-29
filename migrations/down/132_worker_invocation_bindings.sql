-- Remove activation tokens and dispatch bindings only before any retained invocation evidence exists.
LOCK TABLE execution_tree,worker_tasks,worker_task_invocations IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM worker_task_invocations LIMIT 1)
 OR EXISTS(SELECT 1 FROM worker_tasks WHERE state IN ('pending','claimed') LIMIT 1) THEN
  RAISE EXCEPTION 'Cannot remove worker invocation evidence while tasks or bindings remain';
 END IF;
END $$;
DROP INDEX worker_invocation_native_node;
DROP TABLE worker_task_invocations;
DROP FUNCTION preserve_worker_invocation();
DROP TRIGGER execution_activation ON execution_tree;
DROP FUNCTION rotate_execution_activation();
ALTER TABLE execution_tree DROP COLUMN activation_id;
