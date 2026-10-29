CREATE TRIGGER IF NOT EXISTS execution_activation_insert AFTER INSERT ON execution_tree
 WHEN NEW.activation_id IS NULL
 BEGIN UPDATE execution_tree SET activation_id=lower(hex(randomblob(16))) WHERE id=NEW.id; END;
CREATE TRIGGER IF NOT EXISTS execution_activation AFTER UPDATE OF state ON execution_tree
 WHEN OLD.state<>'pending' AND NEW.state='pending'
 BEGIN UPDATE execution_tree SET activation_id=lower(hex(randomblob(16))) WHERE id=NEW.id; END;
CREATE TRIGGER IF NOT EXISTS execution_activation_not_null BEFORE UPDATE OF activation_id ON execution_tree
 WHEN NEW.activation_id IS NULL AND OLD.activation_id IS NOT NULL
 BEGIN SELECT RAISE(ABORT,'execution activation cannot be cleared'); END;
CREATE TRIGGER IF NOT EXISTS worker_invocation_update BEFORE UPDATE ON worker_task_invocations
 BEGIN SELECT RAISE(ABORT,'worker invocation bindings are immutable'); END;
CREATE TRIGGER IF NOT EXISTS worker_invocation_delete BEFORE DELETE ON worker_task_invocations
 BEGIN SELECT RAISE(ABORT,'worker invocation bindings are immutable'); END;
