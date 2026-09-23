-- Rolling back requires all stored values to fit the original INTEGER column.
ALTER TABLE task_instances
    ALTER COLUMN max_concurrency TYPE INTEGER;
