-- The Rust model uses u32; INTEGER silently wrapped values above i32::MAX.
ALTER TABLE task_instances
    ALTER COLUMN max_concurrency TYPE BIGINT;
