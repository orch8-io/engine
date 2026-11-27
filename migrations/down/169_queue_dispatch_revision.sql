-- Remove the queue dispatch revision and the trigger and function that maintain it.
DROP TRIGGER queue_dispatch_refresh_revision ON queue_dispatch;
DROP FUNCTION refresh_queue_dispatch_revision();
ALTER TABLE queue_dispatch DROP COLUMN revision;
