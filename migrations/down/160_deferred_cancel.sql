-- Remove durable deferred-cancellation intent from the signal inbox.
DROP INDEX signal_inbox_deferred_cancel;
ALTER TABLE signal_inbox DROP COLUMN deferred_cancel;
