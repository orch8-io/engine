-- Rollback 044_trigger_poll_state.sql
--
-- Undoes: CREATE TABLE trigger_poll_state.
--
-- WARNING: all polling-trigger cursors are permanently deleted — after a
-- re-migration every activepieces_poll trigger starts from a fresh cursor,
-- which may re-deliver items the piece had already reported.

DROP TABLE IF EXISTS trigger_poll_state;
