-- Runtime state for polling triggers (trigger_type = 'activepieces_poll').
--
-- One row per polling trigger registration:
--   state                — opaque dedupe cursor blob returned by the
--                          ActivePieces sidecar's last successful poll
--                          (store contents: lastPoll epoch, item ids, ...),
--                          sent back verbatim on the next poll.
--   last_poll_at         — completion time of the most recent poll attempt.
--   last_error           — message from the most recent failed poll
--                          (NULL after a successful poll).
--   consecutive_failures — failed polls in a row; reset to 0 on success.
--
-- Kept separate from `triggers` so the poll loop can persist its cursor
-- without rewriting the user-authored trigger definition (avoids
-- lost-update races with concurrent API edits).
CREATE TABLE IF NOT EXISTS trigger_poll_state (
    slug                 TEXT PRIMARY KEY REFERENCES triggers(slug) ON DELETE CASCADE,
    state                TEXT NOT NULL DEFAULT 'null',
    last_poll_at         TIMESTAMPTZ,
    last_error           TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
