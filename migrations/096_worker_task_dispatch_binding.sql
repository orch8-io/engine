-- Retry attempts become claimable only once their effect id is bound.
--
-- awaiting_dispatch  TRUE on a retry row pre-inserted by a failure / lease /
--                    timeout resolution. The re-dispatch of that attempt
--                    creates its effect receipt and binds `effect_id` in the
--                    same upsert that clears this flag, so no worker can claim
--                    the attempt before its effect id exists (settlement then
--                    always uses the stored id and never recomputes it).
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS awaiting_dispatch BOOLEAN
    NOT NULL DEFAULT FALSE;
