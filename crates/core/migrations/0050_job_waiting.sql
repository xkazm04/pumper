-- N02 "jobs that wait": a `waiting` lifecycle state for human/agent-in-the-loop
-- work. A running app calls `ctx.await_input(resume_state, request)`, which
-- forces a checkpoint and returns `Error::AwaitingInput`; the worker parks the
-- row here instead of failing it, releases its permit, and `POST
-- /jobs/{id}/resume {input}` re-queues it with attempt headroom.
--
--   input_request       what the app asked the outside world for (JSON). Read by
--                       `GET /jobs/{id}`, the `waiting` SSE event and MCP
--                       `wait_job` — this is the half of the loop a job had no
--                       way to say before.
--   waiting_since       when the park landed (how long has this been pending?).
--   waiting_expires_at  NULL = wait forever. Past this instant the expiry sweep
--                       on the reaper tick fails the job through `finalize`, so
--                       callbacks and terminal triggers fire like any other
--                       permanent failure. Set from `[waiting] expiry_secs`
--                       (default 0 = no deadline, i.e. today's behaviour).
--   resumed_input       the input the resume door stored, handed to the next
--                       attempt as `ctx.restore_input()`. Never serialized on
--                       the job row: the motivating payloads are approvals and
--                       2FA codes, so it is treated like `callback_secret`.
--
-- Additive: every column is nullable, so existing rows and every query that
-- does not mention them are unchanged.
ALTER TABLE jobs ADD COLUMN input_request TEXT;
ALTER TABLE jobs ADD COLUMN waiting_since TEXT;
ALTER TABLE jobs ADD COLUMN waiting_expires_at TEXT;
ALTER TABLE jobs ADD COLUMN resumed_input TEXT;

-- The expiry sweep's only query: waiting rows with a deadline, oldest first.
CREATE INDEX IF NOT EXISTS idx_jobs_waiting_expiry
    ON jobs (waiting_expires_at)
    WHERE status = 'waiting' AND waiting_expires_at IS NOT NULL;
