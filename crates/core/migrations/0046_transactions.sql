-- N01 Transact v2: the approval ledger behind live (irreversible) browser
-- submissions.
--
-- Inert until `[transact] allow_live = true`: with the switch off the dry-run
-- path still writes `pending` rows (so an operator can see what WOULD be
-- queued for approval), but `POST /transactions/{id}/approve` answers 409 and
-- nothing can ever reach the `submitted` state.
--
-- `idempotency_key` is UNIQUE, and that uniqueness IS the double-submit lock:
-- one key can only ever own one ledger row, and only a `pending` row can be
-- approved, so a second approve on a key that already submitted matches
-- nothing.

CREATE TABLE IF NOT EXISTS transactions (
    id              TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    app             TEXT NOT NULL,
    -- The dry-run (staging) job that produced the evidence bundle. NULL only
    -- for a row whose job row was pruned.
    job_id          TEXT,
    -- The session-vault identity the flow ran as, and the one the live submit
    -- will run as. NULL = profile-less (the shared default Chrome).
    profile         TEXT,
    -- pending | approved | submitted | rejected | expired
    state           TEXT NOT NULL,
    -- SHA-256 over the canonical reviewed surface (submit target + filled
    -- fields). The approve door quotes it and the commit re-probes it; a drift
    -- between review and submit is a refusal, never a click.
    evidence_sha    TEXT NOT NULL,
    -- Principal id (N20) that approved. NULL in `open` mode: the synthetic
    -- operator is not a `principals.id`, and inventing one would make an
    -- unattributed approval look attributed.
    approved_by     TEXT,
    approved_at     TEXT,
    submitted_at    TEXT,
    -- Artifact path of the post-submit evidence bundle, relative to the
    -- committing job's artifact dir.
    receipt_path    TEXT,
    -- Approval deadline stamped at `pending` from `[transact] approval_ttl_secs`.
    -- NULL = never expires.
    expires_at      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_transactions_state ON transactions (state, created_at);
-- The per-profile daily cap counts submissions in a window for one identity.
CREATE INDEX IF NOT EXISTS idx_transactions_profile ON transactions (profile, submitted_at);
