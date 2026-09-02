//! The transactions ledger (N01, Transact v2): the approval gate between a
//! dry-run browser flow and the one irreversible click it prepared.
//!
//! A `transact` job with `submit: true` no longer refuses at the door. It runs
//! the flow **dry-run** exactly as v1 did, writes a `pending` ledger row keyed
//! on the caller's `idempotency_key`, and then **parks** on N02's `waiting`
//! state with the evidence bundle as its `input_request`. A human (or an agent
//! with `admin` scope) reads the bundle, quotes its digest to
//! `POST /transactions/{id}/approve`, and the resume re-drives the flow and
//! performs the recorded `submit_action` — once, ever.
//!
//! ## Why the state machine is a pure function
//!
//! Every refusal this file owes a caller is a decision over values that cannot
//! change between two attempts: the row's state, its deadline, the digest that
//! was reviewed, the operator's switch. [`approve_decision`] and
//! [`commit_guard`] are therefore total, side-effect-free functions with the
//! anti-patterns they defend named in their tests — the door and the engine
//! consult the same function, so the two surfaces cannot disagree about what a
//! stale approval does.
//!
//! ## What makes double submission impossible
//!
//! `transactions.idempotency_key` is UNIQUE (migration `0046`), so one key owns
//! at most one row for the life of the store. Only a `pending` row is
//! approvable, and the approve write is guarded on `state = 'pending'` in SQL —
//! so a second approve (a stale tab, an agent retry, a second approver) matches
//! no row and is refused, rather than minting a second live lineage for one
//! irreversible action.

// The digest and the commit guard live beside the evidence types they are
// computed over (`crate::engine`), so an engine can compute and check them
// without the `storage` feature this ledger needs. Re-exported here because
// this module is where the approval lifecycle is documented.
pub use crate::engine::{commit_guard, evidence_digest, CommitRefusal};
use crate::{Error, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use sqlx::SqlitePool;

// ── state ────────────────────────────────────────────────────────────────────

/// Where one ledger row sits in its lifecycle.
///
/// `submitted`, `rejected` and `expired` are terminal: nothing moves a row out
/// of them. That is the property the double-submit guard rests on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionState {
    /// Evidence captured, awaiting a decision. The only approvable state.
    Pending,
    /// Approved; the parked job has been resumed and is committing.
    Approved,
    /// The irreversible action ran and a post-submit receipt exists. Terminal.
    Submitted,
    /// A human said no. Terminal.
    Rejected,
    /// The approval deadline passed unanswered. Terminal.
    Expired,
}

impl TransactionState {
    pub fn as_str(self) -> &'static str {
        match self {
            TransactionState::Pending => "pending",
            TransactionState::Approved => "approved",
            TransactionState::Submitted => "submitted",
            TransactionState::Rejected => "rejected",
            TransactionState::Expired => "expired",
        }
    }

    /// Parses a stored state. An unknown string is an error, never a silent
    /// downgrade to `pending`: a row this build cannot understand must not
    /// become approvable by accident.
    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "pending" => TransactionState::Pending,
            "approved" => TransactionState::Approved,
            "submitted" => TransactionState::Submitted,
            "rejected" => TransactionState::Rejected,
            "expired" => TransactionState::Expired,
            other => {
                return Err(Error::Parse(format!(
                    "unknown transaction state '{other}' (known: pending, approved, submitted, \
                     rejected, expired)"
                )))
            }
        })
    }

    /// Terminal states never transition again.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TransactionState::Submitted | TransactionState::Rejected | TransactionState::Expired
        )
    }
}

/// One row of the ledger.
#[derive(Debug, Clone, Serialize)]
pub struct Transaction {
    pub id: String,
    pub idempotency_key: String,
    pub app: String,
    pub job_id: Option<String>,
    pub profile: Option<String>,
    pub state: TransactionState,
    /// SHA-256 over the reviewed surface — see [`evidence_digest`].
    pub evidence_sha: String,
    pub approved_by: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub receipt_path: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ── the reviewed surface ─────────────────────────────────────────────────────

// ── the state machine, as pure functions ─────────────────────────────────────

/// Why an approval was refused. Each variant is a fact about the request that
/// no retry could discover differently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApprovalRefusal {
    /// `[transact] allow_live = false`: this node cannot submit anything.
    LiveDisabled,
    /// The row is not `pending` — including the case that matters most, a row
    /// that already `submitted`.
    NotPending(TransactionState),
    /// The approval deadline passed.
    Expired,
    /// The caller quoted a digest that is not the one on the row: they are
    /// approving evidence this row does not hold.
    EvidenceMismatch { on_row: String, quoted: String },
    /// This profile already submitted its allowance for the day.
    DailyCapReached { cap: i64, used: i64 },
}

impl ApprovalRefusal {
    /// The sentence the HTTP door and the MCP tool both answer with.
    pub fn message(&self) -> String {
        match self {
            ApprovalRefusal::LiveDisabled => "live submission is disabled on this node: set \
                 [transact] allow_live = true to let an approval reach a real page. The ledger \
                 row stays pending and nothing was submitted."
                .to_string(),
            ApprovalRefusal::NotPending(state) => format!(
                "transaction is '{}', not 'pending': only a pending transaction can be approved. \
                 A '{}' row is terminal, so this approval changed nothing — which is exactly \
                 what stops a second approve on an already-submitted idempotency key from \
                 submitting twice.",
                state.as_str(),
                state.as_str()
            ),
            ApprovalRefusal::Expired => "the approval deadline ([transact] approval_ttl_secs) \
                 passed: the evidence describes a page as it was, and approving it now would \
                 act on a page nobody reviewed. Re-run the dry run to capture fresh evidence."
                .to_string(),
            ApprovalRefusal::EvidenceMismatch { on_row, quoted } => format!(
                "evidence_sha mismatch: this transaction holds {on_row}, the approval quoted \
                 {quoted}. An approval must name the evidence it read, or it is not an approval \
                 of anything."
            ),
            ApprovalRefusal::DailyCapReached { cap, used } => format!(
                "this profile has already submitted {used} transaction(s) today, at the \
                 [transact] max_submits_per_profile_per_day cap of {cap}. Nothing was submitted."
            ),
        }
    }
}

/// Whether an approval may proceed. Total and side-effect free: the HTTP door,
/// the MCP tool and any future policy engine consult this one function.
///
/// `quoted_evidence_sha` is optional because a pure "proceed" approval from a
/// surface that just read the row is legitimate; when it IS supplied it must
/// match, which is the only way an approval can prove it read the bundle.
#[allow(clippy::too_many_arguments)]
pub fn approve_decision(
    allow_live: bool,
    state: TransactionState,
    expires_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    row_evidence_sha: &str,
    quoted_evidence_sha: Option<&str>,
    submitted_today: i64,
    daily_cap: Option<i64>,
) -> std::result::Result<(), ApprovalRefusal> {
    if !allow_live {
        return Err(ApprovalRefusal::LiveDisabled);
    }
    if state != TransactionState::Pending {
        return Err(ApprovalRefusal::NotPending(state));
    }
    if expires_at.is_some_and(|deadline| deadline <= now) {
        return Err(ApprovalRefusal::Expired);
    }
    if let Some(quoted) = quoted_evidence_sha {
        if quoted != row_evidence_sha {
            return Err(ApprovalRefusal::EvidenceMismatch {
                on_row: row_evidence_sha.to_string(),
                quoted: quoted.to_string(),
            });
        }
    }
    if let Some(cap) = daily_cap.filter(|c| *c > 0) {
        if submitted_today >= cap {
            return Err(ApprovalRefusal::DailyCapReached {
                cap,
                used: submitted_today,
            });
        }
    }
    Ok(())
}

/// Whether a rejection may proceed: only a `pending` row can be rejected, for
/// the same reason only a `pending` row can be approved.
pub fn reject_decision(state: TransactionState) -> std::result::Result<(), ApprovalRefusal> {
    if state != TransactionState::Pending {
        return Err(ApprovalRefusal::NotPending(state));
    }
    Ok(())
}

// ── store ────────────────────────────────────────────────────────────────────

/// Fixed-width RFC 3339 UTC, the same encoding every other ledger in this store
/// uses, so lexicographic SQL comparison matches chronological order.
fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|d| d.with_timezone(&Utc))
        .map_err(|e| Error::Parse(format!("bad timestamp '{s}': {e}")))
}

#[derive(sqlx::FromRow)]
struct TxRow {
    id: String,
    idempotency_key: String,
    app: String,
    job_id: Option<String>,
    profile: Option<String>,
    state: String,
    evidence_sha: String,
    approved_by: Option<String>,
    approved_at: Option<String>,
    submitted_at: Option<String>,
    receipt_path: Option<String>,
    created_at: String,
    updated_at: String,
}

impl TxRow {
    fn decode(self) -> Result<Transaction> {
        Ok(Transaction {
            id: self.id,
            idempotency_key: self.idempotency_key,
            app: self.app,
            job_id: self.job_id,
            profile: self.profile,
            state: TransactionState::parse(&self.state)?,
            evidence_sha: self.evidence_sha,
            approved_by: self.approved_by,
            approved_at: self.approved_at.as_deref().map(parse_ts).transpose()?,
            submitted_at: self.submitted_at.as_deref().map(parse_ts).transpose()?,
            receipt_path: self.receipt_path,
            created_at: parse_ts(&self.created_at)?,
            updated_at: parse_ts(&self.updated_at)?,
        })
    }
}

const TX_COLUMNS: &str = "id, idempotency_key, app, job_id, profile, state, evidence_sha, \
                          approved_by, approved_at, submitted_at, receipt_path, created_at, \
                          updated_at";

/// What a dry-run stage hands the ledger.
pub struct NewTransaction<'a> {
    pub idempotency_key: &'a str,
    pub app: &'a str,
    pub job_id: Option<&'a str>,
    pub profile: Option<&'a str>,
    pub evidence_sha: &'a str,
}

/// Upserts the `pending` row a dry run owes its idempotency key, returning the
/// row as it now stands.
///
/// **Re-staging the same key is not a new transaction.** A row that already
/// moved on (approved / submitted / rejected / expired) is returned untouched:
/// a fresh dry run cannot resurrect a key that already submitted, which is what
/// keeps the UNIQUE index a real lock rather than an advisory one. Only a
/// `pending` row is refreshed with the new evidence, because re-reviewing a
/// still-open request against newer evidence is exactly right.
pub async fn stage_pending(pool: &SqlitePool, new: NewTransaction<'_>) -> Result<Transaction> {
    let now = Utc::now();
    if let Some(existing) = by_key(pool, new.idempotency_key).await? {
        if existing.state != TransactionState::Pending {
            return Ok(existing);
        }
        sqlx::query(
            "UPDATE transactions SET job_id = ?2, profile = ?3, evidence_sha = ?4, \
             updated_at = ?5 WHERE id = ?1 AND state = 'pending'",
        )
        .bind(&existing.id)
        .bind(new.job_id)
        .bind(new.profile)
        .bind(new.evidence_sha)
        .bind(ts(now))
        .execute(pool)
        .await?;
        return get(pool, &existing.id)
            .await?
            .ok_or_else(|| Error::Parse("transaction vanished mid-stage".into()));
    }
    let id = uuid::Uuid::new_v4().to_string();
    sqlx::query(
        "INSERT INTO transactions (id, idempotency_key, app, job_id, profile, state, \
         evidence_sha, created_at, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 'pending', ?6, ?7, ?7)",
    )
    .bind(&id)
    .bind(new.idempotency_key)
    .bind(new.app)
    .bind(new.job_id)
    .bind(new.profile)
    .bind(new.evidence_sha)
    .bind(ts(now))
    .execute(pool)
    .await?;
    get(pool, &id)
        .await?
        .ok_or_else(|| Error::Parse("transaction vanished after insert".into()))
}

pub async fn get(pool: &SqlitePool, id: &str) -> Result<Option<Transaction>> {
    let row: Option<TxRow> = sqlx::query_as(&format!(
        "SELECT {TX_COLUMNS} FROM transactions WHERE id = ?1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.map(TxRow::decode).transpose()
}

pub async fn by_key(pool: &SqlitePool, key: &str) -> Result<Option<Transaction>> {
    let row: Option<TxRow> = sqlx::query_as(&format!(
        "SELECT {TX_COLUMNS} FROM transactions WHERE idempotency_key = ?1"
    ))
    .bind(key)
    .fetch_optional(pool)
    .await?;
    row.map(TxRow::decode).transpose()
}

/// The ledger, newest first, optionally narrowed to one state.
pub async fn list(
    pool: &SqlitePool,
    state: Option<TransactionState>,
    limit: i64,
) -> Result<Vec<Transaction>> {
    let rows: Vec<TxRow> = match state {
        Some(s) => {
            sqlx::query_as(&format!(
                "SELECT {TX_COLUMNS} FROM transactions WHERE state = ?1 \
             ORDER BY created_at DESC LIMIT ?2"
            ))
            .bind(s.as_str())
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
        None => {
            sqlx::query_as(&format!(
                "SELECT {TX_COLUMNS} FROM transactions ORDER BY created_at DESC LIMIT ?1"
            ))
            .bind(limit)
            .fetch_all(pool)
            .await?
        }
    };
    rows.into_iter().map(TxRow::decode).collect()
}

/// Moves a `pending` row to `approved`, guarded on `state = 'pending'` **in
/// SQL**. Returns `false` when the guard matched nothing — which is what makes
/// the approve door idempotent by refusal rather than by hope.
pub async fn mark_approved(pool: &SqlitePool, id: &str, approved_by: Option<&str>) -> Result<bool> {
    let now = ts(Utc::now());
    let r = sqlx::query(
        "UPDATE transactions SET state = 'approved', approved_by = ?2, approved_at = ?3, \
         updated_at = ?3 WHERE id = ?1 AND state = 'pending'",
    )
    .bind(id)
    .bind(approved_by)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(r.rows_affected() > 0)
}

/// Moves a `pending` row to `rejected`. Same SQL guard, same reason.
pub async fn mark_rejected(pool: &SqlitePool, id: &str, by: Option<&str>) -> Result<bool> {
    let now = ts(Utc::now());
    let r = sqlx::query(
        "UPDATE transactions SET state = 'rejected', approved_by = ?2, approved_at = ?3, \
         updated_at = ?3 WHERE id = ?1 AND state = 'pending'",
    )
    .bind(id)
    .bind(by)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(r.rows_affected() > 0)
}

/// Stamps the terminal `submitted` state and its receipt, guarded on
/// `state = 'approved'`: only a run that came through an approval can claim a
/// submission, and only once.
pub async fn mark_submitted(pool: &SqlitePool, id: &str, receipt_path: &str) -> Result<bool> {
    let now = ts(Utc::now());
    let r = sqlx::query(
        "UPDATE transactions SET state = 'submitted', submitted_at = ?3, receipt_path = ?2, \
         updated_at = ?3 WHERE id = ?1 AND state = 'approved'",
    )
    .bind(id)
    .bind(receipt_path)
    .bind(&now)
    .execute(pool)
    .await?;
    Ok(r.rows_affected() > 0)
}

/// Expires every `pending` row older than `ttl_secs`, returning how many.
///
/// The deadline is **derived** (`created_at + ttl`) rather than stamped at
/// staging time, so `[transact] approval_ttl_secs` is a live operator control:
/// shortening it retires the stale mandates already sitting in the ledger, and
/// lengthening it does not orphan rows behind a deadline nobody can move.
/// `ttl_secs == 0` sweeps nothing — "no TTL" means what it says.
pub async fn expire_stale(pool: &SqlitePool, ttl_secs: u64) -> Result<u64> {
    if ttl_secs == 0 {
        return Ok(0);
    }
    let cutoff = ts(Utc::now() - chrono::Duration::seconds(ttl_secs as i64));
    let r = sqlx::query(
        "UPDATE transactions SET state = 'expired', updated_at = ?2 \
         WHERE state = 'pending' AND created_at <= ?1",
    )
    .bind(&cutoff)
    .bind(ts(Utc::now()))
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}

/// How many transactions this profile has submitted since `since` — the input
/// to the per-profile daily cap. A profile-less flow (`None`) is counted as its
/// own bucket, not merged with every named identity.
pub async fn submitted_since(
    pool: &SqlitePool,
    profile: Option<&str>,
    since: DateTime<Utc>,
) -> Result<i64> {
    let since = ts(since);
    let count: (i64,) = match profile {
        Some(p) => {
            sqlx::query_as(
                "SELECT COUNT(*) FROM transactions WHERE state = 'submitted' \
             AND profile = ?1 AND submitted_at >= ?2",
            )
            .bind(p)
            .bind(since)
            .fetch_one(pool)
            .await?
        }
        None => {
            sqlx::query_as(
                "SELECT COUNT(*) FROM transactions WHERE state = 'submitted' \
             AND profile IS NULL AND submitted_at >= ?1",
            )
            .bind(since)
            .fetch_one(pool)
            .await?
        }
    };
    Ok(count.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::CommitRefusal;

    fn t(offset_secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + offset_secs, 0).unwrap()
    }

    fn ok_approve(
        state: TransactionState,
        expires: Option<DateTime<Utc>>,
    ) -> std::result::Result<(), ApprovalRefusal> {
        approve_decision(
            true,
            state,
            expires,
            t(0),
            "sha-a",
            Some("sha-a"),
            0,
            Some(5),
        )
    }

    /// The anti-pattern this whole card exists to stop: a key that already
    /// submitted being approved a second time and submitting again. Only a
    /// `pending` row is approvable, so the second approve is a refusal that
    /// names the state — and the SQL guard in [`mark_approved`] enforces the
    /// same rule at the write.
    #[test]
    fn duplicate_key_not_resubmitted() {
        let err = ok_approve(TransactionState::Submitted, None).unwrap_err();
        assert_eq!(
            err,
            ApprovalRefusal::NotPending(TransactionState::Submitted)
        );
        assert!(err.message().contains("submitted"));
        // ...and neither can an approved (already committing) row be re-approved.
        assert_eq!(
            ok_approve(TransactionState::Approved, None).unwrap_err(),
            ApprovalRefusal::NotPending(TransactionState::Approved)
        );
        // The happy path still works, or the test proves nothing.
        assert!(ok_approve(TransactionState::Pending, None).is_ok());
    }

    /// A deadline that has passed is a refusal, not a warning: the evidence
    /// describes a page as it was, and nobody re-reviewed it.
    #[test]
    fn expired_not_approvable() {
        // Deadline one second ago.
        assert_eq!(
            ok_approve(TransactionState::Pending, Some(t(-1))).unwrap_err(),
            ApprovalRefusal::Expired
        );
        // Exactly now is also past — a deadline is inclusive.
        assert_eq!(
            ok_approve(TransactionState::Pending, Some(t(0))).unwrap_err(),
            ApprovalRefusal::Expired
        );
        // One second of headroom is still approvable.
        assert!(ok_approve(TransactionState::Pending, Some(t(1))).is_ok());
        // No deadline never expires.
        assert!(ok_approve(TransactionState::Pending, None).is_ok());
    }

    /// The commit-time guard. An approval binds to the digest of what was
    /// reviewed; a page that drifted afterwards must end as a refusal, never as
    /// a click on a button nobody saw.
    #[test]

    /// An approval that quotes a digest the row does not hold is approving
    /// something else's evidence.
    #[test]
    fn quoted_digest_must_match_the_row() {
        let err = approve_decision(
            true,
            TransactionState::Pending,
            None,
            t(0),
            "sha-a",
            Some("sha-b"),
            0,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, ApprovalRefusal::EvidenceMismatch { .. }));
        // Omitting the quote is legal (a surface that just read the row).
        assert!(approve_decision(
            true,
            TransactionState::Pending,
            None,
            t(0),
            "sha-a",
            None,
            0,
            None
        )
        .is_ok());
    }

    /// `allow_live = false` refuses BEFORE anything else, so a node with the
    /// switch off never even reports which of the other rules a request broke.
    #[test]
    fn live_disabled_refuses_before_every_other_rule() {
        let err = approve_decision(
            false,
            TransactionState::Pending,
            None,
            t(0),
            "sha-a",
            Some("sha-a"),
            0,
            None,
        )
        .unwrap_err();
        assert_eq!(err, ApprovalRefusal::LiveDisabled);
    }

    /// A cap of `0` (and `None`) means "no cap" — the same "omitted means
    /// unlimited" convention every budget in this repo uses. A cap that IS set
    /// refuses at the boundary, not one past it.
    #[test]
    fn daily_cap_refuses_at_the_boundary_and_zero_means_unlimited() {
        let at = |used, cap| {
            approve_decision(
                true,
                TransactionState::Pending,
                None,
                t(0),
                "sha-a",
                None,
                used,
                cap,
            )
        };
        assert!(at(2, Some(3)).is_ok());
        assert_eq!(
            at(3, Some(3)).unwrap_err(),
            ApprovalRefusal::DailyCapReached { cap: 3, used: 3 }
        );
        assert!(at(9_999, Some(0)).is_ok());
        assert!(at(9_999, None).is_ok());
    }

    #[test]
    fn reject_only_reaches_a_pending_row() {
        assert!(reject_decision(TransactionState::Pending).is_ok());
        assert_eq!(
            reject_decision(TransactionState::Submitted).unwrap_err(),
            ApprovalRefusal::NotPending(TransactionState::Submitted)
        );
    }

    /// The digest must be stable across probe ORDER (two renders of the same
    /// page can enumerate fields differently) and must change on every fact a
    /// reviewer actually looked at.
    #[test]

    /// A redacted password's PLAINTEXT must never be an input to the digest —
    /// the digest travels in URLs, logs and approval payloads.
    #[test]
    #[test]
    fn state_round_trips_and_refuses_junk() {
        for s in [
            TransactionState::Pending,
            TransactionState::Approved,
            TransactionState::Submitted,
            TransactionState::Rejected,
            TransactionState::Expired,
        ] {
            assert_eq!(TransactionState::parse(s.as_str()).unwrap(), s);
        }
        assert!(TransactionState::parse("PENDING").is_err());
        assert!(TransactionState::parse("").is_err());
    }

    #[tokio::test]
    async fn staging_a_key_that_already_submitted_does_not_reopen_it() {
        let store = crate::testing::TempStore::new("tx-stage").await;
        let s = &store.storage.pool();
        let row = stage_pending(
            s,
            NewTransaction {
                idempotency_key: "k1",
                app: "transact",
                job_id: Some("job-1"),
                profile: Some("portal"),
                evidence_sha: "sha-a",
            },
        )
        .await
        .unwrap();
        assert_eq!(row.state, TransactionState::Pending);
        assert!(mark_approved(s, &row.id, Some("p1")).await.unwrap());
        // A second approve finds no pending row: the guard is in the SQL.
        assert!(!mark_approved(s, &row.id, Some("p2")).await.unwrap());
        assert!(mark_submitted(s, &row.id, "receipt.json").await.unwrap());
        assert!(!mark_submitted(s, &row.id, "again.json").await.unwrap());

        // Re-staging the SAME key must not resurrect it.
        let again = stage_pending(
            s,
            NewTransaction {
                idempotency_key: "k1",
                app: "transact",
                job_id: Some("job-2"),
                profile: Some("portal"),
                evidence_sha: "sha-b",
            },
        )
        .await
        .unwrap();
        assert_eq!(again.id, row.id);
        assert_eq!(again.state, TransactionState::Submitted);
        assert_eq!(
            again.evidence_sha, "sha-a",
            "terminal evidence is immutable"
        );
        assert_eq!(again.receipt_path.as_deref(), Some("receipt.json"));
        // The cap window: this profile has one submission inside it, and a
        // profile-less flow is its own bucket rather than a share of this one.
        let day_ago = Utc::now() - chrono::Duration::days(1);
        assert_eq!(
            submitted_since(s, Some("portal"), day_ago).await.unwrap(),
            1
        );
        assert_eq!(submitted_since(s, None, day_ago).await.unwrap(), 0);
        // A window that starts after the submission does not count it.
        let future = Utc::now() + chrono::Duration::days(1);
        assert_eq!(submitted_since(s, Some("portal"), future).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn expiry_sweep_only_retires_pending_rows_and_only_with_a_ttl() {
        let store = crate::testing::TempStore::new("tx-expire").await;
        let s = &store.storage.pool();
        let open = stage_pending(
            s,
            NewTransaction {
                idempotency_key: "open",
                app: "transact",
                job_id: None,
                profile: None,
                evidence_sha: "sha",
            },
        )
        .await
        .unwrap();
        let decided = stage_pending(
            s,
            NewTransaction {
                idempotency_key: "decided",
                app: "transact",
                job_id: None,
                profile: None,
                evidence_sha: "sha",
            },
        )
        .await
        .unwrap();
        assert!(mark_rejected(s, &decided.id, None).await.unwrap());

        // A TTL of zero is "no deadline": nothing is swept, ever.
        assert_eq!(expire_stale(s, 0).await.unwrap(), 0);
        assert_eq!(
            get(s, &open.id).await.unwrap().unwrap().state,
            TransactionState::Pending
        );
        // A TTL longer than the row's age leaves it alone.
        assert_eq!(expire_stale(s, 3600).await.unwrap(), 0);
        // A TTL of 0.. wait, a one-second-in-the-past cutoff retires it — and
        // touches ONLY the pending row: a rejected decision stays rejected.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        assert_eq!(expire_stale(s, 1).await.unwrap(), 1);
        assert_eq!(
            get(s, &open.id).await.unwrap().unwrap().state,
            TransactionState::Expired
        );
        assert_eq!(
            get(s, &decided.id).await.unwrap().unwrap().state,
            TransactionState::Rejected
        );
        // An expired row is terminal: it cannot be approved back to life.
        assert!(!mark_approved(s, &open.id, None).await.unwrap());
        assert_eq!(
            list(s, Some(TransactionState::Pending), 10)
                .await
                .unwrap()
                .len(),
            0
        );
        assert_eq!(list(s, None, 10).await.unwrap().len(), 2);
    }
}
