//! Cursor subscriptions over the durable event log (N05): the selector model,
//! and the one outbox drain every push consumer is served by.
//!
//! ## Why there is only one drain
//!
//! Before this, four event kinds had durable, retried, dead-letterable delivery
//! — a job's `callback_url`, a dataset `watch`, a saved-search alert and
//! `[webhooks] failure_url` — each hand-wired at its own call site with its own
//! secret-resolution branch. Every OTHER kind (a job succeeding, a trigger
//! firing, a contract verdict, a governance action, an ingested external event)
//! could not be subscribed to at all. A subscription is the generalization: an
//! event *selector*, a sink, and a *cursor*.
//!
//! One pass, on the scheduler tick and again in a finished job's fan-out:
//!
//! 1. flush the bus's pending rows into the log (so no cursor can run past what
//!    is durable);
//! 2. for each enabled subscription, read the events after its `cursor_seq`,
//!    keep the ones its selector matches, and hand each to the SAME
//!    [`crate::webhook`] transport a watch takes — so the delivery log, the
//!    in-process ladder, the DLQ and manual replay are unchanged, `plugin:`
//!    sinks (N10) included;
//! 3. advance the cursor.
//!
//! ## What "advance the cursor" means, exactly
//!
//! The cursor moves once every event in the page has a **durable delivery row**
//! — not once the receiver has answered. That is a deliberate departure from
//! "advance only on `delivered`", and the reason is that the DLQ already owns
//! the answer: a delivery row is retried on a 30s→2h ladder and then parked in
//! the dead-letter queue for manual replay. Blocking the cursor on the receiver
//! instead would make one dead endpoint stop a subscription forever *and*
//! duplicate the retry machinery. So: at-least-once handoff into a durable log,
//! with the delivery log as the record of what happened next. A delivery row
//! that cannot be WRITTEN does not advance the cursor — the event has not been
//! handed off, `last_error` says why, and the next tick retries it.
//!
//! ## Watches are an adapter, not a second mechanism
//!
//! A watch **is** a subscription with a dataset selector. The `watches` table
//! stays the source of truth for watch rows (and `POST /watches` keeps working,
//! deprecated), but the fan-out runs through this drain: `worker::notify_watches`
//! now emits one `dataset.changed` event per changed `(app, dataset)` instead of
//! dispatching per watch, and the drain fans that event out to every matching
//! watch AND every matching native subscription. One event, one code path, and
//! a watch that has been disabled for a day resumes from its own cursor.

use pumper_core::{EventRecord, Subscription, Watch};
use serde_json::{json, Value};
use tracing::warn;

use crate::state::AppState;

/// Pages one drain call will walk per subscription before leaving the rest to
/// the next tick. Bounds a catch-up: a subscription re-enabled after a week
/// works through its backlog over several ticks instead of queueing the whole
/// log onto the delivery pool at once.
const MAX_PAGES_PER_TICK: usize = 20;

/// Serializes drain passes process-wide.
///
/// Not a nicety: the drain runs at the end of EVERY finished job's fan-out as
/// well as on the scheduler tick, so on a busy server several passes overlap by
/// construction. Two passes that read the same page before either advances the
/// cursor would each dispatch it — a duplicate delivery, which at-least-once
/// permits but which nothing here has any reason to produce. The cursor fence
/// (`cursor_seq < ?`) keeps the *state* correct under concurrency; this keeps
/// the *deliveries* from doubling.
///
/// A full lock rather than `try_lock`: a skipped pass would leave a just-emitted
/// `job.succeeded` sitting until the next scheduler tick, which is the latency
/// the in-fan-out drain exists to avoid. A pass is bounded
/// (`outbox_batch` × [`MAX_PAGES_PER_TICK`] per target), so waiting is bounded too.
static DRAIN_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---- Selectors ---------------------------------------------------------------

/// One equality test against a JSON pointer into the stored event.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectorFilter {
    /// RFC-6901 pointer into the event body (`/app`, `/result/dataset`).
    pub pointer: String,
    pub equals: Value,
}

/// What a subscription wants. Every field is a narrowing: an empty/absent field
/// matches everything, so `{}` is "the whole log" and a selector can only ever
/// get more specific.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Selector {
    /// Event kinds (`job.succeeded`, `dataset.changed`, `external`, …). Empty =
    /// every kind.
    pub kinds: Vec<String>,
    /// Namespace. `None` or `"*"` = every app.
    pub app: Option<String>,
    /// Dataset, matched against the event's subject and its `result.dataset`.
    /// `None` or `"*"` = every dataset.
    pub dataset: Option<String>,
    pub filters: Vec<SelectorFilter>,
}

/// Parses (and validates) a selector from its stored/POSTed JSON.
///
/// Refusals are the point: an unparseable selector accepted at create time is a
/// subscription that sits `enabled` forever and never fires — the same
/// accepted-but-dead shape `watch_target_refusal` exists to kill. A pointer that
/// is not a pointer is refused HERE, not silently treated as "matches nothing".
pub fn parse_selector(v: &Value) -> Result<Selector, String> {
    let obj = match v {
        Value::Null => return Ok(Selector::default()),
        Value::Object(map) => map,
        other => return Err(format!("selector must be an object, got {other}")),
    };
    for key in obj.keys() {
        if !matches!(key.as_str(), "kinds" | "app" | "dataset" | "filters") {
            return Err(format!(
                "unknown selector field '{key}' (expected: kinds, app, dataset, filters)"
            ));
        }
    }
    let kinds = match obj.get("kinds") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let kind = item
                    .as_str()
                    .ok_or_else(|| format!("selector.kinds must be strings, got {item}"))?;
                if kind.is_empty() {
                    return Err("selector.kinds must not contain an empty string".into());
                }
                out.push(kind.to_string());
            }
            out
        }
        Some(other) => return Err(format!("selector.kinds must be an array, got {other}")),
    };
    let app = optional_str(obj.get("app"), "selector.app")?;
    let dataset = optional_str(obj.get("dataset"), "selector.dataset")?;
    let mut filters = Vec::new();
    match obj.get("filters") {
        None | Some(Value::Null) => {}
        Some(Value::Array(items)) => {
            for item in items {
                let pointer = item.get("pointer").and_then(Value::as_str).ok_or_else(|| {
                    format!("selector.filters[] needs a `pointer` string: {item}")
                })?;
                if !pointer.starts_with('/') {
                    return Err(format!(
                        "selector.filters[].pointer must be a JSON pointer starting with '/', \
                         got {pointer:?}"
                    ));
                }
                let equals = item
                    .get("equals")
                    .cloned()
                    .ok_or_else(|| format!("selector.filters[] needs an `equals` value: {item}"))?;
                filters.push(SelectorFilter {
                    pointer: pointer.to_string(),
                    equals,
                });
            }
        }
        Some(other) => return Err(format!("selector.filters must be an array, got {other}")),
    }
    Ok(Selector {
        kinds,
        app,
        dataset,
        filters,
    })
}

fn optional_str(v: Option<&Value>, field: &str) -> Result<Option<String>, String> {
    match v {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if s.is_empty() => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(other) => Err(format!("{field} must be a string, got {other}")),
    }
}

impl Selector {
    /// Whether this selector covers one logged event. Pure — the whole matching
    /// contract is testable without a store, a bus or a receiver.
    pub fn matches(&self, event: &EventRecord) -> bool {
        if !self.kinds.is_empty() && !self.kinds.iter().any(|k| k == &event.kind) {
            return false;
        }
        if let Some(app) = &self.app {
            if app != "*" && app != &event.app {
                return false;
            }
        }
        if let Some(dataset) = &self.dataset {
            if dataset != "*" && !event_names_dataset(event, dataset) {
                return false;
            }
        }
        self.filters
            .iter()
            .all(|f| event.payload.pointer(&f.pointer) == Some(&f.equals))
    }

    pub fn to_json(&self) -> Value {
        let mut out = json!({});
        if !self.kinds.is_empty() {
            out["kinds"] = json!(self.kinds);
        }
        if let Some(app) = &self.app {
            out["app"] = json!(app);
        }
        if let Some(dataset) = &self.dataset {
            out["dataset"] = json!(dataset);
        }
        if !self.filters.is_empty() {
            out["filters"] = Value::Array(
                self.filters
                    .iter()
                    .map(|f| json!({ "pointer": f.pointer, "equals": f.equals }))
                    .collect(),
            );
        }
        out
    }
}

/// Whether a logged event is about `dataset`.
///
/// Two places carry it, and both are checked because they are populated by
/// different emitters: the log row's `subject_id` (what `dataset.changed` sets)
/// and the event body's `result.dataset` (what the delivered payload carries).
/// Checking only one would make a selector match on one emitter's events and
/// silently ignore another's.
fn event_names_dataset(event: &EventRecord, dataset: &str) -> bool {
    if event.subject_id == dataset {
        return true;
    }
    event
        .payload
        .pointer("/result/dataset")
        .and_then(Value::as_str)
        == Some(dataset)
}

/// The selector a watch IS: `dataset.changed` in the watch's namespace, for the
/// watch's dataset (`"*"` = every dataset of the app).
pub fn watch_selector(watch: &Watch) -> Selector {
    Selector {
        kinds: vec![DATASET_CHANGED.to_string()],
        app: Some(watch.app.clone()),
        dataset: Some(watch.dataset.clone()),
        filters: Vec::new(),
    }
}

/// The event kind a dataset watch subscribes to.
pub const DATASET_CHANGED: &str = "dataset.changed";

// ---- The outbox --------------------------------------------------------------

/// One drain target: a native subscription row, or a watch adapted onto the same
/// shape. The `kind` is what the delivery log records, which is why a watch
/// keeps `change` — every existing `GET /watches/{id}/deliveries` query, and
/// `resolve_secret`'s `change` arm, keep working unchanged.
struct Target {
    id: String,
    delivery_kind: &'static str,
    selector: Selector,
    sink: String,
    url: String,
    secret: Option<String>,
    cursor: i64,
    /// A watch's payload carries `watch_id`; a native subscription's does not.
    is_watch: bool,
}

impl Target {
    fn from_subscription(sub: Subscription, selector: Selector) -> Self {
        Self {
            id: sub.id,
            delivery_kind: "subscription",
            selector,
            sink: sub.sink,
            url: sub.url,
            secret: sub.secret,
            cursor: sub.cursor_seq,
            is_watch: false,
        }
    }

    fn from_watch(watch: Watch) -> Self {
        let selector = watch_selector(&watch);
        Self {
            id: watch.id,
            delivery_kind: "change",
            selector,
            sink: watch.sink,
            url: watch.url,
            secret: watch.secret,
            cursor: watch.cursor_seq,
            is_watch: true,
        }
    }
}

/// One outbox pass. Safe to call from anywhere: it is a no-op when the durable
/// log is off (`[events] log_enabled = false`), in which case the worker's
/// legacy in-line watch dispatch is what delivers.
pub async fn drain(state: &AppState) {
    if !state.events.logs() {
        return;
    }
    let _pass = DRAIN_LOCK.lock().await;
    // Flush first, ALWAYS — a cursor must never be able to run past what is
    // durable, and the events this tick is meant to deliver are usually the ones
    // still sitting in the queue.
    match state.events.persist_pending(&state.storage).await {
        Ok(0) => {}
        Ok(n) => tracing::debug!(rows = n, "event log: appended"),
        Err(e) => {
            warn!("event log: append failed, events stay queued for the next tick: {e}");
            return;
        }
    }
    let dropped = state.events.dropped_before_persist();
    if dropped > 0 {
        warn!(
            dropped,
            "event log: events were dropped before their log write (the pending queue hit \
             `[events] pending_capacity`) — those events are NOT in the log and no subscription \
             will ever see them"
        );
    }

    let targets = match load_targets(state).await {
        Ok(targets) => targets,
        Err(e) => {
            warn!("subscription drain: could not load targets: {e}");
            return;
        }
    };
    for target in targets {
        drain_target(state, target).await;
    }
}

/// Every enabled push target: native subscriptions plus watch adapters.
async fn load_targets(state: &AppState) -> pumper_core::Result<Vec<Target>> {
    let mut targets = Vec::new();
    for sub in state.storage.list_subscriptions(true).await? {
        match parse_selector(&sub.selector) {
            Ok(selector) => targets.push(Target::from_subscription(sub, selector)),
            Err(e) => {
                // Stored rows are validated at create time, so this is a row
                // written by an older build or edited by hand. Skipping it
                // loudly beats matching everything or nothing silently.
                warn!(subscription = %sub.id, "subscription has an unusable selector: {e}");
            }
        }
    }
    for watch in state.storage.enabled_watches_all().await? {
        targets.push(Target::from_watch(watch));
    }
    Ok(targets)
}

/// Walks one target forward from its cursor, dispatching what its selector
/// matches. Stops at the first event whose delivery row cannot be written.
async fn drain_target(state: &AppState, target: Target) {
    let batch = state.config.events.outbox_batch.max(1);
    let mut cursor = target.cursor;
    for _ in 0..MAX_PAGES_PER_TICK {
        let page = match state.storage.events_after(cursor, None, None, batch).await {
            Ok(page) => page,
            Err(e) => {
                warn!(target = %target.id, "subscription drain: read failed: {e}");
                return;
            }
        };
        if page.is_empty() {
            return;
        }
        let page_len = page.len();
        let mut reached = cursor;
        for event in page {
            let seq = event.seq;
            if target.selector.matches(&event) {
                if let Err(e) = dispatch(state, &target, &event).await {
                    // The cursor stops HERE, at the last event that did get a
                    // delivery row: an event with no durable row has not been
                    // handed off and must be retried, not skipped.
                    warn!(target = %target.id, seq, "subscription drain: dispatch failed: {e}");
                    advance(state, &target, reached).await;
                    let _ = state
                        .storage
                        .record_subscription_error(&target.id, &e.to_string())
                        .await;
                    return;
                }
            }
            // Advanced past NON-matching events too: a narrow selector must not
            // re-scan the whole log on every tick forever.
            reached = seq;
        }
        advance(state, &target, reached).await;
        cursor = reached;
        if (page_len as i64) < batch {
            return;
        }
    }
}

async fn advance(state: &AppState, target: &Target, seq: i64) {
    if seq <= target.cursor {
        return;
    }
    let result = if target.is_watch {
        state.storage.advance_watch_cursor(&target.id, seq).await
    } else {
        state
            .storage
            .advance_subscription_cursor(&target.id, seq)
            .await
    };
    if let Err(e) = result {
        warn!(target = %target.id, seq, "subscription drain: cursor advance failed: {e}");
    }
}

/// Hands one matched event to the target's sink through the shared webhook
/// transport, creating the delivery row FIRST so the cursor can only advance
/// past events that are durably owed to somebody.
async fn dispatch(
    state: &AppState,
    target: &Target,
    event: &EventRecord,
) -> Result<(), pumper_core::Error> {
    let payload = delivery_payload(target, event);
    crate::webhook::dispatch_logged(
        state,
        target.delivery_kind,
        &target.id,
        &target.sink,
        &target.url,
        &event.kind,
        &payload,
        target.secret.clone(),
    )
    .await
}

/// The body one target receives for one event.
///
/// A watch receives exactly what it received before N05 — the `dataset.changed`
/// payload with its own `watch_id` stamped in — because a receiver written
/// against the old shape must not have to change. A native subscription
/// receives the event envelope (`seq`, `kind`, `app`, `subject_id`, `payload`),
/// which is what makes a cursor consumer able to resume: the body carries the
/// sequence it should record.
fn delivery_payload(target: &Target, event: &EventRecord) -> Value {
    if target.is_watch {
        let mut body = event
            .payload
            .pointer("/result")
            .cloned()
            .unwrap_or_else(|| event.payload.clone());
        if let Value::Object(map) = &mut body {
            map.insert("watch_id".into(), json!(target.id));
        }
        return body;
    }
    json!({
        "subscription_id": target.id,
        "seq": event.seq,
        "kind": event.kind,
        "app": event.app,
        "subject_id": event.subject_id,
        "created_at": event.created_at,
        "payload": event.payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn event(seq: i64, kind: &str, app: &str, subject: &str, payload: Value) -> EventRecord {
        EventRecord {
            seq,
            kind: kind.into(),
            app: app.into(),
            subject_id: subject.into(),
            payload,
            created_at: Utc::now(),
        }
    }

    fn changed(app: &str, dataset: &str) -> EventRecord {
        event(
            1,
            DATASET_CHANGED,
            app,
            dataset,
            json!({ "app": app, "status": DATASET_CHANGED, "result": { "dataset": dataset, "count": 2 } }),
        )
    }

    /// An empty selector is the whole log, not an empty one. The anti-pattern:
    /// treating an absent field as a filter that matches nothing, which turns
    /// `POST /subscriptions {}` into a subscription that sits enabled forever
    /// and never fires.
    #[test]
    fn an_empty_selector_matches_everything_not_nothing() {
        let sel = parse_selector(&json!({})).expect("empty selector");
        assert!(sel.matches(&changed("grants", "unified")));
        assert!(sel.matches(&event(2, "job.succeeded", "fake", "j1", json!({}))));
        assert_eq!(parse_selector(&Value::Null).expect("null"), sel);
    }

    #[test]
    fn kinds_and_app_narrow_independently() {
        let sel = parse_selector(&json!({ "kinds": ["job.succeeded"], "app": "fake" }))
            .expect("selector");
        assert!(sel.matches(&event(1, "job.succeeded", "fake", "j1", json!({}))));
        assert!(!sel.matches(&event(2, "job.failed", "fake", "j1", json!({}))));
        assert!(!sel.matches(&event(3, "job.succeeded", "other", "j1", json!({}))));
    }

    /// `*` is the wildcard the watch model already used, and it must mean the
    /// same thing here or porting a `dataset = "*"` watch onto a selector would
    /// silently narrow it to a dataset literally named `*`.
    #[test]
    fn star_is_a_wildcard_not_a_literal() {
        let sel = parse_selector(&json!({ "app": "*", "dataset": "*" })).expect("selector");
        assert!(sel.matches(&changed("grants", "unified")));
        assert!(sel.matches(&changed("fake", "d")));
    }

    /// The dataset is carried in two places by two emitters; matching only one
    /// would make a selector work against half the system.
    #[test]
    fn dataset_matches_the_subject_or_the_body() {
        let sel = parse_selector(&json!({ "dataset": "unified" })).expect("selector");
        assert!(sel.matches(&changed("grants", "unified")));
        // Subject empty, dataset only in the body.
        assert!(sel.matches(&event(
            9,
            DATASET_CHANGED,
            "grants",
            "",
            json!({ "result": { "dataset": "unified" } })
        )));
        assert!(!sel.matches(&changed("grants", "raw")));
    }

    #[test]
    fn filters_are_pointer_equality_into_the_event_body() {
        let sel = parse_selector(&json!({
            "filters": [{ "pointer": "/result/count", "equals": 2 }]
        }))
        .expect("selector");
        assert!(sel.matches(&changed("grants", "unified")));
        let sel = parse_selector(&json!({
            "filters": [{ "pointer": "/result/count", "equals": 99 }]
        }))
        .expect("selector");
        assert!(!sel.matches(&changed("grants", "unified")));
        // A pointer that resolves to nothing does not match — and does not panic.
        let sel = parse_selector(&json!({
            "filters": [{ "pointer": "/nope/deep", "equals": null }]
        }))
        .expect("selector");
        assert!(!sel.matches(&changed("grants", "unified")));
    }

    /// A selector this build cannot express is refused at the door, not stored
    /// and quietly ignored.
    #[test]
    fn a_malformed_selector_is_refused_not_stored_dead() {
        for bad in [
            json!({ "kinds": "job.succeeded" }),
            json!({ "kinds": [1] }),
            json!({ "kinds": [""] }),
            json!({ "app": 7 }),
            json!({ "filters": [{ "pointer": "result/dataset", "equals": 1 }] }),
            json!({ "filters": [{ "pointer": "/a" }] }),
            json!({ "typo": true }),
            json!([1, 2]),
        ] {
            assert!(
                parse_selector(&bad).is_err(),
                "selector {bad} must be refused"
            );
        }
    }

    #[test]
    fn a_watch_is_a_dataset_changed_selector() {
        let watch = Watch {
            id: "w1".into(),
            app: "grants".into(),
            dataset: "unified".into(),
            url: "https://example.test/h".into(),
            secret: None,
            sink: "webhook".into(),
            enabled: true,
            cursor_seq: 0,
            created_at: Utc::now(),
        };
        let sel = watch_selector(&watch);
        assert!(sel.matches(&changed("grants", "unified")));
        assert!(!sel.matches(&changed("grants", "raw")));
        assert!(!sel.matches(&changed("other", "unified")));
        // And a job event never reaches a watch, however the namespaces line up.
        assert!(!sel.matches(&event(5, "job.succeeded", "grants", "j1", json!({}))));
        // Round-trips through storage as JSON.
        assert_eq!(parse_selector(&sel.to_json()).expect("round trip"), sel);
    }

    /// A watch receives the same body it received before the outbox existed —
    /// the `dataset.changed` payload with its own id stamped in. A receiver
    /// written against the old shape must not have to change.
    #[test]
    fn a_watch_body_is_unchanged_and_a_subscription_body_carries_the_cursor() {
        let watch_target = Target::from_watch(Watch {
            id: "w1".into(),
            app: "grants".into(),
            dataset: "unified".into(),
            url: String::new(),
            secret: None,
            sink: "webhook".into(),
            enabled: true,
            cursor_seq: 0,
            created_at: Utc::now(),
        });
        let ev = changed("grants", "unified");
        let body = delivery_payload(&watch_target, &ev);
        assert_eq!(body["dataset"], "unified");
        assert_eq!(body["count"], 2);
        assert_eq!(body["watch_id"], "w1", "the watch still learns which it is");
        assert!(
            body.get("seq").is_none(),
            "the old shape gains nothing else"
        );

        let sub_target = Target::from_subscription(
            Subscription {
                id: "s1".into(),
                name: None,
                selector: json!({}),
                sink: "webhook".into(),
                url: String::new(),
                secret: None,
                cursor_seq: 0,
                enabled: true,
                principal_id: None,
                created_at: Utc::now(),
                last_delivered_at: None,
                last_error: None,
            },
            Selector::default(),
        );
        let body = delivery_payload(&sub_target, &ev);
        assert_eq!(body["seq"], 1, "a cursor consumer is told where it now is");
        assert_eq!(body["kind"], DATASET_CHANGED);
        assert_eq!(body["subscription_id"], "s1");
        assert_eq!(body["payload"]["result"]["count"], 2);
    }
}
