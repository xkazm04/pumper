//! The index-time enrichment pipeline (N11): the ordered list of [`Enricher`]s
//! `[search] enrichers` names, the shipped regex pass behind that trait, and the
//! WASM-plugin enricher that lets a deployment add an entity KIND by installing
//! a `.wasm` instead of by bumping the index schema.
//!
//! Two invariants this module exists to hold:
//!
//! 1. **Fail open, per document.** An enricher that traps, times out, returns
//!    malformed output, or names a plugin nobody installed yields NO entities
//!    for that document and is counted. It never fails the batch: the index is a
//!    derived artifact, and losing a document entirely because one optional
//!    field could not be computed is a strictly worse outcome than losing the
//!    field.
//! 2. **Order decides collisions, first writer wins.** `["builtin",
//!    "plugin:x"]` means the shipped regexes own `amount`/`event_date` and the
//!    plugin may add anything else — so appending a plugin cannot change what
//!    existing `amount_gte` queries match.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use pumper_core::config::SearchConfig;
use pumper_core::{
    merge_entities, parse_enricher_spec, EnrichInput, Enricher, EnricherSpec, EnricherStat, Entity,
    Error, Plugins, Result, SearchDoc, ENTITY_AMOUNT, ENTITY_EVENT_DATE,
};
use serde_json::Value;

use crate::enrich;

/// The shipped regex pass, behind the trait: `amount` (largest US-dollar amount
/// with an explicit currency marker) and `event_date` (earliest upcoming
/// deadline-like date). Identical rules to the pre-N11 built-in path — this is a
/// move, not a rewrite, so an index built before and after holds the same two
/// fields for the same documents.
pub struct BuiltinEnricher;

#[async_trait]
impl Enricher for BuiltinEnricher {
    fn name(&self) -> &str {
        pumper_core::ENRICHER_BUILTIN
    }

    async fn enrich(&self, input: &EnrichInput) -> Vec<Entity> {
        builtin_entities(&input.text, input.now)
    }

    /// The whole batch on ONE blocking thread. This is where the regex CPU work
    /// has to stay: it used to run inside the writer-lock closure, and moving it
    /// out (onto a blocking thread, before the lock) is what stopped it
    /// serializing every other indexing path. A per-document `spawn_blocking`
    /// would hand that back one hop at a time.
    async fn enrich_batch(&self, inputs: &[EnrichInput]) -> Vec<Vec<Entity>> {
        let inputs = inputs.to_vec();
        tokio::task::spawn_blocking(move || {
            inputs
                .iter()
                .map(|i| builtin_entities(&i.text, i.now))
                .collect::<Vec<_>>()
        })
        .await
        // A panicked enrichment thread yields no entities for the batch rather
        // than taking the batch down — the fail-open rule, applied to ourselves.
        .unwrap_or_else(|e| {
            tracing::warn!("built-in enrichment task panicked: {e}");
            Vec::new()
        })
    }
}

/// The two built-in entities for one document's text. Absent, never zero: a
/// document with no marked amount has no `amount` entity at all, so an
/// `amount_gte` filter cannot match it.
fn builtin_entities(text: &str, now: i64) -> Vec<Entity> {
    let (amount, event_date) = enrich::enrich_fields(text, now);
    let mut out = Vec::with_capacity(2);
    if let Some(amount) = amount {
        out.push(Entity::new(ENTITY_AMOUNT, amount));
    }
    if let Some(date) = event_date {
        out.push(Entity::new(ENTITY_EVENT_DATE, date));
    }
    out
}

/// A WASM enricher: one core-module plugin call per document, under the host's
/// own fuel budget and memory cap, with every failure swallowed and counted.
pub struct PluginEnricher {
    /// The `[search] enrichers` entry, e.g. `plugin:enrich-money-date` — what
    /// `GET /search/status` reports under, so a stat row can be matched back to
    /// the config line that produced it.
    label: String,
    /// The plugin name the host loaded the module under.
    plugin: String,
    plugins: Arc<dyn Plugins>,
    params: Value,
    failures: AtomicU64,
}

impl PluginEnricher {
    pub fn new(plugin: impl Into<String>, plugins: Arc<dyn Plugins>) -> Self {
        let plugin = plugin.into();
        Self {
            label: format!("{}{plugin}", pumper_core::ENRICHER_PLUGIN_PREFIX),
            plugin,
            plugins,
            params: Value::Null,
            failures: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Enricher for PluginEnricher {
    fn name(&self) -> &str {
        &self.label
    }

    fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    async fn enrich(&self, input: &EnrichInput) -> Vec<Entity> {
        let out = match self
            .plugins
            .run(&self.plugin, &input.text, &self.params)
            .await
        {
            Ok(value) => value,
            Err(e) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    plugin = %self.plugin,
                    "enricher plugin failed on one document; indexing it without those \
                     entities: {e}"
                );
                return Vec::new();
            }
        };
        match parse_enricher_output(&out) {
            Ok(entities) => entities,
            Err(why) => {
                self.failures.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    plugin = %self.plugin,
                    "enricher plugin returned output this host cannot read ({why}); \
                     indexing the document without those entities"
                );
                Vec::new()
            }
        }
    }
}

/// The enricher plugin output contract: `{"entities": {kind: scalar|array}}`.
///
/// A named refusal rather than a silent empty result: a plugin that returns
/// `{"amount": 5}` (the fields, not the envelope) or a `null` entity value is a
/// plugin whose author will otherwise see an index with no entities and no
/// reason. `null` is dropped specifically — "no match = no field" means an
/// absent entity, and writing a null would make a stored `currency` key that
/// carries no currency.
pub fn parse_enricher_output(out: &Value) -> std::result::Result<Vec<Entity>, String> {
    let Some(entities) = out.get("entities") else {
        return Err("no `entities` key (the contract is {\"entities\": {kind: value}})".into());
    };
    if entities.is_null() {
        return Ok(Vec::new());
    }
    let Some(map) = entities.as_object() else {
        return Err(format!("`entities` must be an object, got {entities}"));
    };
    let mut parsed = Vec::with_capacity(map.len());
    for (kind, value) in map {
        let kind = kind.trim();
        if kind.is_empty() {
            return Err("an entity kind must not be empty".into());
        }
        if value.is_null() {
            continue; // absent, never a null placeholder
        }
        if value.is_object() {
            return Err(format!(
                "entity '{kind}' is an object; entity values are scalars or arrays of scalars"
            ));
        }
        parsed.push(Entity::new(kind, value.clone()));
    }
    Ok(parsed)
}

/// One enricher plus what it has done, so `GET /search/status` can report a pass
/// that runs and finds nothing separately from one that fails on every document.
struct Slot {
    enricher: Arc<dyn Enricher>,
    docs: AtomicU64,
    entities: AtomicU64,
}

/// The configured enrichment pipeline: the ordered enricher list plus its
/// counters.
pub struct Enrichment {
    slots: Vec<Slot>,
}

impl Enrichment {
    /// Builds the pipeline from `[search] enrichers`.
    ///
    /// An unparseable entry is a hard error at construction (i.e. at boot): the
    /// alternative is a typo that silently drops a whole enrichment pass, which
    /// looks exactly like a corpus with nothing to extract. A `plugin:` entry
    /// naming a module the host has not loaded is only a WARNING — plugins hot-
    /// swap through `POST /plugins/reload`, so "not loaded yet" is a legitimate
    /// state — and every document it is offered counts as a failure until it is.
    pub fn from_config(cfg: &SearchConfig, plugins: Option<Arc<dyn Plugins>>) -> Result<Self> {
        let mut enrichers: Vec<Arc<dyn Enricher>> = Vec::with_capacity(cfg.enrichers.len());
        for entry in &cfg.enrichers {
            match parse_enricher_spec(entry)
                .map_err(|why| Error::App(format!("[search] enrichers: {why} (entry {entry:?})")))?
            {
                EnricherSpec::Builtin => enrichers.push(Arc::new(BuiltinEnricher)),
                EnricherSpec::Plugin(name) => {
                    let Some(plugins) = plugins.clone() else {
                        return Err(Error::App(format!(
                            "[search] enrichers names {entry:?}, but this process has no plugin \
                             host wired to the search index"
                        )));
                    };
                    if !plugins.has(&name) {
                        tracing::warn!(
                            plugin = %name,
                            "[search] enrichers names a plugin that is not loaded — every \
                             document counts as an enricher failure until it is installed \
                             (`just plugins-install`, then POST /plugins/reload)"
                        );
                    }
                    enrichers.push(Arc::new(PluginEnricher::new(name, plugins)));
                }
            }
        }
        Ok(Self::from_enrichers(enrichers))
    }

    /// The pipeline over an explicit enricher list (tests, and the shared tail
    /// of [`from_config`]).
    pub fn from_enrichers(enrichers: Vec<Arc<dyn Enricher>>) -> Self {
        Self {
            slots: enrichers
                .into_iter()
                .map(|enricher| Slot {
                    enricher,
                    docs: AtomicU64::new(0),
                    entities: AtomicU64::new(0),
                })
                .collect(),
        }
    }

    /// Names, in configured order — what a status surface reports even before
    /// anything has been indexed.
    pub fn names(&self) -> Vec<&str> {
        self.slots.iter().map(|s| s.enricher.name()).collect()
    }

    /// Per-enricher counters for `GET /search/status`.
    pub fn stats(&self) -> Vec<EnricherStat> {
        self.slots
            .iter()
            .map(|s| EnricherStat {
                name: s.enricher.name().to_string(),
                docs: s.docs.load(Ordering::Relaxed),
                entities: s.entities.load(Ordering::Relaxed),
                failures: s.enricher.failures(),
            })
            .collect()
    }

    /// Runs every configured enricher over a batch, in order, merging into one
    /// entity map per document (first writer wins).
    ///
    /// Runs BEFORE the index writer lock is taken — the property the pre-N11
    /// code established and this must not give back.
    pub async fn run(&self, docs: &[SearchDoc]) -> Vec<BTreeMap<String, Value>> {
        let inputs: Vec<EnrichInput> = docs.iter().map(EnrichInput::from_doc).collect();
        let mut merged: Vec<BTreeMap<String, Value>> = vec![BTreeMap::new(); docs.len()];
        for slot in &self.slots {
            // Every enricher sees every document, so `docs` counts offers, not
            // successes — a pass with docs > 0 and entities == 0 is an enricher
            // that ran and honestly found nothing.
            slot.docs.fetch_add(docs.len() as u64, Ordering::Relaxed);
            let per_doc = slot.enricher.enrich_batch(&inputs).await;
            for (i, entities) in per_doc.into_iter().enumerate() {
                let Some(into) = merged.get_mut(i) else {
                    continue;
                };
                slot.entities
                    .fetch_add(entities.len() as u64, Ordering::Relaxed);
                merge_entities(into, entities);
            }
        }
        merged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The output contract, refused BY NAME. The anti-pattern: treating any
    /// unreadable plugin output as "found nothing", so a plugin returning the
    /// fields without the `entities` envelope produces an index with no entities
    /// and no explanation anywhere.
    #[test]
    fn malformed_plugin_output_is_refused_by_name_not_read_as_an_empty_result() {
        let err = parse_enricher_output(&serde_json::json!({"amount": 5}))
            .expect_err("the envelope is the contract");
        assert!(err.contains("no `entities` key"), "{err}");

        let err = parse_enricher_output(&serde_json::json!({"entities": [1, 2]}))
            .expect_err("entities is an object");
        assert!(err.contains("must be an object"), "{err}");

        let err = parse_enricher_output(&serde_json::json!({"entities": {"": 1}}))
            .expect_err("an unnamed kind is not a kind");
        assert!(err.contains("must not be empty"), "{err}");

        let err = parse_enricher_output(&serde_json::json!({"entities": {"geo": {"lat": 1}}}))
            .expect_err("a nested object is not a filterable value");
        assert!(err.contains("scalars or arrays"), "{err}");
    }

    /// A null entity is an ABSENT entity, never a stored null: the whole
    /// doctrine is "no match = no field", and a `currency` key carrying null
    /// would make a document look like it has a currency nobody can read.
    #[test]
    fn a_null_entity_value_is_dropped_instead_of_stored() {
        let out = parse_enricher_output(&serde_json::json!({
            "entities": {"currency": "czk", "ico": null, "amounts": [1, 2]}
        }))
        .expect("valid output");
        let kinds: Vec<&str> = out.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(kinds, vec!["amounts", "currency"]);
        assert_eq!(out[1].value, serde_json::json!("czk"));
        // An explicit `entities: null` is "this document has none", not an error.
        assert_eq!(
            parse_enricher_output(&serde_json::json!({"entities": null})).unwrap(),
            Vec::new()
        );
    }

    struct StubEnricher {
        name: String,
        emit: Vec<Entity>,
    }

    #[async_trait]
    impl Enricher for StubEnricher {
        fn name(&self) -> &str {
            &self.name
        }
        async fn enrich(&self, _input: &EnrichInput) -> Vec<Entity> {
            self.emit.clone()
        }
    }

    /// The ordering contract, at pipeline level: a later enricher ADDS kinds and
    /// cannot take one from an earlier pass. The anti-pattern is last-writer-
    /// wins, where installing a plugin silently redefines `amount` for every
    /// query already calibrated on the built-in rules.
    #[tokio::test]
    async fn a_later_enricher_adds_kinds_without_taking_an_earlier_ones() {
        let pipeline = Enrichment::from_enrichers(vec![
            Arc::new(StubEnricher {
                name: "first".into(),
                emit: vec![Entity::new(ENTITY_AMOUNT, 100u64)],
            }),
            Arc::new(StubEnricher {
                name: "second".into(),
                emit: vec![
                    Entity::new(ENTITY_AMOUNT, 7u64),
                    Entity::new("currency", "czk"),
                ],
            }),
        ]);
        let doc = SearchDoc {
            id: "a:b:c".into(),
            app: "a".into(),
            dataset: "b".into(),
            url: String::new(),
            title: "t".into(),
            body: "body".into(),
            indexed_at: 1_767_225_600,
        };
        let merged = pipeline.run(&[doc]).await;
        assert_eq!(merged[0][ENTITY_AMOUNT], serde_json::json!(100));
        assert_eq!(merged[0]["currency"], serde_json::json!("czk"));

        let stats = pipeline.stats();
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].name, "first");
        assert_eq!((stats[0].docs, stats[0].entities), (1, 1));
        // `entities` counts what the pass EMITTED, before collision merging —
        // otherwise a shadowed pass looks like one that found nothing.
        assert_eq!((stats[1].docs, stats[1].entities), (1, 2));
        assert!(stats.iter().all(|s| s.failures == 0));
    }
}
