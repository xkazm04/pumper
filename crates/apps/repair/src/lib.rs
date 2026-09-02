//! `repair` — closed-loop extraction repair, Tier 0 only, shadow by default.
//!
//! # What it does
//!
//! For a source the health detector has already judged `degraded` or
//! `quarantined`, this app tries to answer one question with **no model and no
//! money**: *which rule set produces the values this source used to produce,
//! from the markup it serves today?*
//!
//! The old values come from `record_revisions` — the era in which we believed
//! the extractor worked — and the new markup from the bodies the runs retained.
//! That pairing turns repair from a judgement into a **search with an answer
//! key**, which is what makes every check downstream deterministic code.
//!
//! # What it deliberately does not do
//!
//! - **No Tier 1.** `resilient-extraction.md` §6.3's Claude proposal is not
//!   built. `AppContext::research` is never called, so a run of this app cannot
//!   spend a cent, and [`CostClass::Free`] is a fact rather than a hope.
//! - **No writing to the live dataset.** It never upserts a record. The only
//!   thing it can ever change is an `extraction_profiles.active_version`
//!   pointer, and only after a clean shadow streak.
//! - **Nothing at all while `[resilience.repair] enabled = false`**, which is
//!   the shipping default. A disabled run returns a `skipped` result rather
//!   than failing: an inert scheduled run is not an error.
//!
//! # Shadow mode
//!
//! A candidate is registered as a `profile_versions` row and scored against the
//! live rules on the same batch, run after run. It is promoted only when it
//! clears all seven gates EVERY run of the probation window and the live rules
//! fail at least one EVERY run — anything less decisive leaves the source
//! quarantined, which is the outcome the design prefers to a coin flip.

use std::collections::BTreeMap;

use async_trait::async_trait;
use pumper_core::config::RepairConfig;
use pumper_core::datasets::Record;
use pumper_core::extract::extract_batch_with_report;
use pumper_core::induce::{invert, InvertOptions};
use pumper_core::resilience::repair::{
    decide_promotion, diagnosis_hash, gate_compiles, gate_holdout, gate_invariants, gate_lint,
    gate_no_regression, judge, repair_idempotency_key, AgreementScore, CandidateEvidence,
    GoldenScore, PromotionDecision, PromotionInput, DEFAULT_AGREEMENT_MIN,
};
use pumper_core::resilience::sketch::sketch_run;
use pumper_core::resilience::{source_id, SourceState};
use pumper_core::{AppContext, AppManifest, CostClass, Error, ManifestExample, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct Repair;

/// Records pulled from the store to build the corpus. Bounded: a repair reads
/// a sample, not a dataset.
const MAX_CORPUS_RECORDS: i64 = 200;

/// Documents handed to the inverter. The rest of the corpus is the holdout, and
/// the split is **structural** — enforced by the corpus splitter, not by asking
/// anything to behave.
const TRAIN_DOCS: usize = 6;

/// The states a repair may act on. `healthy`/`suspect` need no repair, and
/// `retired` is a dead source rather than a broken extractor.
fn repairable_state(state: SourceState) -> bool {
    matches!(state, SourceState::Degraded | SourceState::Quarantined)
}

/// Splits a corpus into `(train, holdout)`.
///
/// Train/test separation is enforced here and nowhere else. The inverter sees
/// `train` and only `train`; every gate scores on `holdout`. A candidate that
/// overfits to the documents it was given has nothing to score with.
fn split_corpus<T: Clone>(items: &[T], train_docs: usize) -> (Vec<T>, Vec<T>) {
    let n = train_docs.min(items.len().saturating_sub(1));
    (items[..n].to_vec(), items[n..].to_vec())
}

/// The last-known-good values for one record: the newest revision written
/// **before** the source started degrading.
///
/// Reading the record's current `data` instead would hand the inverter the
/// broken values and ask it to reproduce them — a repair that perfectly
/// reproduces the breakage.
fn last_good_values(
    revisions: &[pumper_core::datasets::Revision],
    before: chrono::DateTime<chrono::Utc>,
) -> Option<BTreeMap<String, String>> {
    revisions
        .iter()
        .find(|r| r.created_at < before && r.data.is_some())
        .and_then(|r| r.data.as_ref())
        .and_then(|d| d.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| {
                    let s = pumper_core::resilience::sketch::value_text(v);
                    (!s.is_empty()).then(|| (k.clone(), s))
                })
                .collect()
        })
}

/// Per-field match rate of one rule set against known values — the number the
/// no-regression gate compares.
fn field_rates(produced: &[Value], expected: &[BTreeMap<String, String>]) -> BTreeMap<String, f64> {
    let mut hits: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    for (got, want) in produced.iter().zip(expected) {
        for (field, w) in want {
            let e = hits.entry(field.clone()).or_insert((0, 0));
            e.1 += 1;
            if pumper_core::resilience::sketch::value_text(got.get(field).unwrap_or(&Value::Null))
                == *w
            {
                e.0 += 1;
            }
        }
    }
    hits.into_iter()
        .map(|(f, (ok, n))| (f, if n == 0 { 0.0 } else { ok as f64 / n as f64 }))
        .collect()
}

/// Fields the live rules can no longer produce — what a repair is FOR.
fn broken_fields(live_rates: &BTreeMap<String, f64>, floor: f64) -> Vec<String> {
    live_rates
        .iter()
        .filter(|(_, rate)| **rate < floor)
        .map(|(f, _)| f.clone())
        .collect()
}

/// Match rate below which a field counts as broken.
const BROKEN_FIELD_FLOOR: f64 = 0.5;

/// A run that did nothing, and why. Always `Ok`: a scheduled repair on a
/// healthy fleet is supposed to be a no-op, and a no-op that fails the job
/// teaches an operator to ignore the app.
fn skipped(reason: &str, extra: Value) -> Value {
    let mut out = json!({ "repaired": false, "skipped": reason });
    if let (Some(map), Some(more)) = (out.as_object_mut(), extra.as_object()) {
        for (k, v) in more {
            map.insert(k.clone(), v.clone());
        }
    }
    out
}

#[async_trait]
impl ScrapeApp for Repair {
    fn name(&self) -> &'static str {
        "repair"
    }

    fn description(&self) -> &'static str {
        "Closed-loop extraction repair: inverts a degraded source's own last-known-good \
         values against its current markup, scores the candidate through seven \
         deterministic gates, and promotes it only after a clean shadow streak. Tier 0 \
         only — no model, no spend. Off unless [resilience.repair] enabled = true."
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["source"],
                "properties": {
                    "source": {
                        "type": "string",
                        "description": "Source id, `<app>/<dataset>` — the same unit `GET /sources` keys on."
                    },
                    "profile": {
                        "type": "string",
                        "description": "Extraction profile to repair. Defaults to the one bound to the source; without either, the source is `repairable: false` and the run is skipped."
                    }
                }
            })),
            examples: vec![ManifestExample {
                description: "Attempt a Tier-0 repair of a quarantined extractor source",
                params: json!({"source": "extractor/products", "profile": "acme-products"}),
            }],
            output_shape: Some(
                "{repaired: bool, skipped?: reason, source, diagnosis?, diagnosis_hash?, \
                 idempotency_key?, candidates: [{idx, origin, verdict, holdout_match_rate}], \
                 decision?: {decision, ...}, promoted_version?, clean_runs?, events: [] \
                 (webhook kinds this run would emit — dispatch is the server's, not the app's)}",
            ),
            // Structurally free: this app never calls `ctx.research`, so a run
            // cannot produce a spend event. Tier 1 would change this line, and
            // changing this line is the visible marker that it did.
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let cfg: RepairConfig = ctx.health.config().repair.clone();
        let source = ctx
            .params
            .get("source")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::App("repair needs a `source` (\"<app>/<dataset>\")".into()))?
            .to_string();
        let (src_app, src_dataset) = source
            .split_once('/')
            .ok_or_else(|| Error::App(format!("source '{source}' is not `<app>/<dataset>`")))?;
        let sid = source_id(src_app, src_dataset);

        if !cfg.enabled {
            return Ok(skipped("repair disabled", json!({ "source": sid })));
        }
        let Some(store) = ctx.health.store() else {
            return Ok(skipped("health detection disabled", json!({"source": sid})));
        };
        let Some(health) = store.source(&sid).await? else {
            return Ok(skipped("unknown source", json!({ "source": sid })));
        };
        if !repairable_state(health.state) {
            return Ok(skipped(
                "source state does not call for repair",
                json!({"source": sid, "state": health.state}),
            ));
        }

        let now = chrono::Utc::now();
        let guard = store.repair_guard(&sid, now).await?;
        if guard.blocked {
            return Ok(skipped(
                "cooldown after a rollback",
                json!({"source": sid, "blocked_until": guard.blocked_until}),
            ));
        }
        if guard.promotions_30d >= cfg.max_promotions_30d {
            return Ok(skipped(
                "repair budget exhausted",
                json!({"source": sid, "promotions_30d": guard.promotions_30d}),
            ));
        }
        let Some(profile) = ctx
            .params
            .get("profile")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or(guard.profile.clone())
        else {
            // The inline-rules case: there is nothing to write a repair back to.
            return Ok(skipped(
                "inline rules: nothing to repair against",
                json!({"source": sid, "repairable": false}),
            ));
        };
        let Some((live_version, live_rules_json)) =
            ctx.datasets.profile_rules(&profile, None).await?
        else {
            return Ok(skipped(
                "profile has no active version",
                json!({"source": sid, "profile": profile}),
            ));
        };
        let live_rules: pumper_core::extract::RuleSet =
            serde_json::from_value(live_rules_json.clone())?;

        // ---- corpus: retained bodies paired with last-known-good values -----
        let state_since = chrono::DateTime::parse_from_rfc3339(&health.state_since)
            .map(|d| d.with_timezone(&chrono::Utc))
            .unwrap_or(now);
        let records: Vec<Record> = ctx
            .datasets
            .list_records_view(
                src_app,
                src_dataset,
                &[],
                None,
                MAX_CORPUS_RECORDS,
                None,
                false,
            )
            .await?;
        let mut corpus: Vec<(String, BTreeMap<String, String>)> = Vec::new();
        for record in &records {
            let Ok(body) = ctx.read_source_artifact(src_app, record).await else {
                continue;
            };
            let revisions = ctx
                .datasets
                .history(src_app, src_dataset, &record.key, 20)
                .await?;
            let Some(values) = last_good_values(&revisions, state_since) else {
                continue;
            };
            corpus.push((body, values));
        }
        if corpus.len() < cfg.holdout_min_docs {
            return Ok(skipped(
                "not enough retained bodies with known-good values",
                json!({
                    "source": sid,
                    "corpus": corpus.len(),
                    "holdout_min_docs": cfg.holdout_min_docs,
                }),
            ));
        }

        // ---- what is actually broken ---------------------------------------
        let all_docs: Vec<String> = corpus.iter().map(|(b, _)| b.clone()).collect();
        let all_values: Vec<BTreeMap<String, String>> =
            corpus.iter().map(|(_, v)| v.clone()).collect();
        let live_compiled = live_rules.compile()?;
        let live_out = extract_batch_with_report(&live_compiled, &all_docs);
        let live_values: Vec<Value> = live_out.iter().map(|(v, _)| v.clone()).collect();
        let live_rates = field_rates(&live_values, &all_values);
        let broken = broken_fields(&live_rates, BROKEN_FIELD_FLOOR);
        if broken.is_empty() {
            return Ok(skipped(
                "live rules reproduce the known values: nothing to repair",
                json!({"source": sid, "profile": profile}),
            ));
        }
        let diagnosis = health
            .last_verdict
            .clone()
            .unwrap_or_else(|| "unknown".into());
        let dhash = diagnosis_hash(&diagnosis, &broken);
        let idem = repair_idempotency_key(&sid, &dhash);

        // ---- Tier 0: invert the last-known-good values ----------------------
        let (train_docs, holdout_docs) = split_corpus(&all_docs, TRAIN_DOCS);
        let (train_values, holdout_values) = split_corpus(&all_values, TRAIN_DOCS);
        let inversion = invert(&train_values, &train_docs, &InvertOptions::default());

        let attempt = store
            .start_repair_attempt(
                &sid,
                Some(&ctx.job_id.to_string()),
                &diagnosis,
                &dhash,
                "inversion",
            )
            .await?;
        if inversion.candidates.is_empty() {
            store
                .finish_repair_attempt(&attempt, "inversion", "no_candidate", None, 0.0)
                .await?;
            return Ok(json!({
                "repaired": false,
                "skipped": "inversion produced no candidate",
                "source": sid,
                "profile": profile,
                "diagnosis": diagnosis,
                "diagnosis_hash": dhash,
                "idempotency_key": idem,
                "broken_fields": broken,
                "unresolved": inversion.unresolved,
                "events": Vec::<String>::new(),
            }));
        }

        // ---- score every candidate through the seven gates ------------------
        let baseline_rate = live_rates
            .iter()
            .filter(|(f, _)| !broken.contains(f))
            .map(|(_, r)| *r)
            .fold(f64::NAN, f64::max)
            .max(0.0);
        let mut outputs: Vec<Vec<Value>> = Vec::new();
        let mut compiled_ok: Vec<bool> = Vec::new();
        for rules in &inversion.candidates {
            match gate_compiles(rules) {
                Ok(c) => {
                    let out = extract_batch_with_report(&c, &holdout_docs);
                    outputs.push(out.into_iter().map(|(v, _)| v).collect());
                    compiled_ok.push(true);
                }
                Err(_) => {
                    outputs.push(Vec::new());
                    compiled_ok.push(false);
                }
            }
        }
        let agreements = pumper_core::resilience::repair::gate_agreement(&outputs);
        let invariants = store.invariants(&sid).await.unwrap_or_default();

        let mut rows = Vec::new();
        let mut accepted: Option<(usize, &pumper_core::extract::RuleSet)> = None;
        for (idx, rules) in inversion.candidates.iter().enumerate() {
            let compile_errors = gate_compiles(rules).err().unwrap_or_default();
            let produced = &outputs[idx];
            let sketches = if produced.is_empty() {
                Default::default()
            } else {
                let c = rules.compile()?;
                let pairs = extract_batch_with_report(&c, &holdout_docs);
                sketch_run(pairs.iter().map(|(v, r)| (v, r)))
            };
            let verdict = judge(CandidateEvidence {
                compile_errors,
                lint: gate_lint(rules, &holdout_docs, &broken),
                holdout: gate_holdout(produced, &holdout_values, &broken),
                baseline_rate,
                // Golden documents are NOT built (`data/golden/` has no store),
                // so gate 4 cannot run and therefore REFUSES. That is the whole
                // reason nothing promotes today, and it is the honest state:
                // the anchor against baseline poisoning does not exist yet.
                golden: GoldenScore {
                    checked: 0,
                    exact_ok: 0,
                    exact_mismatches: Vec::new(),
                    shape_mismatches: Vec::new(),
                },
                golden_available: false,
                agreement: agreements.get(idx).cloned().unwrap_or(AgreementScore {
                    group: idx,
                    group_size: 1,
                    groups: inversion.candidates.len(),
                }),
                invariant_violations: gate_invariants(
                    &invariants,
                    produced,
                    &sketches,
                    ctx.health.config().invariant_violation_ratio,
                ),
                regressions: gate_no_regression(
                    &field_rates(produced, &holdout_values),
                    &live_rates,
                    &broken,
                ),
                agreement_min: cfg.agreement_min.max(DEFAULT_AGREEMENT_MIN),
                _marker: std::marker::PhantomData,
            });
            let rules_json = serde_json::to_value(rules)?;
            store
                .record_repair_candidate(
                    &attempt,
                    idx as i64,
                    "inversion",
                    &rules_json,
                    &verdict,
                    Some(live_version),
                )
                .await?;
            rows.push(json!({
                "idx": idx,
                "origin": "inversion",
                "verdict": verdict.rejected_for.clone().unwrap_or_else(|| "accepted".into()),
                "holdout_match_rate": verdict.holdout.rate,
            }));
            if verdict.accepted && accepted.is_none() {
                accepted = Some((idx, rules));
            }
        }

        let Some((idx, winner)) = accepted else {
            store
                .finish_repair_attempt(&attempt, "validating", "rejected", None, 0.0)
                .await?;
            return Ok(json!({
                "repaired": false,
                "skipped": "no candidate cleared all seven gates",
                "source": sid,
                "profile": profile,
                "diagnosis": diagnosis,
                "diagnosis_hash": dhash,
                "idempotency_key": idem,
                "broken_fields": broken,
                "candidates": rows,
                "events": Vec::<String>::new(),
            }));
        };

        // ---- shadow: register the candidate, decide whether it may promote --
        let version = ctx
            .datasets
            .add_profile_version(
                &profile,
                &serde_json::to_value(winner)?,
                "inversion",
                Some(live_version),
                Some(&json!({"attempt": attempt, "candidate": idx, "broken_fields": broken})),
            )
            .await?;
        let clean_runs = store.consecutive_clean_shadow_runs(&sid, &dhash).await?;
        let decision = decide_promotion(
            &cfg,
            &PromotionInput {
                candidate_version: version,
                candidate_diagnosis_hash: dhash.clone(),
                current_diagnosis_hash: dhash.clone(),
                clean_runs,
                candidate_clean: true,
                live_failing: true,
                promotions_30d: guard.promotions_30d,
                blocked_by_cooldown: guard.blocked,
            },
        );

        let mut events: Vec<&str> = Vec::new();
        let (stage, outcome, promoted) = match &decision {
            PromotionDecision::Promote { version } => {
                ctx.datasets
                    .set_active_profile_version(&profile, *version)
                    .await?;
                events.push("source.repair_promoted");
                ("promoted", "promoted", Some(*version))
            }
            PromotionDecision::Wait { .. } => ("shadow", "shadow_clean", None),
            PromotionDecision::Drop { .. } => ("shadow", "shadow_failed", None),
            PromotionDecision::Blocked { .. } => ("shadow", "blocked", None),
        };
        store
            .finish_repair_attempt(&attempt, stage, outcome, promoted, 0.0)
            .await?;

        Ok(json!({
            "repaired": promoted.is_some(),
            "source": sid,
            "profile": profile,
            "diagnosis": diagnosis,
            "diagnosis_hash": dhash,
            "idempotency_key": idem,
            "broken_fields": broken,
            "candidates": rows,
            "shadow_version": version,
            "clean_runs": clean_runs,
            "decision": decision,
            "promoted_version": promoted,
            // Named, not dispatched: `webhook::dispatch_event` lives in the
            // server, above the app boundary, so the worker is what turns these
            // into deliveries. Saying which events a run WOULD emit keeps the
            // gap visible instead of silently absent.
            "events": events,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_degraded_or_quarantined_source_is_repaired() {
        // Repairing a healthy source is not a repair, it is an unrequested
        // rule change; `retired` is a dead site, not a broken extractor.
        assert!(repairable_state(SourceState::Degraded));
        assert!(repairable_state(SourceState::Quarantined));
        for s in [
            SourceState::Healthy,
            SourceState::Suspect,
            SourceState::Probation,
            SourceState::Retired,
            SourceState::Unknown,
        ] {
            assert!(!repairable_state(s), "{s:?}");
        }
    }

    #[test]
    fn the_holdout_is_split_structurally_not_by_asking_nicely() {
        let items: Vec<usize> = (0..30).collect();
        let (train, holdout) = split_corpus(&items, TRAIN_DOCS);
        assert_eq!(train.len(), TRAIN_DOCS);
        assert_eq!(holdout.len(), 30 - TRAIN_DOCS);
        // Disjoint, always: a candidate must never be scored on a document it
        // was derived from.
        assert!(train.iter().all(|t| !holdout.contains(t)));
        // A corpus too small to split still leaves a holdout rather than
        // silently scoring on the training set.
        let tiny: Vec<usize> = (0..3).collect();
        let (t, h) = split_corpus(&tiny, TRAIN_DOCS);
        assert_eq!(t.len(), 2);
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn last_good_values_reads_before_the_break_not_the_broken_present() {
        use chrono::{Duration, Utc};
        let broke_at = Utc::now();
        let rev = |offset: i64, price: &str| pumper_core::datasets::Revision {
            app: "extractor".into(),
            dataset: "products".into(),
            key: "k".into(),
            revision: 1,
            change: "changed".into(),
            data: Some(json!({ "price": price })),
            diff: None,
            created_at: broke_at + Duration::minutes(offset),
            trust: "stable".into(),
            provenance: Default::default(),
        };
        // Newest first, as `Datasets::history` returns them.
        let history = vec![rev(10, ""), rev(-5, "19.99"), rev(-60, "18.00")];
        let got = last_good_values(&history, broke_at).unwrap();
        assert_eq!(got.get("price").map(String::as_str), Some("19.99"));
        // Reading the present would hand the inverter the breakage and ask it
        // to reproduce it perfectly.
        assert_ne!(got.get("price").map(String::as_str), Some(""));
        // No revision predates the break: honest absence, never a guess.
        assert!(last_good_values(&[rev(10, "1")], broke_at).is_none());
    }

    #[test]
    fn a_field_is_broken_only_when_the_live_rules_mostly_miss_it() {
        let rates = BTreeMap::from([
            ("price".to_string(), 0.0),
            ("sku".to_string(), 0.49),
            ("title".to_string(), 0.51),
            ("blurb".to_string(), 1.0),
        ]);
        assert_eq!(
            broken_fields(&rates, BROKEN_FIELD_FLOOR),
            vec!["price".to_string(), "sku".to_string()]
        );
    }

    #[test]
    fn a_disabled_repair_is_a_skipped_run_not_a_failed_one() {
        // A scheduled repair on a healthy fleet is supposed to do nothing, and
        // a no-op that fails the job teaches an operator to ignore the app.
        let out = skipped("repair disabled", json!({"source": "extractor/products"}));
        assert_eq!(out["repaired"], json!(false));
        assert_eq!(out["skipped"], json!("repair disabled"));
        assert_eq!(out["source"], json!("extractor/products"));
    }
}
