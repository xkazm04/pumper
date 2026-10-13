//! **One materiality verdict**, for every monitor that asks "is this change
//! news?"
//!
//! A change detector's product is not the diff: it is the decision to wake
//! somebody. This repo has four monitors in one context and, before this
//! module, five different answers to that decision — one of which was "there
//! isn't one":
//!
//! | monitor | its private answer |
//! | --- | --- |
//! | `watch` | none — the store's exact-hash `ChangeKind`, reported raw, so any moved byte fired every subscribed webhook |
//! | `connector-api-watch` | an empty line-diff treated as noise, *plus* a Claude-scored `patch\|minor\|major\|breaking` ladder on top |
//! | `plugin`'s observatory | `derived_paths()` over the volatile row fields, after "a watch on this dataset fired on 100% of its rows every run" |
//! | `hackernews` | none — per-run-volatile `points`/`comments` straight into the change-detection hash |
//! | [`crate::crawl::change_weight`] | a SimHash Hamming ramp: `0.0` at or below boilerplate churn, `1.0` at a solidly different document |
//!
//! The fifth one was already right, and already in `core`. This module is its
//! home rather than its sibling: `change_weight` now *delegates* here, so the
//! boilerplate ceiling and the structural floor are one pair of numbers in one
//! place instead of two copies that drift.
//!
//! # The three states
//!
//! | state | meaning | what a monitor does |
//! | --- | --- | --- |
//! | [`Significance::Cosmetic`] | the bytes moved, the content did not | write the record, append **no** revision — nothing fires |
//! | [`Significance::Material`] | a real change, in proportion | append a revision; this is the alert |
//! | [`Significance::Structural`] | so far from the old body it may be a different document | alert, and treat a tombstone or a wholesale-rewrite claim with suspicion |
//!
//! `Cosmetic` is the state this repo had nowhere to put. It is not "unchanged"
//! — the body really did move and readers should see the fresh one — which is
//! why the write stays and only the *revision* is withheld.
//!
//! # Why `core` and not a fifth private floor
//!
//! Three of the four monitors are sibling crates under `crates/apps/`, and apps
//! may depend only on `core`. `core` is the only place all of them can reach,
//! and the arithmetic they need ([`crate::simhash::hamming`],
//! [`crate::simhash::simhash_value`]) has been public here all along with zero
//! app reaching for it.
//!
//! # The lever, not just the label
//!
//! A verdict nobody acts on is the defect that produced
//! [`crate::completeness`], where five apps computed a coverage number and one
//! consulted it. So the declaration is wired to the write:
//! [`Sensitivity::derived_paths`] hands a monitor the [`DerivedPaths`] its
//! upsert narrows the change-detection hash by, from the same list the judge
//! strips before measuring. A path that is noise to the judge is noise to the
//! store, by construction rather than by two call sites agreeing.
//!
//! What a monitor does with a `Cosmetic` verdict is then its own write
//! decision, and the one thing it must NOT be is a per-run narrowing: the
//! stored hash was taken over whatever the previous write declared, so
//! narrowing only on the runs you want suppressed compares a narrow hash
//! against a wide one and reports `changed` — the exact opposite of the intent.
//! `watch` instead holds the baseline record and refreshes only the verdict,
//! which leaves the hash input byte-identical and appends no revision. No
//! revision, no watch firing, no webhook, no 03:00 page about a reflow.

use serde_json::{json, Value};

use crate::datasets::DerivedPaths;
use crate::simhash::{hamming, simhash, simhash_value};

/// SimHash Hamming distances at or below this are boilerplate churn — rotating
/// ads, counters, a re-rendered timestamp, a reflow. Same scale as the
/// crawler's default `dedup_distance = 3` for "near-identical page".
pub const COSMETIC_AT_OR_BELOW: u32 = 3;

/// At or above this distance the new body is not a revision of the old one in
/// any readable sense — a replaced page, a login wall, a different document.
/// It is also where [`Verdict::weight`] reaches `1.0`.
pub const STRUCTURAL_AT_OR_ABOVE: u32 = 16;

/// How much a change matters, as an outcome rather than a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Significance {
    /// The bytes moved, the content did not. Write it; do not wake anyone.
    Cosmetic,
    /// A real change, in proportion to the body it changed.
    Material,
    /// Far enough from the old body that it may not be the same document.
    Structural,
}

impl Significance {
    /// The stable wire word — what lands in a job result and a stored record.
    pub fn as_str(self) -> &'static str {
        match self {
            Significance::Cosmetic => "cosmetic",
            Significance::Material => "material",
            Significance::Structural => "structural",
        }
    }

    /// Whether this is the kind of change a monitor exists to report.
    /// [`Significance::Cosmetic`] is the only one that is not.
    pub fn is_news(self) -> bool {
        !matches!(self, Significance::Cosmetic)
    }
}

/// How harshly one monitor grades its own churn, plus the record paths it
/// declares **noise**: written for readers but not evidence of a change in this
/// record (a `fetched_at`, a re-derived rollup, a verdict block).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sensitivity {
    noise: Vec<String>,
    cosmetic_at_or_below: u32,
    structural_at_or_above: u32,
}

impl Default for Sensitivity {
    /// The fleet default, and the one [`crate::crawl::change_weight`] has
    /// always used: cosmetic at or below 3 bits, structural at or above 16.
    fn default() -> Self {
        Sensitivity {
            noise: Vec::new(),
            cosmetic_at_or_below: COSMETIC_AT_OR_BELOW,
            structural_at_or_above: STRUCTURAL_AT_OR_ABOVE,
        }
    }
}

impl Sensitivity {
    /// Custom distance bounds. A monitor over a tiny record (a handful of
    /// tokens) moves more bits per real change than one over a whole page, so
    /// the bounds are data rather than a constant everybody must live with.
    ///
    /// The structural floor is clamped above the cosmetic ceiling: inverted
    /// bounds would make every change structural and the verdict useless.
    pub fn new(cosmetic_at_or_below: u32, structural_at_or_above: u32) -> Self {
        Sensitivity {
            noise: Vec::new(),
            cosmetic_at_or_below,
            structural_at_or_above: structural_at_or_above.max(cosmetic_at_or_below + 1),
        }
    }

    /// Declares `.`-separated record paths as noise — stripped before both the
    /// path diff and the fingerprint, exactly as [`DerivedPaths`] strips them
    /// before the change-detection hash.
    pub fn noise<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.noise = DerivedPaths::new(paths).paths().to_vec();
        self
    }

    /// The declared noise paths.
    pub fn noise_paths(&self) -> &[String] {
        &self.noise
    }

    /// **The lever.** The declared noise paths as the [`DerivedPaths`] the
    /// monitor's upsert should narrow its change-detection hash by — one
    /// declaration serving both the judgment and the write, so the two can
    /// never disagree about what is noise.
    ///
    /// Deliberately a property of the SENSITIVITY and not of the verdict. A
    /// per-verdict narrowing is a trap: the stored hash was taken over
    /// whatever the *previous* write declared, so narrowing only on the runs
    /// you want suppressed compares a narrow hash against a wide one and
    /// reports `changed` — the opposite of the intent. The derived set has to
    /// be constant for one producer.
    pub fn derived_paths(&self) -> DerivedPaths {
        DerivedPaths::new(self.noise.clone())
    }

    /// The distance at or below which a change is cosmetic.
    pub fn cosmetic_at_or_below(&self) -> u32 {
        self.cosmetic_at_or_below
    }

    /// The distance at or above which a change is structural.
    pub fn structural_at_or_above(&self) -> u32 {
        self.structural_at_or_above
    }

    fn strip<'v>(&self, value: &'v Value) -> std::borrow::Cow<'v, Value> {
        DerivedPaths::new(self.noise.clone()).hash_input(value)
    }
}

/// What the judge compares. Three shapes, because the monitors genuinely hold
/// three different things: two records, two bodies of text, or — the crawler
/// and any monitor that stores a fingerprint instead of the body — two
/// fingerprints.
#[derive(Debug, Clone, Default)]
pub struct Signal {
    previous: Option<Value>,
    current: Option<Value>,
    previous_sim: Option<u64>,
    current_sim: u64,
    moved_hint: Option<Vec<String>>,
}

impl Signal {
    /// Two records: both the moved paths and the fingerprints come from the
    /// values, with the sensitivity's declared noise stripped first.
    pub fn records(previous: &Value, current: &Value) -> Self {
        Signal {
            previous: Some(previous.clone()),
            current: Some(current.clone()),
            ..Signal::default()
        }
    }

    /// A record with no predecessor. There is no magnitude to measure, so the
    /// verdict is [`Significance::Structural`] — a first sighting is graded as
    /// a full change because there is no evidence to discount it.
    pub fn first_sighting() -> Self {
        Signal::default()
    }

    /// Two bodies of text — the monitor holds the documents themselves.
    pub fn bodies(previous: &str, current: &str) -> Self {
        Signal {
            previous_sim: Some(simhash(previous)),
            current_sim: simhash(current),
            ..Signal::default()
        }
    }

    /// Two stored fingerprints and nothing else. `None` (or the sentinel `0`
    /// the crawler stores for "never fingerprinted") is a first sighting.
    pub fn fingerprints(previous_sim: Option<u64>, current_sim: u64) -> Self {
        Signal {
            previous_sim: previous_sim.filter(|s| *s != 0),
            current_sim,
            ..Signal::default()
        }
    }

    /// Attaches the record paths the caller already knows moved. Only needed
    /// alongside [`Signal::bodies`] / [`Signal::fingerprints`];
    /// [`Signal::records`] computes them.
    pub fn moved<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.moved_hint = Some(paths.into_iter().map(Into::into).collect());
        self
    }
}

/// One signal plus one sensitivity is one verdict.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    significance: Significance,
    /// `None` only for a first sighting: there is no prior fingerprint, so
    /// there is no distance. Never zero-filled — a fabricated 0 would read as
    /// "nothing changed".
    distance: Option<u32>,
    weight: f64,
    moved_paths: Vec<String>,
    noise_paths_moved: Vec<String>,
    reason: String,
}

impl Verdict {
    pub fn significance(&self) -> Significance {
        self.significance
    }

    /// SimHash Hamming distance between the two fingerprints, or `None` for a
    /// first sighting.
    pub fn distance(&self) -> Option<u32> {
        self.distance
    }

    /// The change magnitude in `[0,1]`: `0.0` at or below the cosmetic
    /// ceiling, ramping to `1.0` at the structural floor. This is the number
    /// [`crate::crawl::change_weight`] returns.
    pub fn weight(&self) -> f64 {
        self.weight
    }

    /// Record paths that moved and are NOT declared noise.
    pub fn moved_paths(&self) -> &[String] {
        &self.moved_paths
    }

    /// Declared-noise paths that moved. Reported, not hidden: a monitor whose
    /// every run moves only noise is a monitor whose record is wrong.
    pub fn noise_paths_moved(&self) -> &[String] {
        &self.noise_paths_moved
    }

    /// One line an operator can read at 03:00. Never the bare word "changed" —
    /// that is the non-answer this module exists to replace.
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Whether this change is worth telling someone about.
    pub fn is_news(&self) -> bool {
        self.significance.is_news()
    }

    /// The `materiality` block a monitor reports and stores.
    pub fn to_value(&self) -> Value {
        json!({
            "significance": self.significance.as_str(),
            "distance": self.distance,
            // 3 dp: enough to read, short of float noise in a stored result.
            "weight": (self.weight * 1000.0).round() / 1000.0,
            "moved_paths": self.moved_paths,
            "noise_paths_moved": self.noise_paths_moved,
            "reason": self.reason,
        })
    }
}

/// **The judgment.** One signal plus one sensitivity is one verdict.
///
/// Two gates, in this order:
///
/// 1. **Declared noise alone** — every path that moved is declared noise, so
///    after stripping there is nothing left to measure. [`Significance::Cosmetic`]
///    with distance `0`, and the reason names the paths.
/// 2. **Magnitude** — the SimHash Hamming distance between the two
///    noise-stripped fingerprints, read against the sensitivity's two bounds.
///
/// A first sighting short-circuits both: there is no prior fingerprint, so
/// there is no magnitude, and the verdict is [`Significance::Structural`] with
/// `None` for the distance rather than a fabricated `0` (which would read as
/// "nothing changed" about a record nobody has ever seen).
pub fn judge(signal: &Signal, sensitivity: &Sensitivity) -> Verdict {
    let (previous_sim, current_sim, moved_paths, noise_paths_moved) =
        match (&signal.previous, &signal.current) {
            // Two records: the paths and the fingerprints both come from the
            // values, with the declared noise stripped from each first — the
            // same narrowing `DerivedPaths` applies to the write's hash.
            (Some(previous), Some(current)) => {
                let (moved, noise_moved) = partition_moved(previous, current, sensitivity);
                (
                    Some(simhash_value(&sensitivity.strip(previous))),
                    simhash_value(&sensitivity.strip(current)),
                    moved,
                    noise_moved,
                )
            }
            // Bodies or bare fingerprints: the caller supplies both, and names
            // the moved paths itself if it knows them.
            _ => (
                signal.previous_sim,
                signal.current_sim,
                signal.moved_hint.clone().unwrap_or_default(),
                Vec::new(),
            ),
        };

    let Some(previous_sim) = previous_sim else {
        return Verdict {
            significance: Significance::Structural,
            distance: None,
            weight: 1.0,
            moved_paths,
            noise_paths_moved,
            reason: "first sighting — no prior fingerprint to discount against".into(),
        };
    };

    let distance = hamming(previous_sim, current_sim);
    let weight = ramp(distance, sensitivity);

    // Gate 1: only declared noise moved. Reported ahead of the distance check
    // because the reason an operator needs is the PATH, not the arithmetic —
    // and because a monitor that keeps landing here has a record that restates
    // the clock rather than the content.
    //
    // Only reachable when the caller handed over the values, because only then
    // does "no substantive path moved" mean anything. A bare fingerprint pair
    // (the crawler) carries no paths and goes straight to the magnitude.
    if signal.previous.is_some() && moved_paths.is_empty() {
        let reason = if noise_paths_moved.is_empty() {
            "nothing moved".to_string()
        } else {
            format!(
                "only declared-noise path(s) moved: {}",
                noise_paths_moved.join(", ")
            )
        };
        return Verdict {
            significance: Significance::Cosmetic,
            distance: Some(distance),
            weight,
            moved_paths,
            noise_paths_moved,
            reason,
        };
    }

    // Gate 2: magnitude.
    let (significance, reason) = if distance <= sensitivity.cosmetic_at_or_below {
        (
            Significance::Cosmetic,
            format!(
                "the fingerprint moved {distance} bit(s) — at or below the cosmetic ceiling of {}",
                sensitivity.cosmetic_at_or_below
            ),
        )
    } else if distance >= sensitivity.structural_at_or_above {
        (
            Significance::Structural,
            format!(
                "the fingerprint moved {distance} bit(s) — at or above {}, so this may not be the \
                 same document",
                sensitivity.structural_at_or_above
            ),
        )
    } else {
        (
            Significance::Material,
            format!(
                "the fingerprint moved {distance} bit(s), between the cosmetic ceiling of {} and \
                 the structural bound of {}",
                sensitivity.cosmetic_at_or_below, sensitivity.structural_at_or_above
            ),
        )
    };

    Verdict {
        significance,
        distance: Some(distance),
        weight,
        moved_paths,
        noise_paths_moved,
        reason,
    }
}

/// The change magnitude in `[0,1]`: `0.0` at or below the cosmetic ceiling,
/// ramping linearly to `1.0` at the structural bound. The arithmetic
/// [`crate::crawl::change_weight`] has always returned, now stated once.
fn ramp(distance: u32, sensitivity: &Sensitivity) -> f64 {
    if distance <= sensitivity.cosmetic_at_or_below {
        return 0.0;
    }
    let span = sensitivity.structural_at_or_above - sensitivity.cosmetic_at_or_below;
    (f64::from(distance - sensitivity.cosmetic_at_or_below) / f64::from(span)).min(1.0)
}

/// Splits the paths that differ between two records into `(substantive,
/// declared noise)`. Walks objects, so a nested block reports as
/// `history.stats` — the same `.`-separated addressing [`DerivedPaths`] uses.
fn partition_moved(
    previous: &Value,
    current: &Value,
    sensitivity: &Sensitivity,
) -> (Vec<String>, Vec<String>) {
    let mut moved = Vec::new();
    collect_moved(previous, current, String::new(), &mut moved);
    let (noise, substantive): (Vec<String>, Vec<String>) = moved
        .into_iter()
        .partition(|path| sensitivity.noise.iter().any(|n| n == path));
    (substantive, noise)
}

/// Appends every `.`-separated path at which the two values differ. Two objects
/// recurse; anything else compares whole, so a moved array reports as its own
/// path rather than per index.
fn collect_moved(previous: &Value, current: &Value, prefix: String, out: &mut Vec<String>) {
    match (previous, current) {
        (Value::Object(a), Value::Object(b)) => {
            let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
            keys.sort_unstable();
            keys.dedup();
            for key in keys {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                match (a.get(key), b.get(key)) {
                    (Some(x), Some(y)) => collect_moved(x, y, path, out),
                    // Appeared or vanished: the path itself moved.
                    _ => out.push(path),
                }
            }
        }
        _ if previous != current => out.push(if prefix.is_empty() {
            // Two non-object values compared whole — a monitor judging bare
            // scalars. Named rather than pushed as an empty string.
            "<record>".to_string()
        } else {
            prefix
        }),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Realistic slices of an API docs page fetched as markdown — the same
    /// fixtures `connector-api-watch` pins its own diff against, so the two
    /// monitors are provably judged by one function over one pair of bodies.
    const OLD_DOC: &str = "# Payments API\n\n## Endpoints\n\n- POST /v1/charges\n- GET /v1/charges/:id\n\nRate limit: 100 req/min\n";
    const NEW_DOC: &str = "# Payments API\n\n## Endpoints\n\n- POST /v1/payment_intents\n- GET /v1/charges/:id\n\nRate limit: 100 req/min\n";
    /// The same lines, re-indented and reordered. Every content hash in the
    /// tree flips on this; nothing about the document changed.
    const REFLOWED: &str = "  # Payments API\n## Endpoints\n- GET /v1/charges/:id\n- POST /v1/charges\n\nRate limit: 100 req/min\n";

    fn doc(markdown: &str) -> Value {
        json!({ "docs_url": "https://x.test/payments", "markdown": markdown })
    }

    /// **Case 1.** Two records differing only on a declared-noise path.
    #[test]
    fn a_declared_noise_path_moving_alone_is_not_news() {
        let sensitivity = Sensitivity::default().noise(["fetched_at"]);
        let before = json!({ "title": "Release Notes", "fetched_at": "2026-10-05T01:00:00Z" });
        let after = json!({ "title": "Release Notes", "fetched_at": "2026-10-05T02:00:00Z" });

        let v = judge(&Signal::records(&before, &after), &sensitivity);
        assert_eq!(v.significance(), Significance::Cosmetic);
        assert!(!v.is_news(), "a clock tick must not wake anyone");
        assert_eq!(v.distance(), Some(0), "the noise path is stripped first");
        assert!(v.moved_paths().is_empty());
        assert_eq!(v.noise_paths_moved(), ["fetched_at"]);
        assert!(
            v.reason().contains("fetched_at"),
            "the reason must name the path: {}",
            v.reason()
        );
        assert!(
            !v.reason().contains("changed"),
            "'changed' is the non-answer this module replaces: {}",
            v.reason()
        );
    }

    /// **Case 2.** One verdict, two monitors: `connector-api-watch`'s own
    /// reflow fixture judged through the shared function, as a record pair AND
    /// as two bare bodies.
    #[test]
    fn a_reflow_is_cosmetic_whichever_monitor_asks() {
        let as_records = judge(
            &Signal::records(&doc(OLD_DOC), &doc(REFLOWED)),
            &Sensitivity::default(),
        );
        assert_eq!(as_records.significance(), Significance::Cosmetic);
        assert_eq!(as_records.distance(), Some(0));
        assert_eq!(
            as_records.moved_paths(),
            ["markdown"],
            "the path really did move — it is the magnitude that is nil"
        );

        let as_bodies = judge(&Signal::bodies(OLD_DOC, REFLOWED), &Sensitivity::default());
        assert_eq!(as_bodies.significance(), Significance::Cosmetic);
        assert_eq!(as_bodies.distance(), as_records.distance());
    }

    /// **Case 3.** One endpoint replaced: material, and the distance is real.
    #[test]
    fn one_replaced_endpoint_is_material_not_cosmetic() {
        let v = judge(
            &Signal::records(&doc(OLD_DOC), &doc(NEW_DOC)),
            &Sensitivity::default(),
        );
        assert_eq!(v.significance(), Significance::Material);
        assert!(v.is_news());
        assert_eq!(v.moved_paths(), ["markdown"]);
        let d = v.distance().expect("a compared pair has a distance");
        assert!(
            d > COSMETIC_AT_OR_BELOW,
            "distance {d} is not above the ceiling"
        );
        assert!(
            d < STRUCTURAL_AT_OR_ABOVE,
            "one endpoint is not a new document (distance {d})"
        );
        assert!(
            v.weight() > 0.0 && v.weight() < 1.0,
            "weight {}",
            v.weight()
        );
    }

    /// **Case 4.** A body replaced wholesale is distinguishable from a material
    /// edit — the state that says "do not trust a tombstone from this run".
    #[test]
    fn a_wholesale_replacement_is_structural_not_merely_material() {
        let other = "# Maintenance Window\n\nScheduled downtime on Sunday between 02:00 and 04:00 UTC while we migrate the primary cluster. No customer action is required and billing is unaffected.\n";
        let v = judge(
            &Signal::records(&doc(OLD_DOC), &doc(other)),
            &Sensitivity::default(),
        );
        assert_eq!(v.significance(), Significance::Structural);
        assert_ne!(
            v.significance(),
            Significance::Material,
            "structural must be readable as its own state"
        );
        let d = v.distance().expect("a compared pair has a distance");
        assert!(d >= STRUCTURAL_AT_OR_ABOVE, "distance {d}");
        assert_eq!(v.weight(), 1.0);
    }

    /// A first sighting has no magnitude to measure, and the verdict must say
    /// so rather than fabricating a distance of 0 — which would read as
    /// "nothing changed" about a record nobody has ever seen.
    #[test]
    fn a_first_sighting_has_no_distance_rather_than_a_zero_one() {
        let v = judge(&Signal::first_sighting(), &Sensitivity::default());
        assert_eq!(v.significance(), Significance::Structural);
        assert_eq!(v.distance(), None);
        assert_eq!(v.weight(), 1.0);
        assert!(v.reason().contains("first sighting"), "{}", v.reason());
    }

    /// The lever: one declaration serves the judgment and the write, so the
    /// paths the judge ignores are exactly the paths the change-detection hash
    /// ignores. Two lists would be two answers again.
    #[test]
    fn the_declared_noise_is_the_same_set_the_write_narrows() {
        let s = Sensitivity::default().noise(["fetched_at", "materiality"]);
        assert_eq!(s.noise_paths(), ["fetched_at", "materiality"]);
        assert_eq!(s.derived_paths().paths(), s.noise_paths());

        // And an empty declaration is the byte-identical default — the whole
        // safety argument for putting this on a shared write path.
        assert!(Sensitivity::default().derived_paths().is_empty());
    }

    /// Inverted bounds would make every change structural. The constructor
    /// clamps rather than trusting the caller.
    #[test]
    fn inverted_bounds_are_clamped_not_honoured() {
        let s = Sensitivity::new(10, 2);
        assert_eq!(s.cosmetic_at_or_below(), 10);
        assert!(s.structural_at_or_above() > 10);
    }
}
