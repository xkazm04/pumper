//! Where a job's rules came from, and whether that origin can ever be repaired.
//!
//! The profile registry (`resilient-extraction.md` §4) exists because repair is
//! structurally impossible against a job parameter: there is nothing to promote,
//! nothing to roll back to, and nothing to stamp a record with. So a job may now
//! say `{"profile": "acme-products"}` where it used to say `{"rules": {…}}`, and
//! the named form is the only one a repair loop can write back to.
//!
//! Inline `rules` are **not** deprecated — they are the right shape for
//! `POST /extract/preview` iteration, and they keep working byte for byte. What
//! changes is that the system now *says* what it cannot do for them:
//! `repairable: false, reason: "inline rules"`. That is the migration incentive,
//! stated as a fact about the job rather than as a warning nobody reads.
//!
//! Everything here is pure: no database, no I/O. The store side lives on
//! [`Datasets`](crate::datasets::Datasets) (`ensure_profile`, `profile_rules`,
//! `add_profile_version`, `set_active_profile_version`).

use serde::Serialize;
use serde_json::Value;

/// Where one job's extraction rules come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RulesSource {
    /// `{"profile": "<name>"}` — a named, versioned entity in the registry.
    /// The only origin a repair can be written back to.
    Profile { name: String },
    /// `{"rules": {…}}` — a rule set that exists only inside this job's params.
    Inline,
    /// Neither key present: this job does not extract with rules at all.
    None,
}

/// Both keys at once. Deliberately its own outcome rather than a precedence
/// rule: the extractor's `MODE_ROOTS` door already treats "two ways to say the
/// same thing" as a caller mistake, because silently preferring one of them is
/// how a job runs rules the submitter did not think it was running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RulesSourceConflict {
    pub profile: String,
}

/// Resolves a job's rules origin from its params.
///
/// `Err` when both `profile` and `rules` are present — see
/// [`RulesSourceConflict`]. An empty/blank `profile` string is not a profile
/// name; it resolves as if the key were absent, so a caller who templated an
/// empty variable gets the inline/none answer rather than a lookup for `""`.
pub fn rules_source(params: &Value) -> Result<RulesSource, RulesSourceConflict> {
    let profile = params
        .get("profile")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let inline = params.get("rules").is_some_and(|v| !v.is_null());
    match (profile, inline) {
        (Some(name), true) => Err(RulesSourceConflict {
            profile: name.to_string(),
        }),
        (Some(name), false) => Ok(RulesSource::Profile {
            name: name.to_string(),
        }),
        (None, true) => Ok(RulesSource::Inline),
        (None, false) => Ok(RulesSource::None),
    }
}

/// Whether a source with this rules origin can be repaired, and — when it
/// cannot — the reason, phrased for `GET /sources/{id}`.
///
/// This is the whole user-visible consequence of the registry in v1, and it is
/// deliberately a *fact reported about the source*, never a refusal: an inline
/// job runs exactly as it always did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Repairability {
    pub repairable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// The profile a repair would write back to, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

/// The repairability verdict for one rules origin.
pub fn repairability(source: &RulesSource) -> Repairability {
    match source {
        RulesSource::Profile { name } => Repairability {
            repairable: true,
            reason: None,
            profile: Some(name.clone()),
        },
        RulesSource::Inline => Repairability {
            repairable: false,
            reason: Some("inline rules"),
            profile: None,
        },
        RulesSource::None => Repairability {
            repairable: false,
            reason: Some("not rule-backed"),
            profile: None,
        },
    }
}

/// Origins a `profile_versions` row may carry. A version whose origin is not one
/// of these was written by something that did not go through this module.
pub const ORIGINS: [&str; 4] = ["human", "inversion", "claude", "rollback"];

/// Whether `origin` is one this build knows how to explain.
pub fn known_origin(origin: &str) -> bool {
    ORIGINS.contains(&origin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inline_rules_are_reported_unrepairable_not_refused() {
        // THE ANTI-PATTERN: treating "cannot be repaired" as "must not run".
        // Inline rules are the preview/iteration shape and keep working; the
        // registry's only job here is to stop pretending they are repairable.
        let src = rules_source(&json!({"urls": ["u"], "rules": {"t": {}}})).unwrap();
        assert_eq!(src, RulesSource::Inline);
        let r = repairability(&src);
        assert!(!r.repairable);
        assert_eq!(r.reason, Some("inline rules"));
        assert_eq!(r.profile, None);
    }

    #[test]
    fn a_profile_backed_job_is_repairable_and_names_its_write_target() {
        let src = rules_source(&json!({"urls": ["u"], "profile": "acme-products"})).unwrap();
        assert_eq!(
            src,
            RulesSource::Profile {
                name: "acme-products".into()
            }
        );
        let r = repairability(&src);
        assert!(r.repairable);
        assert_eq!(r.reason, None);
        assert_eq!(r.profile.as_deref(), Some("acme-products"));
    }

    #[test]
    fn profile_plus_inline_rules_conflicts_not_silently_prefers_one() {
        // Precedence here would run rules the submitter did not think it was
        // running — the same confusion the extractor's mode door refuses.
        let err = rules_source(&json!({"profile": "p", "rules": {"t": {}}})).unwrap_err();
        assert_eq!(err.profile, "p");
    }

    #[test]
    fn a_blank_profile_name_is_absence_not_a_lookup_for_empty_string() {
        assert_eq!(
            rules_source(&json!({"profile": "   ", "rules": {"t": {}}})).unwrap(),
            RulesSource::Inline
        );
        assert_eq!(
            rules_source(&json!({"profile": ""})).unwrap(),
            RulesSource::None
        );
    }

    #[test]
    fn a_null_rules_key_is_absence_not_inline_rules() {
        assert_eq!(
            rules_source(&json!({"profile": "p", "rules": null})).unwrap(),
            RulesSource::Profile { name: "p".into() }
        );
    }

    #[test]
    fn a_ruleless_job_says_not_rule_backed_not_inline() {
        let r = repairability(&rules_source(&json!({"induce": {}})).unwrap());
        assert!(!r.repairable);
        assert_eq!(r.reason, Some("not rule-backed"));
    }

    #[test]
    fn every_origin_the_registry_writes_is_a_known_one() {
        for o in ["human", "inversion", "claude", "rollback"] {
            assert!(known_origin(o), "{o}");
        }
        assert!(!known_origin("magic"));
    }
}
