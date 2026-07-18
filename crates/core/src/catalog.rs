//! The data-source catalog (`catalog/data-sources.toml`) as a load-bearing,
//! machine-readable artifact rather than hand-maintained prose.
//!
//! Each `[[source]]` entry describes one data pipeline: what it scrapes, which
//! app serves it, how fresh, how trustworthy. The file is declared "the single
//! source of truth" in `ONBOARDING.md` and `catalog/README.md`, but until this
//! module it had no reader — so it drifted from the registry silently. Now it is
//! parsed here, served over `GET /catalog/sources`, and cross-checked against the
//! live `AppRegistry` by a server-crate test that fails on drift (a `live` entry
//! whose app isn't registered, or whose `cron` disagrees with the app's
//! `schedule()`).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// One data pipeline in the catalog. Field docs live in `catalog/README.md` and
/// the TOML header; kept in lockstep with the `[[source]]` schema.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Source {
    /// Stable kebab-case slug; equals the Pumper app `name()` when 1:1.
    pub id: String,
    /// App crate serving it (`crates/apps/<app>`); empty when not built yet.
    #[serde(default)]
    pub app: String,
    /// Jurisdiction id in the app's scheme (`us`, `us-ca`, `eu`, `cz`, …).
    #[serde(default)]
    pub market: String,
    pub name: String,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub category: String,
    #[serde(default)]
    pub engine: String,
    #[serde(default)]
    pub access: String,
    #[serde(default)]
    pub cadence: String,
    /// Exact 6-field cron when on the scheduler; empty otherwise.
    #[serde(default)]
    pub cron: String,
    /// `live` | `planned` | `blocked`.
    pub status: String,
    #[serde(default)]
    pub confidence: u8,
    /// Dataset name it writes; empty if n/a.
    #[serde(default)]
    pub dataset: String,
    #[serde(default)]
    pub notes: String,
}

impl Source {
    /// A source is on the scheduler iff it declares a non-empty cron.
    pub fn is_scheduled(&self) -> bool {
        !self.cron.trim().is_empty()
    }

    /// Max-expected age (seconds) between writes for this source's `cadence`, or
    /// `None` for cadences that carry no freshness expectation
    /// (`on-demand`/`one-time`, or an unknown value). Drives the freshness monitor:
    /// a `live` source whose dataset hasn't been written within this window (times
    /// a grace multiplier) is stale.
    pub fn cadence_secs(&self) -> Option<i64> {
        const DAY: i64 = 86_400;
        match self.cadence.trim() {
            "daily" => Some(DAY),
            "weekly" => Some(7 * DAY),
            "monthly" => Some(31 * DAY),
            "quarterly" => Some(93 * DAY),
            "annual" => Some(366 * DAY),
            _ => None, // on-demand | one-time | unknown → no freshness expectation
        }
    }
}

/// The parsed catalog — a list of `[[source]]` entries.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Catalog {
    #[serde(default, rename = "source")]
    pub sources: Vec<Source>,
}

impl Catalog {
    /// Loads from `$PUMPER_CATALOG` or `./catalog/data-sources.toml`. A missing
    /// file is an empty catalog (not an error) so a deployment without the file
    /// still boots; a malformed file IS an error.
    pub fn load() -> Result<Catalog> {
        let path = PathBuf::from(
            std::env::var("PUMPER_CATALOG")
                .unwrap_or_else(|_| "catalog/data-sources.toml".to_string()),
        );
        if !path.exists() {
            tracing::warn!("catalog file {} not found, using empty catalog", path.display());
            return Ok(Catalog::default());
        }
        let raw = std::fs::read_to_string(&path)?;
        Self::parse(&raw).map_err(|e| Error::Config(format!("{}: {e}", path.display())))
    }

    /// Parses catalog TOML from a string (the testable core of [`load`]).
    pub fn parse(raw: &str) -> Result<Catalog> {
        toml::from_str(raw).map_err(|e| Error::Config(e.to_string()))
    }

    /// Sources with `status == "live"` — the pipelines actually running.
    pub fn live(&self) -> impl Iterator<Item = &Source> {
        self.sources.iter().filter(|s| s.status == "live")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sources_and_defaults_missing_fields() {
        let toml = r#"
            [[source]]
            id = "grants-gov"
            app = "grants-gov"
            market = "us"
            name = "Grants.gov"
            status = "live"
            cron = "0 0 9 * * *"

            [[source]]
            id = "future-thing"
            name = "Not built yet"
            status = "planned"
        "#;
        let cat = Catalog::parse(toml).expect("valid");
        assert_eq!(cat.sources.len(), 2);
        assert_eq!(cat.live().count(), 1);
        assert!(cat.sources[0].is_scheduled());
        // Missing optional fields default rather than failing the parse.
        assert_eq!(cat.sources[1].app, "");
        assert!(!cat.sources[1].is_scheduled());
    }

    #[test]
    fn cadence_secs_maps_known_cadences_and_exempts_the_rest() {
        let src = |cadence: &str| Source {
            id: "x".into(),
            app: String::new(),
            market: String::new(),
            name: "x".into(),
            url: String::new(),
            category: String::new(),
            engine: String::new(),
            access: String::new(),
            cadence: cadence.into(),
            cron: String::new(),
            status: "live".into(),
            confidence: 0,
            dataset: String::new(),
            notes: String::new(),
        };
        assert_eq!(src("daily").cadence_secs(), Some(86_400));
        assert_eq!(src("annual").cadence_secs(), Some(366 * 86_400));
        // No freshness expectation for these.
        assert_eq!(src("on-demand").cadence_secs(), None);
        assert_eq!(src("one-time").cadence_secs(), None);
        assert_eq!(src("").cadence_secs(), None);
    }
}
