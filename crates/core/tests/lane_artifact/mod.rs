#![allow(dead_code)]
//! The measurement half of a long lane: every scheduled harness run emits one
//! machine-readable artifact — the measured series, the workload that produced
//! them, and the host that ran them.
//!
//! **Measurement here, judgement in the certifier.** The Rust side is not
//! allowed to decide pass or fail: `scripts/ci/lane-certify.mjs` judges this
//! artifact against the criteria pre-declared in `.lanes/criteria.json`, so the
//! verdict is reproducible from artifact + criteria by anyone holding both, and
//! criteria can never be "adjusted while looking at the results" inside the code
//! that produced them (registry: test-harness/long-lane-certification, "declared
//! before, judged after"). It also means a harness that prints a number nobody
//! bounded is no longer possible: an unjudged series shows up in the certifier's
//! report as declared-but-unbounded.
//!
//! Percentiles and slopes are deliberately NOT computed here. Raw series travel
//! to the artifact so a later run can be re-judged under a changed bound without
//! re-running an hours-long lane.
//!
//! Emitting is a hard failure, never a warning: a lane that produced no artifact
//! must be spelled differently from a lane that passed
//! ([_laws: failure-not-empty-success_]). If this cannot write, the run did not
//! happen as far as the record is concerned, and it says so by panicking.
//!
//! Artifacts land in `.lanes/runs/` (gitignored — they are per-run evidence, and
//! the lane's dashboard is the SEQUENCE of them, kept by CI's cache + uploaded
//! run artifacts, not by the repo). Override the directory with
//! `PUMPER_LANE_DIR`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

pub struct Lane {
    lane: &'static str,
    /// Distinguishes several harnesses feeding ONE lane, so two `#[test]`s in
    /// the same binary never race on the same file. The certifier merges every
    /// `<lane>--*.json` back together before judging.
    part: Option<&'static str>,
    workload: Value,
    series: BTreeMap<String, Vec<f64>>,
    scalars: BTreeMap<String, f64>,
}

impl Lane {
    /// `workload` is not decoration. A load lane certifies only the traffic it
    /// generates, so "holds at N" is meaningless without the workload's shape
    /// travelling beside it — corpus size, record shape, concurrency, and
    /// whether the shape is known-real or declared-approximate.
    pub fn new(lane: &'static str, workload: Value) -> Self {
        Self {
            lane,
            part: None,
            workload,
            series: BTreeMap::new(),
            scalars: BTreeMap::new(),
        }
    }

    pub fn part(mut self, part: &'static str) -> Self {
        self.part = Some(part);
        self
    }

    pub fn series(&mut self, name: &str, values: Vec<f64>) -> &mut Self {
        self.series.insert(name.to_string(), values);
        self
    }

    pub fn durations_ms(&mut self, name: &str, values: &[Duration]) -> &mut Self {
        self.series(
            name,
            values.iter().map(|d| d.as_secs_f64() * 1000.0).collect(),
        )
    }

    pub fn scalar(&mut self, name: &str, value: f64) -> &mut Self {
        self.scalars.insert(name.to_string(), value);
        self
    }

    pub fn secs(&mut self, name: &str, value: Duration) -> &mut Self {
        self.scalar(name, value.as_secs_f64())
    }

    pub fn ms(&mut self, name: &str, value: Duration) -> &mut Self {
        self.scalar(name, value.as_secs_f64() * 1000.0)
    }

    fn dir() -> PathBuf {
        match std::env::var_os("PUMPER_LANE_DIR") {
            Some(d) => PathBuf::from(d),
            // CARGO_MANIFEST_DIR is crates/core; the workspace root is two up.
            // Not `std::env::current_dir()`: an integration test's CWD is the
            // package directory, which would scatter artifacts per crate.
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .join("..")
                .join(".lanes")
                .join("runs"),
        }
    }

    pub fn emit(&self) {
        let dir = Self::dir();
        std::fs::create_dir_all(&dir)
            .unwrap_or_else(|e| panic!("lane {}: cannot create {}: {e}", self.lane, dir.display()));
        let name = match self.part {
            Some(p) => format!("{}--{p}.json", self.lane),
            None => format!("{}.json", self.lane),
        };
        let payload = json!({
            "lane": self.lane,
            "kind": "perf",
            "part": self.part,
            "emittedAtUnix": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            "host": {
                "os": std::env::consts::OS,
                "arch": std::env::consts::ARCH,
                "cpus": std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
            },
            "workload": self.workload,
            "series": self.series,
            "scalars": self.scalars,
        });
        let path = dir.join(&name);
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&payload).expect("lane artifact serializes"),
        )
        .unwrap_or_else(|e| panic!("lane {}: cannot write {}: {e}", self.lane, path.display()));
        println!("lane artifact: {}", path.display());
    }
}
