//! Pumper app: CMS fee-schedule release watcher **and PPRRVU reference-data owner**.
//!
//! Keeps Counterbill's Medicare reference-price database fresh. Counterbill bakes
//! the CMS Physician Fee Schedule (PFS) into generated tables via
//! `scripts/ingest-cms-pfs.mjs`, pinned to one RVU release (e.g. `RVU26A`). CMS
//! republishes the Relative Value Files quarterly (RVU{YY}A/B/C/D) with an annual
//! conversion-factor change — so the baked data silently goes stale. This app is
//! the freshness signal: it detects the LATEST published release and reports
//! whether it is newer than what the caller currently has baked.
//!
//! **M32 (Medicare price oracle)**: on a NEW release (or `force:true`) the app now
//! also downloads the release ZIP itself (via the http engine's binary
//! `fetch_bytes` seam — engine-traits#2-LITE), extracts the PPRRVU CSV, and owns
//! the parsed corpus:
//!   · `fee_schedule` dataset — one record per `{hcpcs}` or `{hcpcs}:{modifier}`
//!     with work/PE/MP RVUs + the conversion factor, queryable through the
//!     existing dataset/`?filter=`/search surfaces.
//!   · `fee_schedule_changes` dataset — one record per release with the
//!     release-over-release diff (counts + bounded top movers by total-RVU delta).
//! The watcher behavior is unchanged: a download/extract/parse failure NEVER
//! fails the run — the release record is already stored, the failure is reported
//! in the `parse` block and logged loudly.
//!
//! ## Pinned ZIP/CSV layout assumptions (NOT live-verified — no download in CI)
//!
//! The RVU ZIP/CSV layout is asserted, not proven, against these pins; any drift
//! is a **loud typed error** (surfaced in `parse.error` + `tracing::error`), never
//! a silently wrong parse:
//!   1. The release ZIP at `https://www.cms.gov/files/zip/rvu{yy}{q}.zip` contains
//!      exactly one CSV whose file name (last path component, case-insensitive)
//!      starts with `PPRRVU` and ends with `.csv` (e.g. `PPRRVU26_JAN.csv`).
//!   2. That CSV has a preamble (title/notes rows) followed by ONE header row —
//!      the first row containing a cell equal to `HCPCS` — then data rows.
//!   3. The header row names (case-insensitive, matched by contains) columns for:
//!      `HCPCS`, work RVU (`WORK`+`RVU`), non-facility PE RVU (`PE`+`RVU`+`NON`),
//!      facility PE RVU (`PE`+`RVU`+`FAC`, not `NON`), malpractice RVU (`MP`+`RVU`)
//!      and the conversion factor (`CONV`). `MOD`/`DESCRIPTION`/`STATUS` are
//!      optional extras.
//!   4. A data row's HCPCS cell is exactly 5 ASCII-alphanumeric characters
//!      (`99213`, `G0008`); anything else (footnotes, blank separators) is skipped.
//!   5. RVU cells are plain decimals; an unparseable cell is stored as `Null`
//!      (honest missing — never a fabricated 0.00).
//!
//! Params: `{ "schedule": "pfs" }`
//!   · `schedule`      — only `"pfs"` is supported today (extensible to clfs/asp).
//!   · `known_release` — OPTIONAL explicit baseline (the release Counterbill has
//!                       baked). Omitted by default: the watcher **self-baselines**
//!                       off the release it stored last run, so `is_newer_than_known`
//!                       clears itself once a release is seen (`baseline_source`
//!                       reports `param`/`stored`/`none`).
//!   · `force`         — OPTIONAL bool: download+parse the latest release even if
//!                       it is unchanged since last run (backfill / re-parse).

use std::collections::BTreeMap;
use std::io::Read as _;

use async_trait::async_trait;
use pumper_core::{
    AppContext, AppManifest, ChangeKind, CostClass, Error, HttpRequest, ManifestExample,
    Provenance, Result, ScrapeApp,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub struct CmsFeeSchedule;

/// CMS PFS Relative Value Files index — lists every `RVU{YY}{Q}` release.
const PFS_INDEX_URL: &str =
    "https://www.cms.gov/medicare/payment/fee-schedules/physician/pfs-relative-value-files";

/// Hard cap for the release-ZIP download (recent RVU ZIPs are ~5–10 MB; 64 MiB
/// leaves generous headroom while bounding memory — `fetch_bytes` buffers).
const MAX_ZIP_BYTES: u64 = 64 * 1024 * 1024;

/// Bound on the `top_movers` list in a release diff.
const TOP_MOVERS_CAP: usize = 20;

/// Bound on the `removed_sample` list in a release diff.
const REMOVED_SAMPLE_CAP: usize = 20;

/// How many prior `fee_schedule` rows the diff reads back (the corpus is ~17k
/// keys per release; this is a defensive ceiling, not an expected truncation).
const PRIOR_LIST_LIMIT: i64 = 200_000;

/// A parsed RVU release, e.g. `RVU26B` → year 2026, quarter `B` (Apr).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Release {
    year: u32,
    /// Uppercase quarter letter `A`..=`D` (A=Jan, B=Apr, C=Jul, D=Oct).
    quarter: char,
}

impl Release {
    /// Canonical id, e.g. `"RVU26B"`.
    fn id(&self) -> String {
        format!("RVU{:02}{}", self.year % 100, self.quarter)
    }

    /// Total-order key: a newer year, then a later quarter, sorts greater.
    fn ord_key(&self) -> (u32, char) {
        (self.year, self.quarter)
    }

    /// Quarter as 1..=4 (A→1 … D→4).
    fn quarter_num(&self) -> u32 {
        (self.quarter as u32) - ('A' as u32) + 1
    }

    /// The direct ZIP URL CMS publishes for this release (lowercase token) — the
    /// same convention `scripts/ingest-cms-pfs.mjs` fetches.
    fn zip_url(&self) -> String {
        format!(
            "https://www.cms.gov/files/zip/rvu{:02}{}.zip",
            self.year % 100,
            self.quarter.to_ascii_lowercase()
        )
    }

    /// The release's landing page under the index.
    fn source_url(&self) -> String {
        format!(
            "{PFS_INDEX_URL}/rvu{:02}{}",
            self.year % 100,
            self.quarter.to_ascii_lowercase()
        )
    }
}

/// Parse a single release id like `"RVU26A"` (case-insensitive). None if malformed
/// or the quarter is outside A–D.
fn parse_release(s: &str) -> Option<Release> {
    let up = s.trim().to_ascii_uppercase();
    let b = up.as_bytes();
    if b.len() < 6 || &b[0..3] != b"RVU" {
        return None;
    }
    let (d1, d2, q) = (b[3], b[4], b[5]);
    if !(d1.is_ascii_digit() && d2.is_ascii_digit() && (b'A'..=b'D').contains(&q)) {
        return None;
    }
    let yy = ((d1 - b'0') as u32) * 10 + (d2 - b'0') as u32;
    Some(Release {
        year: 2000 + yy,
        quarter: q as char,
    })
}

/// Scan an HTML/text blob for every distinct `rvuYYq` release token (in hrefs or
/// text), returned sorted oldest→newest. Pure — the unit-tested core.
fn detect_releases(html: &str) -> Vec<Release> {
    let lower = html.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut seen = std::collections::BTreeSet::new();
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(p) = lower[i..].find("rvu") {
        let pos = i + p;
        i = pos + 3; // advance past this match (monotonic → no infinite loop)
        if pos + 6 <= bytes.len() {
            let (d1, d2, q) = (bytes[pos + 3], bytes[pos + 4], bytes[pos + 5]);
            if d1.is_ascii_digit() && d2.is_ascii_digit() && (b'a'..=b'd').contains(&q) {
                let year = 2000 + ((d1 - b'0') as u32) * 10 + (d2 - b'0') as u32;
                let quarter = (q as char).to_ascii_uppercase();
                if seen.insert((year, quarter)) {
                    out.push(Release { year, quarter });
                }
            }
        }
    }
    out.sort_by_key(Release::ord_key);
    out
}

fn latest(releases: &[Release]) -> Option<Release> {
    releases.iter().max_by_key(|r| r.ord_key()).copied()
}

// ── PPRRVU ingest (M32) ─────────────────────────────────────────────────────

/// Splits one CSV line into cells: commas separate, double quotes wrap cells
/// that contain commas, `""` inside a quoted cell is a literal quote. Minimal
/// by design — the PPRRVU CSV needs nothing more (no embedded newlines).
fn split_csv_line(line: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    cur.push('"');
                } else {
                    quoted = false;
                }
            }
            '"' => quoted = true,
            ',' if !quoted => cells.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    cells.push(cur);
    cells
}

/// Column indices resolved from the PPRRVU header row (pin #3 in the doc header).
#[derive(Debug)]
struct ColumnMap {
    hcpcs: usize,
    modifier: Option<usize>,
    description: Option<usize>,
    status: Option<usize>,
    work: usize,
    pe_nonfac: usize,
    pe_fac: usize,
    mp: usize,
    conv: usize,
}

/// Resolves the pinned columns from a header row, or a drift message naming
/// exactly what is missing (and the header seen) so the failure is actionable.
fn locate_columns(header: &[String]) -> std::result::Result<ColumnMap, String> {
    let up: Vec<String> = header
        .iter()
        .map(|h| h.trim().to_ascii_uppercase())
        .collect();
    let find = |pred: &dyn Fn(&str) -> bool| up.iter().position(|h| pred(h));
    let hcpcs = find(&|h| h == "HCPCS");
    let modifier = find(&|h| h == "MOD" || h == "MODIFIER");
    let description = find(&|h| h.starts_with("DESCRIPTION"));
    let status = find(&|h| h.starts_with("STATUS"));
    let work = find(&|h| h.contains("WORK") && h.contains("RVU"));
    let pe_nonfac = find(&|h| h.contains("PE") && h.contains("RVU") && h.contains("NON"));
    let pe_fac =
        find(&|h| h.contains("PE") && h.contains("RVU") && h.contains("FAC") && !h.contains("NON"));
    let mp = find(&|h| h.contains("MP") && h.contains("RVU"));
    let conv = find(&|h| h.contains("CONV"));

    let mut missing = Vec::new();
    for (name, col) in [
        ("HCPCS", hcpcs),
        ("work RVU", work),
        ("non-facility PE RVU", pe_nonfac),
        ("facility PE RVU", pe_fac),
        ("MP RVU", mp),
        ("conversion factor (CONV)", conv),
    ] {
        if col.is_none() {
            missing.push(name);
        }
    }
    if !missing.is_empty() {
        return Err(format!(
            "PPRRVU header drift: missing column(s) {missing:?} in header row {up:?} — \
             the pinned CSV layout changed; update the pins in app-cms-fee-schedule"
        ));
    }
    Ok(ColumnMap {
        hcpcs: hcpcs.unwrap(),
        modifier,
        description,
        status,
        work: work.unwrap(),
        pe_nonfac: pe_nonfac.unwrap(),
        pe_fac: pe_fac.unwrap(),
        mp: mp.unwrap(),
        conv: conv.unwrap(),
    })
}

/// Whether a cell is a plausible HCPCS code (pin #4): exactly 5 ASCII
/// alphanumerics.
fn looks_like_hcpcs(s: &str) -> bool {
    s.len() == 5 && s.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// A numeric RVU cell as JSON: a parsed number, or `Null` when blank/unparseable
/// (honest missing — never a fabricated 0.00; pin #5).
fn num_cell(cells: &[String], idx: usize) -> Value {
    cells
        .get(idx)
        .and_then(|c| c.trim().parse::<f64>().ok())
        .map(|n| json!(n))
        .unwrap_or(Value::Null)
}

fn text_cell(cells: &[String], idx: Option<usize>) -> Option<String> {
    let s = cells.get(idx?)?.trim();
    (!s.is_empty()).then(|| s.to_string())
}

/// The parsed PPRRVU corpus for one release.
#[derive(Debug)]
struct ParsedSchedule {
    /// `(dataset key, record)` pairs, key = `{hcpcs}` or `{hcpcs}:{modifier}`.
    rows: Vec<(String, Value)>,
    /// The release-wide conversion factor (first parseable CONV cell).
    conversion_factor: Option<f64>,
}

/// Parses PPRRVU CSV text into keyed fee-schedule records. Pure — golden-file
/// tested like `detect_releases`. Errors are loud drift reports (pins #2–#4).
fn parse_pprrvu(csv: &str, release: &str) -> Result<ParsedSchedule> {
    let mut lines = csv.lines();
    let cols = loop {
        let Some(line) = lines.next() else {
            return Err(Error::App(
                "PPRRVU layout drift: no header row containing an 'HCPCS' cell found — \
                 the pinned CSV layout changed"
                    .to_string(),
            ));
        };
        let cells = split_csv_line(line);
        if cells.iter().any(|c| c.trim().eq_ignore_ascii_case("HCPCS")) {
            break locate_columns(&cells).map_err(Error::App)?;
        }
    };

    let mut rows: Vec<(String, Value)> = Vec::new();
    let mut conversion_factor: Option<f64> = None;
    for line in lines {
        let cells = split_csv_line(line);
        let Some(hcpcs) = cells.get(cols.hcpcs).map(|c| c.trim().to_string()) else {
            continue;
        };
        if !looks_like_hcpcs(&hcpcs) {
            continue; // footnote/blank/continuation row — not a code row (pin #4).
        }
        let modifier = text_cell(&cells, cols.modifier);
        let key = match &modifier {
            Some(m) => format!("{hcpcs}:{m}"),
            None => hcpcs.clone(),
        };
        let conv = num_cell(&cells, cols.conv);
        if conversion_factor.is_none() {
            conversion_factor = conv.as_f64();
        }
        let record = json!({
            "release": release,
            "hcpcs": hcpcs,
            "modifier": modifier,
            "description": text_cell(&cells, cols.description),
            "status_code": text_cell(&cells, cols.status),
            "work_rvu": num_cell(&cells, cols.work),
            "pe_rvu_nonfac": num_cell(&cells, cols.pe_nonfac),
            "pe_rvu_fac": num_cell(&cells, cols.pe_fac),
            "mp_rvu": num_cell(&cells, cols.mp),
            "conversion_factor": conv,
        });
        rows.push((key, record));
    }
    if rows.is_empty() {
        return Err(Error::App(
            "PPRRVU layout drift: header row found but zero data rows parsed as HCPCS codes — \
             the pinned CSV layout changed"
                .to_string(),
        ));
    }
    Ok(ParsedSchedule {
        rows,
        conversion_factor,
    })
}

/// Picks the PPRRVU CSV entry out of the ZIP's entry names (pin #1), or None.
fn find_pprrvu_entry(names: &[String]) -> Option<String> {
    names
        .iter()
        .find(|n| {
            let file = n
                .rsplit(['/', '\\'])
                .next()
                .unwrap_or(n)
                .to_ascii_uppercase();
            file.starts_with("PPRRVU") && file.ends_with(".CSV")
        })
        .cloned()
}

/// Extracts `(entry_name, csv_text)` for the PPRRVU CSV from the release ZIP.
/// Loud drift errors when the archive is unreadable or has no PPRRVU CSV.
fn extract_pprrvu_csv(zip_bytes: &[u8]) -> Result<(String, String)> {
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes))
        .map_err(|e| Error::App(format!("RVU ZIP unreadable (not a ZIP, or corrupt): {e}")))?;
    let names: Vec<String> = archive.file_names().map(str::to_string).collect();
    let entry = find_pprrvu_entry(&names).ok_or_else(|| {
        Error::App(format!(
            "PPRRVU layout drift: no PPRRVU*.csv entry in the release ZIP — entries: {names:?}"
        ))
    })?;
    let mut file = archive
        .by_name(&entry)
        .map_err(|e| Error::App(format!("reading '{entry}' from RVU ZIP: {e}")))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| Error::App(format!("decompressing '{entry}' from RVU ZIP: {e}")))?;
    // The PPRRVU CSV is ASCII; lossy keeps any stray byte from failing the run.
    Ok((entry, String::from_utf8_lossy(&bytes).into_owned()))
}

/// The RVU component tuple a diff compares (description tags the movers list).
#[derive(Debug, Clone, PartialEq)]
struct RvuRow {
    work: Option<f64>,
    pe_nonfac: Option<f64>,
    pe_fac: Option<f64>,
    mp: Option<f64>,
    description: Option<String>,
}

impl RvuRow {
    fn of(v: &Value) -> Self {
        let f = |k: &str| v.get(k).and_then(Value::as_f64);
        Self {
            work: f("work_rvu"),
            pe_nonfac: f("pe_rvu_nonfac"),
            pe_fac: f("pe_rvu_fac"),
            mp: f("mp_rvu"),
            description: v
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
        }
    }

    /// Non-facility total RVU (work + non-fac PE + MP) — the standard headline
    /// number a conversion factor multiplies. None unless all three parse.
    fn total_nonfac(&self) -> Option<f64> {
        Some(self.work? + self.pe_nonfac? + self.mp?)
    }

    fn components_differ(&self, other: &Self) -> bool {
        self.work != other.work
            || self.pe_nonfac != other.pe_nonfac
            || self.pe_fac != other.pe_fac
            || self.mp != other.mp
    }
}

/// Release-over-release diff summary (pure): counts + bounded top movers by
/// absolute non-facility total-RVU delta. Cold start (empty `prev`) reports
/// everything as added with no movers.
fn diff_schedules(
    prev: &BTreeMap<String, RvuRow>,
    next: &BTreeMap<String, RvuRow>,
    release: &str,
    prev_release: Option<&str>,
    cf_before: Option<f64>,
    cf_after: Option<f64>,
) -> Value {
    let mut added = 0usize;
    let mut changed = 0usize;
    let mut unchanged = 0usize;
    let mut movers: Vec<(f64, Value)> = Vec::new();
    for (key, new_row) in next {
        match prev.get(key) {
            None => added += 1,
            Some(old_row) if old_row.components_differ(new_row) => {
                changed += 1;
                if let (Some(before), Some(after)) =
                    (old_row.total_nonfac(), new_row.total_nonfac())
                {
                    let delta = after - before;
                    if delta != 0.0 {
                        movers.push((
                            delta.abs(),
                            json!({
                                "key": key,
                                "description": new_row.description,
                                "total_rvu_before": before,
                                "total_rvu_after": after,
                                "delta_total_rvu": delta,
                            }),
                        ));
                    }
                }
            }
            Some(_) => unchanged += 1,
        }
    }
    let removed_keys: Vec<&String> = prev.keys().filter(|k| !next.contains_key(*k)).collect();
    movers.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    movers.truncate(TOP_MOVERS_CAP);
    json!({
        "release": release,
        "prev_release": prev_release,
        "codes_total": next.len(),
        "added": added,
        "removed": removed_keys.len(),
        "changed": changed,
        "unchanged": unchanged,
        "conversion_factor": { "before": cf_before, "after": cf_after },
        "top_movers": movers.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
        "removed_sample": removed_keys
            .into_iter()
            .take(REMOVED_SAMPLE_CAP)
            .collect::<Vec<_>>(),
    })
}

/// sha256 (hex) of a byte body — the `Provenance.artifact_sha` convention.
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// Downloads, extracts, parses and stores one release's PPRRVU corpus, returning
/// the run-output `parse` block. Any error here is reported by the caller
/// WITHOUT failing the watcher run.
async fn ingest_release(ctx: &AppContext, release: Release) -> Result<Value> {
    let mut req = HttpRequest::get(release.zip_url());
    req.max_body_bytes = Some(MAX_ZIP_BYTES);
    // The ZIP is binary and release-immutable; the text response cache is
    // bypassed by fetch_bytes anyway — no_cache documents the intent.
    req.no_cache = true;
    let zip_bytes = ctx.engines.http.fetch_bytes(req).await?;

    let (entry, csv) = extract_pprrvu_csv(&zip_bytes)?;
    let release_id = release.id();
    ctx.save_artifact(&format!("pprrvu-{release_id}.csv"), csv.as_bytes())
        .await?;
    // Provenance (M12): every row below is parsed from THIS CSV, archived beside
    // the job — so both the URL it came from and the content hash of the stored
    // body are known facts, not guesses. `rules_hash` stays Null: the column
    // mapping is pinned Rust code, not a registered RuleSet.
    let artifact_sha = sha256_hex(csv.as_bytes());
    let prov = Provenance {
        source_url: Some(release.zip_url()),
        artifact_sha: Some(artifact_sha.clone()),
        ..Provenance::default()
    };
    let parsed = parse_pprrvu(&csv, &release_id)?;

    // Prior corpus BEFORE the upsert overwrites it — the diff's "before" side.
    let prior = ctx
        .datasets
        .list(&ctx.app, "fee_schedule", PRIOR_LIST_LIMIT)
        .await?;
    let prev_release = prior
        .iter()
        .find_map(|r| {
            r.data
                .get("release")
                .and_then(Value::as_str)
                .map(String::from)
        })
        .filter(|prev| *prev != release_id);
    let cf_before = prior
        .iter()
        .find_map(|r| r.data.get("conversion_factor").and_then(Value::as_f64));
    let prev_map: BTreeMap<String, RvuRow> = prior
        .iter()
        .map(|r| (r.key.clone(), RvuRow::of(&r.data)))
        .collect();
    let next_map: BTreeMap<String, RvuRow> = parsed
        .rows
        .iter()
        .map(|(k, v)| (k.clone(), RvuRow::of(v)))
        .collect();

    let summary = ctx
        .upsert_many_with_provenance("fee_schedule", &parsed.rows, prov.clone())
        .await?;
    let diff = diff_schedules(
        &prev_map,
        &next_map,
        &release_id,
        prev_release.as_deref(),
        cf_before,
        parsed.conversion_factor,
    );
    // The diff is computed from the new parse AND the previously stored corpus,
    // so it carries the same artifact stamp as the rows it summarizes.
    ctx.upsert_with_provenance("fee_schedule_changes", &release_id, &diff, prov)
        .await?;

    Ok(json!({
        "status": "ok",
        "release": release_id,
        "csv_entry": entry,
        "artifact_sha": artifact_sha,
        "rows": parsed.rows.len(),
        "conversion_factor": parsed.conversion_factor,
        "upsert": { "new": summary.new.len(), "changed": summary.changed.len(),
                    "unchanged": summary.unchanged },
        "changes": diff,
    }))
}

#[async_trait]
impl ScrapeApp for CmsFeeSchedule {
    fn name(&self) -> &'static str {
        "cms-fee-schedule"
    }

    fn description(&self) -> &'static str {
        "Watches CMS for the latest Physician Fee Schedule (PFS) RVU release, and \
         on a new release downloads the ZIP, parses the PPRRVU CSV and owns the \
         corpus: per-HCPCS work/PE/MP RVUs + conversion factor in the \
         'fee_schedule' dataset (key {hcpcs} or {hcpcs}:{modifier}) and a \
         release-over-release diff (counts + top movers) in \
         'fee_schedule_changes'. Self-baselines off the last release it stored; \
         pass \"known_release\" to override, \"force\": true to re-download and \
         re-parse the latest release. Params: {\"schedule\":\"pfs\", \
         \"known_release\": null, \"force\": false}"
    }

    fn schedule(&self) -> Option<&'static str> {
        // 06:00:00 on the 1st of each month (sec min hour day month weekday).
        // CMS PFS releases are quarterly; a monthly check is a cheap, ample cadence.
        Some("0 0 6 1 * *")
    }

    fn default_params(&self) -> Value {
        // No hardcoded `known_release`: the watcher self-baselines off the last
        // release it stored (a stale literal would keep `is_newer_than_known`
        // permanently lit). A caller who knows what Counterbill has baked can still
        // pass `known_release` as an explicit override.
        json!({ "schedule": "pfs", "force": false })
    }

    fn manifest(&self) -> AppManifest {
        AppManifest {
            params_schema: Some(json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "properties": {
                    "schedule": {
                        "type": "string",
                        "enum": ["pfs"],
                        "description": "Fee schedule to watch. Only \"pfs\" (Physician Fee Schedule RVU files) is supported today; anything else is a run error, so the enum is the real contract."
                    },
                    "known_release": {
                        "type": ["string", "null"],
                        "description": "OPTIONAL baseline release id (e.g. \"RVU26A\") — what the consumer currently has baked. Omit to self-baseline off the release stored last run."
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Re-download and re-parse the latest release even when it is unchanged since last run (backfill / re-parse). Default false."
                    }
                },
                "additionalProperties": true
            })),
            examples: vec![
                ManifestExample {
                    description: "Monthly freshness check: detect the latest RVU release, self-baselining off the last one stored (the scheduled default)",
                    params: json!({ "schedule": "pfs", "force": false }),
                },
                ManifestExample {
                    description: "Re-parse the current release against an explicit baked baseline (backfill the fee_schedule corpus)",
                    params: json!({ "schedule": "pfs", "known_release": "RVU26A", "force": true }),
                },
            ],
            output_shape: Some(
                "{schedule, latest_release, year, quarter, quarter_num, zip_url, source_url, \
                 index_url, known_release, baseline, baseline_source, is_newer_than_known, \
                 change_since_last_run, is_fresh, releases_found[], ingest: {release, zip_url, \
                 source_url}, parse: {status: ok|skipped|error, rows?, conversion_factor?, \
                 artifact_sha?, changes?, error?}, ingest_hint} — release watch in `releases`, \
                 per-HCPCS RVUs in `fee_schedule`, release diffs in `fee_schedule_changes`",
            ),
            // Only the free http engine: an index page and a direct ZIP download.
            cost_class: CostClass::Free,
        }
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let schedule = ctx
            .params
            .get("schedule")
            .and_then(Value::as_str)
            .unwrap_or("pfs");
        if schedule != "pfs" {
            return Err(Error::App(format!(
                "unsupported schedule '{schedule}' (only 'pfs' is supported today)"
            )));
        }

        let response = ctx
            .engines
            .http
            .fetch(HttpRequest::get(PFS_INDEX_URL))
            .await?;
        if !response.is_success() {
            return Err(Error::App(format!(
                "CMS PFS index returned status {}",
                response.status
            )));
        }
        ctx.save_artifact("pfs-index.html", response.body.as_bytes())
            .await?;

        let releases = detect_releases(&response.body);
        let latest = latest(&releases).ok_or_else(|| {
            Error::App(
                "no RVU release tokens found on the CMS PFS index — the page structure \
                 may have changed (consider the browser engine)"
                    .to_string(),
            )
        })?;

        // Self-baseline: read the release we stored last run BEFORE the upsert
        // below overwrites it. Baseline precedence: explicit `known_release` param
        // (what Counterbill has baked) > the stored release (self-baselining across
        // scheduled runs) > none (cold start). This clears the "permanently stale"
        // alarm — once RVU26B is stored, later runs baseline off it and stop
        // reporting `is_newer_than_known: true` until CMS actually ships RVU26C.
        let stored_prev = ctx
            .datasets
            .get(&ctx.app, "releases", schedule)
            .await?
            .and_then(|r| {
                r.data
                    .get("latest_release")
                    .and_then(Value::as_str)
                    .map(String::from)
            });
        let param_known = ctx
            .params
            .get("known_release")
            .and_then(Value::as_str)
            .map(String::from);
        let (baseline, baseline_source) = match (&param_known, &stored_prev) {
            (Some(k), _) => (Some(k.clone()), "param"),
            (None, Some(s)) => (Some(s.clone()), "stored"),
            (None, None) => (None, "none"),
        };

        // Change detection across scheduled runs: keyed by `schedule`, so a run
        // reports `new`/`changed` only when CMS actually published a newer release.
        let record = json!({
            "latest_release": latest.id(),
            "year": latest.year,
            "quarter": latest.quarter.to_string(),
            "zip_url": latest.zip_url(),
            "source_url": latest.source_url(),
        });
        // The release record is derived from exactly one fetched page — the PFS
        // index — so that URL is the honest stamp (the ZIP it points at has not
        // been fetched at this point).
        let change: ChangeKind = ctx
            .upsert_with_provenance(
                "releases",
                schedule,
                &record,
                Provenance {
                    source_url: Some(PFS_INDEX_URL.to_string()),
                    ..Provenance::default()
                },
            )
            .await?;

        // Is the detected latest newer than the effective baseline? A cold start
        // (no baseline) treats any detected release as actionable.
        let is_newer = match baseline.as_deref().and_then(parse_release) {
            Some(k) => latest.ord_key() > k.ord_key(),
            None => true,
        };

        // M32: parse the release corpus — only on a genuinely new/changed release
        // (or `force`). A failure here is loud but NEVER fails the watcher run:
        // the release record above is already stored and the freshness signal
        // stands on its own.
        let force = ctx
            .params
            .get("force")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let parse_block = if change.is_fresh() || force {
            match ingest_release(&ctx, latest).await {
                Ok(block) => block,
                Err(e) => {
                    tracing::error!(
                        release = %latest.id(),
                        error = %e,
                        "PPRRVU ingest failed — watcher result stands, corpus NOT updated"
                    );
                    json!({ "status": "error", "release": latest.id(), "error": e.to_string() })
                }
            }
        } else {
            json!({
                "status": "skipped",
                "reason": "release unchanged since last run (pass \"force\": true to re-parse)",
            })
        };

        Ok(json!({
            "schedule": schedule,
            "latest_release": latest.id(),
            "year": latest.year,
            "quarter": latest.quarter.to_string(),
            "quarter_num": latest.quarter_num(),
            "zip_url": latest.zip_url(),
            "source_url": latest.source_url(),
            "index_url": PFS_INDEX_URL,
            "known_release": param_known,          // the explicit override, if any
            "baseline": baseline,                  // the release we compared against
            "baseline_source": baseline_source,    // "param" | "stored" | "none"
            "is_newer_than_known": is_newer,
            "change_since_last_run": change,      // "new" | "changed" | "unchanged"
            "is_fresh": change.is_fresh(),         // new or changed since last run
            "releases_found": releases.iter().map(Release::id).collect::<Vec<_>>(),
            // Structured ingest target so a `dataset` trigger (on_change=fresh) can
            // fan out to an ingest job reading these keys from `_trigger`, instead
            // of a human reading the prose hint below.
            "ingest": {
                "release": latest.id(),
                "zip_url": latest.zip_url(),
                "source_url": latest.source_url(),
            },
            // M32 corpus parse outcome: "ok" (rows + diff stored) | "skipped" |
            // "error" (loud; watcher result unaffected).
            "parse": parse_block,
            "ingest_hint": format!(
                "If newer: point scripts/ingest-cms-pfs.mjs at {} (update its ZIP_URL + RELEASE, \
                 or pass --zip a download of {}) and run `npm run ingest:pfs`.",
                latest.id(),
                latest.zip_url()
            ),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A realistic slice: releases appear in both hrefs (lowercase) and link text
    // (uppercase), with a duplicate and an older-year entry to exercise dedup+sort.
    const SAMPLE: &str = r#"
        <ul class="rvu-list">
          <li><a href="/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu26a">RVU26A (January 2026)</a></li>
          <li><a href="/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu26b">RVU26B (April 2026)</a></li>
          <li><a href="/medicare/payment/fee-schedules/physician/pfs-relative-value-files/rvu25d">RVU25D (October 2025)</a></li>
        </ul>
    "#;

    #[test]
    fn detects_dedupes_and_sorts_releases() {
        let ids: Vec<String> = detect_releases(SAMPLE).iter().map(Release::id).collect();
        // href + text mention the same release; deduped, sorted oldest→newest.
        assert_eq!(ids, vec!["RVU25D", "RVU26A", "RVU26B"]);
    }

    #[test]
    fn latest_prefers_newest_year_then_quarter() {
        assert_eq!(latest(&detect_releases(SAMPLE)).unwrap().id(), "RVU26B");
    }

    #[test]
    fn detects_nothing_in_unrelated_html() {
        assert!(detect_releases("<p>no releases here</p>").is_empty());
    }

    #[test]
    fn parse_release_validates_shape_and_quarter() {
        assert_eq!(parse_release("rvu26a").unwrap().id(), "RVU26A");
        assert_eq!(parse_release("RVU26D").unwrap().quarter, 'D');
        assert_eq!(parse_release(" RVU25C ").unwrap().year, 2025);
        assert!(parse_release("RVU26E").is_none()); // quarter out of A–D
        assert!(parse_release("RVU2A").is_none()); // too short
        assert!(parse_release("nope").is_none());
    }

    #[test]
    fn urls_and_ordering_match_cms_conventions() {
        let b = parse_release("RVU26B").unwrap();
        assert_eq!(b.zip_url(), "https://www.cms.gov/files/zip/rvu26b.zip");
        assert!(b.source_url().ends_with("/rvu26b"));
        assert_eq!(b.quarter_num(), 2);

        let a = parse_release("RVU26A").unwrap();
        let y = parse_release("RVU25D").unwrap();
        assert!(b.ord_key() > a.ord_key()); // later quarter, same year
        assert!(a.ord_key() > y.ord_key()); // newer year beats later quarter
    }

    // ── M32: CSV / header / parse ───────────────────────────────────────────

    #[test]
    fn csv_line_splitting_handles_quotes_and_commas() {
        assert_eq!(split_csv_line("a,b,c"), vec!["a", "b", "c"]);
        assert_eq!(
            split_csv_line(r#"99213,,"Office visit, est patient",0.97"#),
            vec!["99213", "", "Office visit, est patient", "0.97"]
        );
        // Doubled quote inside a quoted cell is a literal quote.
        assert_eq!(
            split_csv_line(r#""say ""hi""",x"#),
            vec![r#"say "hi""#, "x"]
        );
        // Trailing empty cell survives.
        assert_eq!(split_csv_line("a,"), vec!["a", ""]);
    }

    /// A realistic PPRRVU header row (2026-shape column names, pin #3).
    fn pprrvu_header() -> Vec<String> {
        [
            "HCPCS",
            "MOD",
            "DESCRIPTION",
            "STATUS CODE",
            "NOT USED FOR MEDICARE PAYMENT",
            "WORK RVU",
            "NON-FAC PE RVU",
            "NON-FAC NA INDICATOR",
            "FACILITY PE RVU",
            "FACILITY NA INDICATOR",
            "MP RVU",
            "NON-FACILITY TOTAL",
            "FACILITY TOTAL",
            "CONV FACTOR",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    #[test]
    fn header_locating_maps_the_pinned_columns() {
        let cols = locate_columns(&pprrvu_header()).expect("pinned header resolves");
        assert_eq!(cols.hcpcs, 0);
        assert_eq!(cols.modifier, Some(1));
        assert_eq!(cols.description, Some(2));
        assert_eq!(cols.status, Some(3));
        assert_eq!(cols.work, 5);
        assert_eq!(cols.pe_nonfac, 6);
        assert_eq!(
            cols.pe_fac, 8,
            "facility PE must not match the NON-FAC column"
        );
        assert_eq!(cols.mp, 10);
        assert_eq!(cols.conv, 13);
    }

    #[test]
    fn header_drift_errors_loudly_naming_the_missing_column() {
        let mut header = pprrvu_header();
        header[5] = "WORK UNITS".to_string(); // work RVU column renamed → drift.
        let err = locate_columns(&header).unwrap_err();
        assert!(err.contains("drift"), "loud drift wording: {err}");
        assert!(err.contains("work RVU"), "names the missing column: {err}");
    }

    /// A miniature PPRRVU CSV: title preamble, header, code rows (one with a
    /// modifier, one with an unparseable RVU cell), and a footnote row.
    const PPRRVU_SAMPLE: &str = "\
2026 NATIONAL PHYSICIAN FEE SCHEDULE RELATIVE VALUE FILE,,,,,,,,,,,,,\n\
,,,,,,,,,,,,,\n\
HCPCS,MOD,DESCRIPTION,STATUS CODE,NOT USED FOR MEDICARE PAYMENT,WORK RVU,NON-FAC PE RVU,NON-FAC NA INDICATOR,FACILITY PE RVU,FACILITY NA INDICATOR,MP RVU,NON-FACILITY TOTAL,FACILITY TOTAL,CONV FACTOR\n\
99213,,\"Office o/p est low 20-29 min\",A,,1.30,1.26,,0.55,,0.10,2.66,1.95,32.3465\n\
99213,25,\"Office o/p est low w/ modifier\",A,,1.30,1.26,,0.55,,0.10,2.66,1.95,32.3465\n\
G0008,,\"Admin influenza virus vac\",X,,0.00,0.61,,NA,,0.01,0.62,0.62,32.3465\n\
\"NOTE: RVUs are not payment amounts\",,,,,,,,,,,,,\n";

    #[test]
    fn parse_pprrvu_extracts_keyed_rvu_rows() {
        let parsed = parse_pprrvu(PPRRVU_SAMPLE, "RVU26B").expect("sample parses");
        assert_eq!(parsed.rows.len(), 3, "footnote/preamble rows skipped");
        assert_eq!(parsed.conversion_factor, Some(32.3465));

        let (key, row) = &parsed.rows[0];
        assert_eq!(key, "99213", "no modifier → bare HCPCS key");
        assert_eq!(row["work_rvu"], json!(1.30));
        assert_eq!(row["pe_rvu_nonfac"], json!(1.26));
        assert_eq!(row["pe_rvu_fac"], json!(0.55));
        assert_eq!(row["mp_rvu"], json!(0.10));
        assert_eq!(row["conversion_factor"], json!(32.3465));
        assert_eq!(row["release"], json!("RVU26B"));
        assert_eq!(row["status_code"], json!("A"));

        let (key, _) = &parsed.rows[1];
        assert_eq!(key, "99213:25", "modifier joins the key");

        // 'NA' facility PE is honest Null, never a fabricated 0.00 (pin #5).
        let (_, g0008) = &parsed.rows[2];
        assert_eq!(g0008["pe_rvu_fac"], Value::Null);
        assert_eq!(g0008["work_rvu"], json!(0.0), "a real 0.00 stays 0.00");
    }

    #[test]
    fn parse_pprrvu_without_header_row_is_loud_drift() {
        let err = parse_pprrvu("just,some,cells\n1,2,3\n", "RVU26B").unwrap_err();
        assert!(err.to_string().contains("drift"), "{err}");
        // Header present but nothing parsed as a code row is also drift.
        let header_only = PPRRVU_SAMPLE.lines().take(3).collect::<Vec<_>>().join("\n");
        let err = parse_pprrvu(&header_only, "RVU26B").unwrap_err();
        assert!(err.to_string().contains("zero data rows"), "{err}");
    }

    #[test]
    fn pprrvu_zip_entry_is_found_case_insensitively() {
        let names = vec![
            "RVU26B/README.txt".to_string(),
            "RVU26B/GPCI2026.csv".to_string(),
            "RVU26B/PPRRVU26_APR.csv".to_string(),
        ];
        assert_eq!(
            find_pprrvu_entry(&names).as_deref(),
            Some("RVU26B/PPRRVU26_APR.csv")
        );
        let lower = vec!["pprrvu26_apr.csv".to_string()];
        assert!(find_pprrvu_entry(&lower).is_some());
        assert!(find_pprrvu_entry(&["OPPSCAP.csv".to_string()]).is_none());
    }

    #[test]
    fn extract_pprrvu_csv_round_trips_a_real_zip() {
        use std::io::Write as _;
        // Build an in-memory ZIP with a decoy entry + the PPRRVU CSV.
        let mut cursor = std::io::Cursor::new(Vec::new());
        {
            let mut writer = zip::ZipWriter::new(&mut cursor);
            let opts = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Deflated);
            writer.start_file("README.txt", opts).unwrap();
            writer.write_all(b"decoy").unwrap();
            writer.start_file("PPRRVU26_APR.csv", opts).unwrap();
            writer.write_all(PPRRVU_SAMPLE.as_bytes()).unwrap();
            writer.finish().unwrap();
        }
        let (entry, csv) = extract_pprrvu_csv(cursor.get_ref()).expect("zip extracts");
        assert_eq!(entry, "PPRRVU26_APR.csv");
        assert_eq!(csv, PPRRVU_SAMPLE);
        // Garbage bytes are a loud typed error, not a panic.
        let err = extract_pprrvu_csv(b"not a zip").unwrap_err();
        assert!(err.to_string().contains("ZIP"), "{err}");
    }

    #[test]
    fn artifact_sha_is_a_stable_sha256_of_the_archived_csv() {
        // The stamp must be the plain sha256 hex of the exact bytes written as
        // the artifact — a replay tool has to be able to re-derive it.
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let a = sha256_hex(PPRRVU_SAMPLE.as_bytes());
        assert_eq!(a.len(), 64);
        assert_eq!(a, sha256_hex(PPRRVU_SAMPLE.as_bytes()), "deterministic");
        assert_ne!(a, sha256_hex(format!("{PPRRVU_SAMPLE} ").as_bytes()));
    }

    // ── M32: release diff ───────────────────────────────────────────────────

    fn rvu(work: f64, pe_nonfac: f64, pe_fac: f64, mp: f64, desc: &str) -> RvuRow {
        RvuRow {
            work: Some(work),
            pe_nonfac: Some(pe_nonfac),
            pe_fac: Some(pe_fac),
            mp: Some(mp),
            description: Some(desc.to_string()),
        }
    }

    #[test]
    fn diff_counts_added_removed_changed_and_ranks_movers() {
        let prev: BTreeMap<String, RvuRow> = [
            ("99213".to_string(), rvu(1.30, 1.26, 0.55, 0.10, "visit")),
            ("99214".to_string(), rvu(1.92, 1.66, 0.80, 0.14, "visit40")),
            ("GONE1".to_string(), rvu(0.5, 0.5, 0.2, 0.05, "retired")),
        ]
        .into();
        let next: BTreeMap<String, RvuRow> = [
            // Small move: total 2.66 → 2.71 (+0.05).
            ("99213".to_string(), rvu(1.35, 1.26, 0.55, 0.10, "visit")),
            // Big move: total 3.72 → 4.72 (+1.00) — must rank first.
            ("99214".to_string(), rvu(2.92, 1.66, 0.80, 0.14, "visit40")),
            ("NEW01".to_string(), rvu(1.0, 1.0, 0.5, 0.1, "brand new")),
        ]
        .into();
        let diff = diff_schedules(
            &prev,
            &next,
            "RVU26B",
            Some("RVU26A"),
            Some(32.74),
            Some(32.35),
        );
        assert_eq!(diff["codes_total"], json!(3));
        assert_eq!(diff["added"], json!(1));
        assert_eq!(diff["removed"], json!(1));
        assert_eq!(diff["changed"], json!(2));
        assert_eq!(diff["unchanged"], json!(0));
        assert_eq!(diff["prev_release"], json!("RVU26A"));
        assert_eq!(diff["conversion_factor"]["before"], json!(32.74));
        assert_eq!(diff["conversion_factor"]["after"], json!(32.35));
        let movers = diff["top_movers"].as_array().unwrap();
        assert_eq!(movers.len(), 2);
        assert_eq!(movers[0]["key"], json!("99214"), "largest |delta| first");
        let delta = movers[0]["delta_total_rvu"].as_f64().unwrap();
        assert!((delta - 1.00).abs() < 1e-9, "delta ≈ 1.00, got {delta}");
        assert_eq!(diff["removed_sample"], json!(["GONE1"]));
    }

    #[test]
    fn diff_cold_start_reports_everything_added_with_no_movers() {
        let prev = BTreeMap::new();
        let next: BTreeMap<String, RvuRow> =
            [("99213".to_string(), rvu(1.3, 1.26, 0.55, 0.1, "visit"))].into();
        let diff = diff_schedules(&prev, &next, "RVU26B", None, None, Some(32.35));
        assert_eq!(diff["added"], json!(1));
        assert_eq!(diff["changed"], json!(0));
        assert_eq!(diff["removed"], json!(0));
        assert_eq!(diff["prev_release"], Value::Null);
        assert!(diff["top_movers"].as_array().unwrap().is_empty());
    }

    #[test]
    fn diff_top_movers_list_is_bounded() {
        let mut prev = BTreeMap::new();
        let mut next = BTreeMap::new();
        for i in 0..(TOP_MOVERS_CAP + 15) {
            let key = format!("A{i:04}");
            prev.insert(key.clone(), rvu(1.0, 1.0, 0.5, 0.1, "x"));
            // Every code moves by a distinct delta.
            next.insert(key, rvu(1.0 + (i as f64) * 0.01 + 0.01, 1.0, 0.5, 0.1, "x"));
        }
        let diff = diff_schedules(&prev, &next, "RVU26B", Some("RVU26A"), None, None);
        assert_eq!(diff["top_movers"].as_array().unwrap().len(), TOP_MOVERS_CAP);
        assert_eq!(diff["changed"], json!(TOP_MOVERS_CAP + 15));
    }

    #[test]
    fn unparseable_rvu_components_never_enter_the_movers_list() {
        // A row whose MP is Null on one side: components differ (counted as
        // changed) but no fabricated total can rank it as a mover.
        let mut incomplete = rvu(1.0, 1.0, 0.5, 0.1, "x");
        incomplete.mp = None;
        let prev: BTreeMap<String, RvuRow> = [("99213".to_string(), incomplete)].into();
        let next: BTreeMap<String, RvuRow> =
            [("99213".to_string(), rvu(2.0, 1.0, 0.5, 0.1, "x"))].into();
        let diff = diff_schedules(&prev, &next, "RVU26B", Some("RVU26A"), None, None);
        assert_eq!(diff["changed"], json!(1));
        assert!(diff["top_movers"].as_array().unwrap().is_empty());
    }
}
