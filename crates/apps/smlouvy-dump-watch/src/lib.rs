//! Pumper app: Registr smluv (Czech contract register) dump-index watcher.
//!
//! `data.smlouvy.gov.cz/index.xml` is the authoritative listing of every monthly
//! bulk dump the Ministry publishes — one `<dump>` per (year, month), each with a
//! sha1 hash, byte size, generation timestamp, and the dump's download URL. The
//! Ministry re-generates a month's dump when late contracts land, which changes
//! its hash and size while its (year, month) stays the same.
//!
//! This app parses that index into one record per dump, keyed by the dump URL,
//! and syncs it as a FULL SNAPSHOT — so a consumer (e.g. tender-radar's Registr
//! smluv ingest) learns precisely **which dump is new or re-generated**, with its
//! sha1 and size, instead of the generic `watch` app's "the index page changed"
//! fingerprint. A `dataset` trigger on `on_change=fresh` can fan a re-download of
//! exactly the changed dumps.
//!
//! It deliberately does NOT download the (large, ~100 MB) dumps — the heavy fetch
//! + parse belongs in the consuming app; this only answers "what dumps exist and
//! which changed?".
//!
//! Params: `{ "index_url": "https://data.smlouvy.gov.cz/index.xml", "year_from": null }`
//!   · `index_url` — override the index location (default is the production URL).
//!   · `year_from` — OPTIONAL: keep only dumps whose `rok` ≥ this, so a consumer
//!                   that only cares about recent months doesn't churn on the full
//!                   2016→now history (~100+ dumps). Omitted → all dumps.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use serde_json::{json, Value};

pub struct SmlouvyDumpWatch;

const DEFAULT_INDEX_URL: &str = "https://data.smlouvy.gov.cz/index.xml";

/// One `<dump>` entry parsed from the index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Dump {
    year: u32,
    month: u32,
    /// sha1 hex of the dump file (the `hashDumpu` element text).
    hash: String,
    /// Dump size in bytes (`velikostDumpu`).
    size_bytes: u64,
    /// Generation timestamp as published (`casGenerovani`, RFC-3339-ish).
    generated_at: String,
    /// Absolute download URL (`odkaz`) — the stable natural key.
    url: String,
}

impl Dump {
    /// The record persisted for this dump.
    fn record(&self) -> Value {
        json!({
            "year": self.year,
            "month": self.month,
            "period": format!("{:04}-{:02}", self.year, self.month),
            "hash": self.hash,
            "hash_algo": "sha1",
            "size_bytes": self.size_bytes,
            "generated_at": self.generated_at,
            "url": self.url,
        })
    }
}

/// Text of the first `<tag>…</tag>` in `block` (attributes on the open tag are
/// tolerated, e.g. `<hashDumpu algoritmus="sha1">…`). Returns the trimmed inner
/// text, or `None` if the element is absent. Namespace-prefix agnostic because the
/// index uses a default namespace (no element prefixes).
fn tag_text<'a>(block: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}");
    let start = block.find(&open)?;
    // Skip to the end of the (possibly attribute-bearing) open tag.
    let after_open = start + block[start..].find('>')? + 1;
    let close = format!("</{tag}>");
    let end = block[after_open..].find(&close)? + after_open;
    Some(block[after_open..end].trim())
}

/// Parse the dump index XML into dumps, in document order. Pure + unit-tested: a
/// flat, stable government schema, so a scoped tag scan beats pulling in an XML
/// dependency. Entries missing a URL or an unparseable year/month are skipped
/// (defensive against a partial/garbled feed) rather than failing the whole run.
fn parse_dumps(xml: &str) -> Vec<Dump> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while let Some(rel) = xml[i..].find("<dump>") {
        let start = i + rel + "<dump>".len();
        let Some(rel_end) = xml[start..].find("</dump>") else {
            break;
        };
        let block = &xml[start..start + rel_end];
        i = start + rel_end + "</dump>".len();

        let url = match tag_text(block, "odkaz") {
            Some(u) if !u.is_empty() => u.to_string(),
            _ => continue,
        };
        let (Some(year), Some(month)) = (
            tag_text(block, "rok").and_then(|s| s.parse::<u32>().ok()),
            tag_text(block, "mesic").and_then(|s| s.parse::<u32>().ok()),
        ) else {
            continue;
        };
        out.push(Dump {
            year,
            month,
            hash: tag_text(block, "hashDumpu").unwrap_or("").to_string(),
            size_bytes: tag_text(block, "velikostDumpu")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0),
            generated_at: tag_text(block, "casGenerovani").unwrap_or("").to_string(),
            url,
        });
    }
    out
}

#[async_trait]
impl ScrapeApp for SmlouvyDumpWatch {
    fn name(&self) -> &'static str {
        "smlouvy-dump-watch"
    }

    fn description(&self) -> &'static str {
        "Watches the Registr smluv (Czech contract register) bulk-dump index at \
         data.smlouvy.gov.cz/index.xml. Parses it into one change-detected record \
         per monthly dump (year, month, sha1, size, download URL), keyed by dump \
         URL and synced as a full snapshot — so a consumer learns exactly which \
         dump is new or re-generated (hash change) and can fan out a targeted \
         re-download via a dataset trigger. Does not download the (~100 MB) dumps \
         themselves. Params: {\"index_url\": \"…/index.xml\", \"year_from\": null \
         (optional: keep only dumps with rok >= year_from)}"
    }

    fn schedule(&self) -> Option<&'static str> {
        // 05:30:00 daily. The Ministry regenerates dumps overnight; a daily check
        // is ample and cheap (one small XML fetch).
        Some("0 30 5 * * *")
    }

    fn default_params(&self) -> Value {
        json!({ "index_url": DEFAULT_INDEX_URL })
    }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let index_url = ctx
            .params
            .get("index_url")
            .and_then(Value::as_str)
            .unwrap_or(DEFAULT_INDEX_URL)
            .to_string();
        let year_from = ctx
            .params
            .get("year_from")
            .and_then(Value::as_u64)
            .map(|y| y as u32);

        let response = ctx.engines.http.fetch(HttpRequest::get(&index_url)).await?;
        if !response.is_success() {
            return Err(Error::App(format!(
                "Registr smluv index {index_url} returned status {}",
                response.status
            )));
        }
        ctx.save_artifact("index.xml", response.body.as_bytes())
            .await?;

        let mut dumps = parse_dumps(&response.body);
        if dumps.is_empty() {
            return Err(Error::App(format!(
                "no <dump> entries parsed from {index_url} — the feed may be empty or its \
                 schema changed"
            )));
        }
        let total_parsed = dumps.len();
        if let Some(y) = year_from {
            dumps.retain(|d| d.year >= y);
        }

        // Full snapshot: the index IS the complete current listing, so a dump that
        // vanishes (the Ministry retiring a month) is a real `removed`. Keyed by the
        // dump URL — a re-generated month keeps its URL and surfaces as `changed`
        // because its hash/size differ.
        let items: Vec<(String, Value)> =
            dumps.iter().map(|d| (d.url.clone(), d.record())).collect();
        let summary = ctx.sync_many("dumps", &items).await?;

        // The freshly-changed dumps are the actionable ingest targets — a dataset
        // trigger reads these keys from `_trigger` and re-downloads exactly them.
        let fresh_urls: Vec<&str> = summary.fresh_keys().map(String::as_str).collect();
        let newest = dumps.iter().max_by_key(|d| (d.year, d.month));

        Ok(json!({
            "index_url": index_url,
            "dumps_in_index": total_parsed,
            "dumps_tracked": dumps.len(),
            "year_from": year_from,
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "removed": summary.removed.len(),
            "fresh_dumps": fresh_urls,
            "newest_period": newest.map(|d| format!("{:04}-{:02}", d.year, d.month)),
            "newest_url": newest.map(|d| d.url.clone()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A realistic slice of the real feed: default namespace, hashDumpu with an
    // attribute, two months, one with a later generation time.
    const SAMPLE: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<index xmlns="http://portal.gov.cz/rejstriky/ISRS/1.2/">
  <dump>
    <mesic>6</mesic><rok>2026</rok>
    <hashDumpu algoritmus="sha1">aaaa1111bbbb2222cccc3333dddd4444eeee5555</hashDumpu>
    <velikostDumpu>84123456</velikostDumpu>
    <casGenerovani>2026-07-01T00:11:51+02:00</casGenerovani>
    <dokoncenyMesic>1</dokoncenyMesic>
    <odkaz>https://data.smlouvy.gov.cz/dump_2026_06.xml</odkaz>
  </dump>
  <dump>
    <mesic>1</mesic><rok>2017</rok>
    <hashDumpu algoritmus="sha1">66dc71395aaf41aa8563da9c895d678c7b3466b4</hashDumpu>
    <velikostDumpu>72499846</velikostDumpu>
    <casGenerovani>2026-04-22T00:07:04+02:00</casGenerovani>
    <dokoncenyMesic>1</dokoncenyMesic>
    <odkaz>https://data.smlouvy.gov.cz/dump_2017_01.xml</odkaz>
  </dump>
</index>"#;

    #[test]
    fn parses_every_dump_with_fields() {
        let dumps = parse_dumps(SAMPLE);
        assert_eq!(dumps.len(), 2);
        let d = &dumps[0];
        assert_eq!((d.year, d.month), (2026, 6));
        assert_eq!(d.hash, "aaaa1111bbbb2222cccc3333dddd4444eeee5555");
        assert_eq!(d.size_bytes, 84123456);
        assert_eq!(d.url, "https://data.smlouvy.gov.cz/dump_2026_06.xml");
        assert_eq!(d.generated_at, "2026-07-01T00:11:51+02:00");
    }

    #[test]
    fn record_shape_carries_period_and_key_fields() {
        let rec = parse_dumps(SAMPLE)[0].record();
        assert_eq!(rec["period"], "2026-06");
        assert_eq!(rec["hash_algo"], "sha1");
        assert_eq!(rec["size_bytes"], 84123456);
        assert_eq!(rec["url"], "https://data.smlouvy.gov.cz/dump_2026_06.xml");
    }

    #[test]
    fn tag_text_tolerates_attributes_and_missing_tags() {
        let block = r#"<hashDumpu algoritmus="sha1">abc123</hashDumpu><rok>2026</rok>"#;
        assert_eq!(tag_text(block, "hashDumpu"), Some("abc123"));
        assert_eq!(tag_text(block, "rok"), Some("2026"));
        assert_eq!(tag_text(block, "velikostDumpu"), None);
    }

    #[test]
    fn skips_entries_missing_url_or_date() {
        let xml = r#"
          <dump><mesic>3</mesic><rok>2025</rok><odkaz>https://x/dump_2025_03.xml</odkaz></dump>
          <dump><mesic>4</mesic><rok>2025</rok></dump>
          <dump><rok>2025</rok><odkaz>https://x/no_month.xml</odkaz></dump>
        "#;
        let dumps = parse_dumps(xml);
        assert_eq!(dumps.len(), 1, "only the complete entry is kept");
        assert_eq!(dumps[0].url, "https://x/dump_2025_03.xml");
    }

    #[test]
    fn empty_or_unrelated_xml_yields_no_dumps() {
        assert!(parse_dumps("<index></index>").is_empty());
        assert!(parse_dumps("not xml at all").is_empty());
    }
}
