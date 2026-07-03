//! Example app: Hacker News front page via the plain-HTTP engine.
//! Serves as the template for classic fetch-and-parse use cases.

use async_trait::async_trait;
use pumper_core::{AppContext, Error, HttpRequest, Result, ScrapeApp};
use scraper::{Html, Selector};
use serde::Serialize;
use serde_json::{json, Value};

pub struct HackerNews;

#[derive(Serialize)]
struct Story {
    rank: u32,
    id: Option<String>,
    title: String,
    url: Option<String>,
    points: Option<u32>,
    author: Option<String>,
    comments: Option<u32>,
}

#[async_trait]
impl ScrapeApp for HackerNews {
    fn name(&self) -> &'static str {
        "hackernews"
    }

    fn description(&self) -> &'static str {
        "Hacker News front page stories (http engine demo). Params: {\"pages\": 1-5}"
    }

    // Uncomment for a recurring scrape every 6 hours:
    // fn schedule(&self) -> Option<&'static str> { Some("0 0 */6 * * *") }

    async fn run(&self, ctx: AppContext) -> Result<Value> {
        let pages = ctx
            .params
            .get("pages")
            .and_then(Value::as_u64)
            .unwrap_or(1)
            .clamp(1, 5);

        let mut stories: Vec<Story> = Vec::new();
        for page in 1..=pages {
            let response = ctx
                .engines
                .http
                .fetch(HttpRequest::get(format!("https://news.ycombinator.com/news?p={page}")))
                .await?;
            if !response.is_success() {
                return Err(Error::App(format!("HN returned status {}", response.status)));
            }
            let offset = stories.len() as u32;
            stories.extend(parse_front_page(&response.body, offset));
        }

        ctx.save_artifact("stories.json", &serde_json::to_vec_pretty(&stories)?)
            .await?;

        // Dedup + change detection: upsert each story keyed by its HN id, so a
        // scheduled run only surfaces stories that are new or whose score/
        // comment counts changed since last time.
        let items: Vec<(String, Value)> = stories
            .iter()
            .map(|s| {
                let key = s.id.clone().unwrap_or_else(|| format!("rank-{}", s.rank));
                (key, serde_json::to_value(s).unwrap_or(Value::Null))
            })
            .collect();
        let summary = ctx.upsert_many("stories", &items).await?;

        Ok(json!({
            "count": stories.len(),
            "new": summary.new.len(),
            "changed": summary.changed.len(),
            "unchanged": summary.unchanged,
            "stories": stories,
        }))
    }
}

fn sel(css: &str) -> Selector {
    Selector::parse(css).expect("valid selector")
}

fn parse_front_page(html: &str, rank_offset: u32) -> Vec<Story> {
    let doc = Html::parse_document(html);
    let row_sel = sel("tr.athing");
    let title_sel = sel("span.titleline > a");
    let subtext_sel = sel("td.subtext");
    let score_sel = sel("span.score");
    let user_sel = sel("a.hnuser");
    let link_sel = sel("a");

    // Each story row is followed by a metadata row; the td.subtext cells come
    // in the same document order, so zipping by index pairs them up.
    let subtexts: Vec<_> = doc.select(&subtext_sel).collect();

    doc.select(&row_sel)
        .enumerate()
        .filter_map(|(i, row)| {
            let title_link = row.select(&title_sel).next()?;
            let title = title_link.text().collect::<String>();
            let url = title_link.value().attr("href").map(|href| {
                if href.starts_with("item?") {
                    format!("https://news.ycombinator.com/{href}")
                } else {
                    href.to_string()
                }
            });

            let subtext = subtexts.get(i);
            let points = subtext
                .and_then(|s| s.select(&score_sel).next())
                .and_then(|score| {
                    let text = score.text().collect::<String>();
                    text.split_whitespace().next()?.parse().ok()
                });
            let author = subtext
                .and_then(|s| s.select(&user_sel).next())
                .map(|a| a.text().collect::<String>());
            let comments = subtext.and_then(|s| {
                s.select(&link_sel)
                    .filter_map(|a| {
                        let text = a.text().collect::<String>().replace('\u{a0}', " ");
                        if !text.contains("comment") {
                            return None;
                        }
                        text.split_whitespace().next()?.parse::<u32>().ok()
                    })
                    .last()
            });

            Some(Story {
                rank: rank_offset + i as u32 + 1,
                id: row.value().attr("id").map(String::from),
                title,
                url,
                points,
                author,
                comments,
            })
        })
        .collect()
}
