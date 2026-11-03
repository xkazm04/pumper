//! HTML -> clean Markdown preprocessing. Strips boilerplate (scripts, styles,
//! nav/header/footer chrome) and serializes the meaningful content as Markdown.
//! Useful for storing readable snapshots and, especially, for shrinking a page
//! to the tokens that matter before handing it to the Claude engine.

use ego_tree::NodeRef;
use scraper::node::Node;
use scraper::Html;

/// Tags whose entire subtree is dropped.
const SKIP: &[&str] = &[
    "script",
    "style",
    "noscript",
    "template",
    "svg",
    "canvas",
    "iframe",
    "head",
    "nav",
    "header",
    "footer",
    "aside",
    "form",
    "button",
    "input",
    "select",
    "textarea",
    "figure",
    "figcaption",
];

/// Converts an HTML document to Markdown.
///
/// Document text is escaped only where a character could parse as Markdown in
/// the assembled output (see [`resolve_escapes`]): `5 * 3`, `snake_case` and
/// prices stay as they are; `*not emphasis*` and `# not a heading` do not
/// silently become syntax.
pub fn html_to_markdown(html: &str) -> String {
    let doc = Html::parse_document(html);
    let mut out = String::new();
    let mut ctx = Ctx::default();
    walk(doc.tree.root(), &mut out, &mut ctx);
    resolve_escapes(&normalize(&out), false)
}

/// Converts an HTML *fragment* to Markdown — for a scoped extraction rule that
/// yields one element's HTML (e.g. `article.content`) rather than a whole page.
/// Uses `parse_fragment` so the input isn't wrapped in `<html><body>`; the same
/// `SKIP` rules still apply inside the subtree (a nested `<form>` stays dropped).
///
/// A fragment's output is a field value its caller may embed or join, so what
/// follows its end is unknown: a delimiter there is escaped as though a partner
/// follows.
pub fn html_fragment_to_markdown(html: &str) -> String {
    let doc = Html::parse_fragment(html);
    let mut out = String::new();
    let mut ctx = Ctx::default();
    walk(doc.tree.root(), &mut out, &mut ctx);
    resolve_escapes(&normalize(&out), true)
}

/// Characters of visible text (SKIP subtrees dropped, whitespace collapsed the
/// same way the Markdown conversion collapses it), counted only up to `cap` and
/// saturating there.
///
/// The tier-escalation decision only needs a predicate — "is there at least N
/// chars of content?" — not a Markdown document. On the extractor/plugin hot
/// paths, which don't request Markdown, using this instead of building and
/// discarding a full `html_to_markdown` avoids a whole DOM serialize plus a
/// document-sized `String` allocation per page, and it early-exits the walk once
/// `cap` is reached. (It counts text, not markup/link URLs, so it is a slight
/// under-count of the Markdown length — the right direction for "real content".)
pub fn text_len_capped(html: &str, cap: usize) -> usize {
    if cap == 0 {
        return 0;
    }
    let doc = Html::parse_document(html);
    let mut acc = TextAcc {
        cap,
        count: 0,
        prev_ws: true,
        out: None,
    };
    walk_visible_text(doc.tree.root(), &mut acc);
    acc.count
}

/// The visible text itself, under the same rules as [`text_len_capped`] (SKIP
/// subtrees dropped, whitespace collapsed, truncated at `cap` chars).
///
/// This is the input to the *content* fingerprint in the extraction-health
/// detector: fingerprinting raw HTML would make every markup change look like a
/// content change and destroy the text-blind/structure-blind asymmetry the
/// detector depends on. Takes a parsed tree because the caller usually already
/// has one (and needs it for the DOM fingerprint too).
pub fn visible_text_capped(doc: &Html, cap: usize) -> String {
    if cap == 0 {
        return String::new();
    }
    let mut out = String::new();
    let mut acc = TextAcc {
        cap,
        count: 0,
        prev_ws: true,
        out: Some(&mut out),
    };
    walk_visible_text(doc.tree.root(), &mut acc);
    out
}

/// Shared state for the visible-text walk: counts chars, and optionally collects
/// them. One walker so the counting predicate and the collected text can never
/// disagree about what "visible" means.
struct TextAcc<'a> {
    cap: usize,
    count: usize,
    /// Mirrors `push_text`: a leading whitespace at a word boundary isn't counted.
    prev_ws: bool,
    out: Option<&'a mut String>,
}

impl TextAcc<'_> {
    fn done(&self) -> bool {
        self.count >= self.cap
    }

    fn push(&mut self, ch: char) {
        if ch.is_whitespace() {
            if self.prev_ws {
                return;
            }
            self.prev_ws = true;
            self.count += 1;
            if let Some(out) = self.out.as_deref_mut() {
                out.push(' ');
            }
        } else {
            self.prev_ws = false;
            self.count += 1;
            if let Some(out) = self.out.as_deref_mut() {
                out.push(ch);
            }
        }
    }
}

fn walk_visible_text(node: NodeRef<Node>, acc: &mut TextAcc) {
    if acc.done() {
        return;
    }
    match node.value() {
        Node::Text(text) => {
            for ch in text.text.chars() {
                acc.push(ch);
                if acc.done() {
                    return;
                }
            }
        }
        Node::Element(el) if SKIP.contains(&el.name()) => {}
        _ => {
            for child in node.children() {
                walk_visible_text(child, acc);
                if acc.done() {
                    return;
                }
            }
        }
    }
}

#[derive(Default, Clone)]
struct Ctx {
    /// Preserve whitespace inside <pre>.
    pre: bool,
    /// Ordered-list counter stack (None = unordered).
    list_stack: Vec<Option<u32>>,
    /// Inside an inline code span, where a backslash is literal.
    code: bool,
}

fn walk(node: NodeRef<Node>, out: &mut String, ctx: &mut Ctx) {
    match node.value() {
        Node::Text(text) => push_text(&text.text, out, ctx.pre || ctx.code),
        Node::Element(el) => {
            let name = el.name();
            if SKIP.contains(&name) {
                return;
            }
            match name {
                "br" => out.push('\n'),
                "hr" => out.push_str("\n\n---\n\n"),
                "img" => {
                    let alt = el.attr("alt").unwrap_or("");
                    if let Some(src) = el.attr("src") {
                        out.push_str(&format!("![{alt}]({src})"));
                    }
                }
                "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => {
                    let level = name[1..].parse::<usize>().unwrap_or(1);
                    block(out);
                    out.push_str(&"#".repeat(level));
                    out.push(' ');
                    walk_children(node, out, ctx);
                    block(out);
                }
                "p" | "div" | "section" | "article" | "main" => {
                    block(out);
                    walk_children(node, out, ctx);
                    block(out);
                }
                "table" => render_table(node, out, ctx),
                "ul" | "ol" => {
                    block(out);
                    ctx.list_stack
                        .push(if name == "ol" { Some(1) } else { None });
                    walk_children(node, out, ctx);
                    ctx.list_stack.pop();
                    block(out);
                }
                "li" => {
                    let depth = ctx.list_stack.len().saturating_sub(1);
                    out.push('\n');
                    out.push_str(&"  ".repeat(depth));
                    match ctx.list_stack.last_mut() {
                        Some(Some(n)) => {
                            out.push_str(&format!("{n}. "));
                            *n += 1;
                        }
                        _ => out.push_str("- "),
                    }
                    walk_children(node, out, ctx);
                }
                "a" => {
                    let href = el.attr("href").unwrap_or_default().to_string();
                    let start = out.len();
                    walk_children(node, out, ctx);
                    // Only emit link syntax when there's visible text and a real href.
                    let has_text = !out[start..].trim().is_empty();
                    if !href.is_empty() && !href.starts_with('#') && has_text {
                        out.push_str(&format!("]({href})"));
                        out.insert(start, '[');
                    }
                }
                "strong" | "b" => wrap(node, out, ctx, "**"),
                "em" | "i" => wrap(node, out, ctx, "_"),
                "code" if !ctx.pre => {
                    let was_code = ctx.code;
                    ctx.code = true;
                    wrap(node, out, ctx, "`");
                    ctx.code = was_code;
                }
                "pre" => {
                    block(out);
                    out.push_str("```\n");
                    let was_pre = ctx.pre;
                    ctx.pre = true;
                    walk_children(node, out, ctx);
                    ctx.pre = was_pre;
                    if !out.ends_with('\n') {
                        out.push('\n');
                    }
                    out.push_str("```");
                    block(out);
                }
                "blockquote" => {
                    block(out);
                    let start = out.len();
                    walk_children(node, out, ctx);
                    // Prefix each produced line with "> ".
                    let quoted: String = out[start..]
                        .trim()
                        .lines()
                        .map(|l| format!("> {l}"))
                        .collect::<Vec<_>>()
                        .join("\n");
                    out.truncate(start);
                    out.push_str(&quoted);
                    block(out);
                }
                _ => walk_children(node, out, ctx),
            }
        }
        _ => walk_children(node, out, ctx),
    }
}

fn walk_children(node: NodeRef<Node>, out: &mut String, ctx: &mut Ctx) {
    for child in node.children() {
        walk(child, out, ctx);
    }
}

/// Renders a `<table>` as a GitHub pipe table. **The first row is the header**:
/// when it holds `<th>` cells they become the headers; a `<th>`-less table
/// promotes its first `<tr>` (data row) to the header instead. Cells with
/// nested block content degrade to inline text (whitespace collapsed, `|`
/// escaped). Empty tables emit nothing.
fn render_table(node: NodeRef<Node>, out: &mut String, ctx: &Ctx) {
    let mut rows: Vec<Vec<String>> = Vec::new();
    collect_rows(node, &mut rows, ctx);
    rows.retain(|r| !r.is_empty());
    let Some(cols) = rows.iter().map(Vec::len).max().filter(|c| *c > 0) else {
        return; // no cells → nothing to render
    };

    block(out);
    emit_row(out, &rows[0], cols);
    out.push('|');
    for _ in 0..cols {
        out.push_str(" --- |");
    }
    out.push('\n');
    for row in &rows[1..] {
        emit_row(out, row, cols);
    }
    block(out);
}

/// Walks a table subtree collecting one `Vec<cell>` per `<tr>`. Recurses through
/// `<thead>`/`<tbody>`/`<tfoot>` wrappers; a nested `<table>` inside a cell is
/// NOT scanned for rows here (its text is flattened into the cell by
/// [`cell_text`]).
fn collect_rows(node: NodeRef<Node>, rows: &mut Vec<Vec<String>>, ctx: &Ctx) {
    for child in node.children() {
        if let Node::Element(el) = child.value() {
            match el.name() {
                "tr" => {
                    let mut cells = Vec::new();
                    for cell in child.children() {
                        if let Node::Element(c) = cell.value() {
                            if matches!(c.name(), "td" | "th") {
                                cells.push(cell_text(cell, ctx));
                            }
                        }
                    }
                    rows.push(cells);
                }
                "table" => {} // nested table: flattened as cell text, not rows
                _ => collect_rows(child, rows, ctx),
            }
        }
    }
}

/// Renders one cell's content as a single inline string: full inline markdown
/// (bold, links, …) with whitespace/newlines collapsed and `|` escaped so it
/// can't break the pipe-table grid.
fn cell_text(cell: NodeRef<Node>, ctx: &Ctx) -> String {
    let mut buf = String::new();
    let mut c = ctx.clone();
    walk_children(cell, &mut buf, &mut c);
    buf.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace(&format!("{TEXT}|"), "|")
        .replace('|', "\\|")
}

/// Emits one `| a | b |` table row, padding short rows to `cols` cells.
fn emit_row(out: &mut String, cells: &[String], cols: usize) {
    out.push('|');
    for i in 0..cols {
        out.push(' ');
        out.push_str(cells.get(i).map(String::as_str).unwrap_or(""));
        out.push_str(" |");
    }
    out.push('\n');
}

fn wrap(node: NodeRef<Node>, out: &mut String, ctx: &mut Ctx, marker: &str) {
    let start = out.len();
    walk_children(node, out, ctx);
    if out[start..].trim().is_empty() {
        return; // nothing to emphasize
    }
    out.push_str(marker);
    out.insert_str(start, marker);
}

/// Appends document text. `raw` (inside `<pre>` or `<code>`) copies it as is;
/// otherwise whitespace is collapsed and every character is marked as text, so
/// `resolve_escapes` can tell it from the syntax this converter emits.
fn push_text(text: &str, out: &mut String, raw: bool) {
    // The provenance mark is ours; document text cannot forge one.
    let chars = text.chars().filter(|c| *c != TEXT);
    if raw {
        out.extend(chars);
        return;
    }
    // Collapse runs of whitespace to a single space, but keep a leading/trailing
    // space if the raw text had one (word boundaries between inline elements).
    let mut prev_ws = out.ends_with([' ', '\n']) || out.is_empty();
    for ch in chars {
        if ch.is_whitespace() {
            if !prev_ws {
                out.push(' ');
                prev_ws = true;
            }
        } else {
            out.push(TEXT);
            out.push(ch);
            prev_ws = false;
        }
    }
}

/// Ensures the buffer ends with a blank-line block separator.
fn block(out: &mut String) {
    while out.ends_with(' ') {
        out.pop();
    }
    if out.is_empty() {
        return;
    }
    if !out.ends_with("\n\n") {
        while out.ends_with('\n') {
            out.pop();
        }
        out.push_str("\n\n");
    }
}

/// Collapse 3+ newlines, trim trailing spaces per line, trim ends.
fn normalize(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut blank_run = 0;
    for line in s.lines() {
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            blank_run += 1;
            if blank_run <= 1 {
                result.push('\n');
            }
        } else {
            blank_run = 0;
            result.push_str(trimmed);
            result.push('\n');
        }
    }
    result.trim().to_string()
}

// ---------------------------------------------------------------------------
// Escaping document text
//
// Text is escaped only where a character could actually parse as Markdown in
// the assembled output: a blanket escaper turns `snake_case`, `5 * 3` and every
// bracket into backslash noise that each plain-text consumer then has to undo.
// Whether a character is syntax depends on characters that OTHER runs emit (a
// `**` from the next `<b>`, a code span's backtick, a link's `[`), so the
// decision cannot be made while a text node is being pushed. `push_text` marks
// each text character with `TEXT` and the decision is taken here, once the
// whole output is assembled:
//
// - A pair whose halves are both document text is neutralised by escaping the
//   LATER half, which can always see the earlier one. So the lookahead only has
//   to find syntax this converter emits, which cannot be escaped.
// - Backticks are the exception: a backslash does not stop a backtick from
//   closing a code span, so the opener is the one that must be escaped.
// - A fragment's end is unknown context (its caller may embed or join it), so a
//   delimiter there is escaped as though a partner follows.
// ---------------------------------------------------------------------------

/// Provenance mark: the next character came from document text, not from
/// syntax this converter emitted. Never survives into the output.
const TEXT: char = '\u{E000}';

#[derive(Clone, Copy)]
struct Tok {
    c: char,
    /// Document text (escapable) rather than emitted syntax.
    text: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Next {
    Char(char),
    /// Nothing follows inside the inline container: behaves as whitespace.
    End,
    /// Something may follow that this decision cannot see: assume the worst.
    Unknown,
}

/// One inline container: the characters a Markdown parser pairs delimiters
/// across (a paragraph, a list item, a heading, one table cell). Line breaks
/// inside a paragraph do not end it: emphasis spans them.
struct Container {
    idx: Vec<usize>,
    /// First character of block content, where block syntax (headings, list
    /// markers, quotes, thematic breaks) can form.
    start: Vec<bool>,
}

fn resolve_escapes(s: &str, fragment: bool) -> String {
    let mut toks = Vec::with_capacity(s.len());
    let mut text = false;
    for c in s.chars() {
        if c == TEXT {
            text = true;
        } else {
            toks.push(Tok { c, text });
            text = false;
        }
    }
    let mut escape = vec![false; toks.len()];
    let conts = containers(&toks);
    let last = conts.len().saturating_sub(1);
    for (k, cont) in conts.iter().enumerate() {
        decide(&toks, cont, fragment && k == last, &mut escape);
    }
    let mut out = String::with_capacity(s.len() + 16);
    for (t, e) in toks.iter().zip(&escape) {
        if *e {
            out.push('\\');
        }
        out.push(t.c);
    }
    out
}

fn is_syntax(t: &Tok, c: char) -> bool {
    !t.text && t.c == c
}

fn new_container() -> Container {
    Container {
        idx: Vec::new(),
        start: Vec::new(),
    }
}

/// Splits the assembled output into inline containers, skipping fenced code.
fn containers(toks: &[Tok]) -> Vec<Container> {
    let mut lines: Vec<(usize, usize)> = Vec::new();
    let mut a = 0;
    for (i, t) in toks.iter().enumerate() {
        if t.c == '\n' {
            lines.push((a, i));
            a = i + 1;
        }
    }
    lines.push((a, toks.len()));

    let mut out = Vec::new();
    let mut cur: Option<Container> = None;
    let mut in_fence = false;
    let close = |cur: &mut Option<Container>, out: &mut Vec<Container>| {
        if let Some(c) = cur.take() {
            if !c.idx.is_empty() {
                out.push(c);
            }
        }
    };
    for &(a, b) in &lines {
        let line = &toks[a..b];
        if line.len() >= 3 && line[..3].iter().all(|t| is_syntax(t, '`')) {
            close(&mut cur, &mut out);
            in_fence = !in_fence;
            continue;
        }
        if in_fence {
            continue;
        }
        if line.iter().all(|t| t.c.is_whitespace()) {
            close(&mut cur, &mut out);
            continue;
        }
        if is_syntax(&line[0], '|') {
            // A table row: every cell is its own inline container.
            close(&mut cur, &mut out);
            let mut cell = new_container();
            for i in a + 1..b {
                if is_syntax(&toks[i], '|') && !is_syntax(&toks[i - 1], '\\') {
                    out.push(std::mem::replace(&mut cell, new_container()));
                } else {
                    cell.idx.push(i);
                    cell.start.push(false);
                }
            }
            if !cell.idx.is_empty() {
                out.push(cell);
            }
            continue;
        }
        // Step past the structural prefix this converter emitted.
        let mut p = a;
        let mut new_item = false;
        let mut heading = false;
        loop {
            while p < b && !toks[p].text && toks[p].c == ' ' {
                p += 1;
            }
            if p + 1 < b && is_syntax(&toks[p], '>') && toks[p + 1].c == ' ' {
                p += 2;
                continue;
            }
            if p + 1 < b && is_syntax(&toks[p], '-') && toks[p + 1].c == ' ' {
                p += 2;
                new_item = true;
                continue;
            }
            let mut d = p;
            while d < b && !toks[d].text && toks[d].c.is_ascii_digit() {
                d += 1;
            }
            if d > p && d + 1 < b && is_syntax(&toks[d], '.') && toks[d + 1].c == ' ' {
                p = d + 2;
                new_item = true;
                continue;
            }
            let mut h = p;
            while h < b && is_syntax(&toks[h], '#') {
                h += 1;
            }
            if h > p && h < b && toks[h].c == ' ' {
                p = h + 1;
                heading = true;
            }
            break;
        }
        if heading || new_item {
            close(&mut cur, &mut out);
        }
        let c = cur.get_or_insert_with(new_container);
        if !c.idx.is_empty() {
            c.idx.push(a - 1); // the line break joining this line to the previous
            c.start.push(false);
        }
        for i in p..b {
            c.idx.push(i);
            c.start.push(i == p && !heading);
        }
        if heading {
            close(&mut cur, &mut out);
        }
    }
    close(&mut cur, &mut out);
    out
}

fn decide(toks: &[Tok], cont: &Container, unknown_end: bool, esc: &mut [bool]) {
    let n = cont.idx.len();
    let tok = |k: usize| toks[cont.idx[k]];
    let ch = |k: usize| tok(k).c;
    let next_at = |j: usize| -> Next {
        if j < n {
            Next::Char(ch(j))
        } else if unknown_end {
            Next::Unknown
        } else {
            Next::End
        }
    };
    for k in 0..n {
        if !tok(k).text {
            continue;
        }
        let c = ch(k);
        let next = next_at(k + 1);
        let next_ws = match next {
            Next::End => true,
            Next::Char(x) => x.is_whitespace(),
            Next::Unknown => false,
        };
        let prev = (k > 0).then(|| ch(k - 1));
        let prev_ws = prev.is_none_or(char::is_whitespace);
        let at_start = cont.start[k];
        let line_only = |c: char| {
            (k..n)
                .map(ch)
                .take_while(|x| *x != '\n')
                .all(|x| x == c || x == ' ' || x == '\t')
        };
        // Could the parser pair this character with another run of it? Earlier
        // runs of any origin count; later runs count only when they are emitted
        // syntax (a later text run escapes itself), except for backticks.
        let partner = |c: char| -> bool {
            let mut a = k;
            while a > 0 && ch(a - 1) == c {
                a -= 1;
            }
            let mut b = k + 1;
            while b < n && ch(b) == c {
                b += 1;
            }
            (0..a).any(|j| ch(j) == c)
                || (b..n).any(|j| ch(j) == c && (c == '`' || !tok(j).text))
                || unknown_end
        };
        let e = match c {
            '\\' => match next {
                Next::Char(x) => x.is_ascii_punctuation() || x == '\n',
                Next::Unknown => true,
                Next::End => false,
            },
            '*' | '_' | '~' => {
                if at_start && (line_only(c) || (c == '*' && (next_ws || next == Next::Unknown))) {
                    true
                } else {
                    // Space on both sides: can neither open nor close.
                    let inert_space = prev_ws && next_ws;
                    // An intraword underscore is never emphasis.
                    let inert_intraword = c == '_'
                        && prev.is_some_and(char::is_alphanumeric)
                        && matches!(next, Next::Char(x) if x.is_alphanumeric());
                    !inert_space && !inert_intraword && partner(c)
                }
            }
            '`' => {
                let run_len = (k..n).take_while(|&j| ch(j) == '`').count();
                (at_start && run_len >= 3) || partner('`')
            }
            '[' => (k + 1..n).any(|j| is_syntax(&tok(j), ']')) || unknown_end,
            ']' => (0..k).any(|j| ch(j) == '['),
            '<' => match next {
                Next::Char(x) => x.is_ascii_alphabetic() || matches!(x, '/' | '!' | '?'),
                Next::Unknown => true,
                Next::End => false,
            },
            '!' => matches!(next, Next::Char('[') | Next::Unknown),
            '&' => {
                let mut j = k + 1;
                let mut seen = 0;
                loop {
                    match next_at(j) {
                        Next::Char('#') if seen == 0 => break true,
                        Next::Char(';') => break seen > 0,
                        Next::Char(x) if x.is_ascii_alphanumeric() => seen += 1,
                        Next::Unknown => break true,
                        _ => break false,
                    }
                    j += 1;
                }
            }
            '#' if at_start => {
                let h = (k..n).take_while(|&j| ch(j) == '#').count();
                h <= 6
                    && match next_at(k + h) {
                        Next::Char(x) => x.is_whitespace(),
                        _ => true,
                    }
            }
            '>' => at_start,
            '-' | '+' => at_start && (next_ws || next == Next::Unknown || line_only(c)),
            '=' => at_start && line_only('='),
            '.' | ')' => {
                let mut j = k;
                while j > 0 && ch(j - 1).is_ascii_digit() && tok(j - 1).text {
                    j -= 1;
                }
                j < k && k - j <= 9 && cont.start[j] && (next_ws || next == Next::Unknown)
            }
            _ => false,
        };
        if e {
            esc[cont.idx[k]] = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        html_fragment_to_markdown, html_to_markdown, text_len_capped, visible_text_capped,
    };

    #[test]
    fn visible_text_agrees_with_the_counter_and_drops_the_same_boilerplate() {
        let html = "<nav>Home About</nav><p>Real content here.</p><script>var x=1;</script>";
        let doc = scraper::Html::parse_document(html);
        let text = visible_text_capped(&doc, 1000);
        assert!(text.contains("Real content here."), "{text}");
        assert!(
            !text.contains("Home"),
            "nav leaked into the content fingerprint: {text}"
        );
        assert!(
            !text.contains("var x"),
            "script leaked into the content fingerprint: {text}"
        );
        // The collector and the counter must never disagree about "visible".
        assert_eq!(text.chars().count(), text_len_capped(html, 1000));
        // Truncation is by the same cap.
        assert_eq!(visible_text_capped(&doc, 4).chars().count(), 4);
        assert!(visible_text_capped(&doc, 0).is_empty());
    }

    #[test]
    fn text_len_capped_saturates_and_agrees_with_markdown_on_the_threshold() {
        // A content-rich page: both the capped counter and the full markdown are
        // over the threshold (the counter saturates at the cap).
        let rich = format!("<main><p>{}</p></main>", "word ".repeat(200)); // ~1000 chars
        assert_eq!(text_len_capped(&rich, 250), 250, "saturates at the cap");
        assert!(html_to_markdown(&rich).chars().count() >= 250);

        // A thin page: both are under the threshold, and the counter returns the
        // exact (un-saturated) length.
        let thin = "<main><p>just a little</p></main>";
        let n = text_len_capped(thin, 250);
        assert!(
            n < 250 && n == "just a little".len(),
            "exact when below cap: {n}"
        );
        assert!(html_to_markdown(thin).chars().count() < 250);

        // Boilerplate is dropped, same as the markdown conversion.
        let boiler =
            "<nav>Home About Contact Services</nav><footer>copyright notice here</footer><p>hi</p>";
        assert_eq!(
            text_len_capped(boiler, 250),
            "hi".len(),
            "nav/footer skipped"
        );
    }

    #[test]
    fn strips_boilerplate_and_keeps_content() {
        let html = r#"<html><head><style>x{}</style></head><body>
            <nav>Home About</nav>
            <main><h1>Title</h1><p>Hello <strong>world</strong>.</p>
            <ul><li>one</li><li>two</li></ul></main>
            <footer>copyright</footer></body></html>"#;
        let md = html_to_markdown(html);
        assert!(md.contains("# Title"), "{md}");
        assert!(md.contains("Hello **world**."), "{md}");
        assert!(md.contains("- one"), "{md}");
        assert!(md.contains("- two"), "{md}");
        assert!(!md.contains("Home About"), "nav leaked: {md}");
        assert!(!md.contains("copyright"), "footer leaked: {md}");
    }

    #[test]
    fn links_and_headings() {
        let md = html_to_markdown(r#"<h2>Docs</h2><p><a href="https://x.com">site</a></p>"#);
        assert!(md.contains("## Docs"), "{md}");
        assert!(md.contains("[site](https://x.com)"), "{md}");
    }

    #[test]
    fn empty_input() {
        assert_eq!(html_to_markdown(""), "");
    }

    #[test]
    fn table_with_th_header() {
        let html = r#"<table>
            <thead><tr><th>Name</th><th>Price</th></tr></thead>
            <tbody>
              <tr><td>Widget</td><td>$9.99</td></tr>
              <tr><td>Gadget</td><td>$12.00</td></tr>
            </tbody>
        </table>"#;
        let md = html_to_markdown(html);
        assert!(md.contains("| Name | Price |"), "{md}");
        assert!(md.contains("| --- | --- |"), "{md}");
        assert!(md.contains("| Widget | $9.99 |"), "{md}");
        assert!(md.contains("| Gadget | $12.00 |"), "{md}");
    }

    #[test]
    fn table_without_th_promotes_first_row() {
        let html = "<table><tr><td>a</td><td>b</td></tr><tr><td>1</td><td>2</td></tr></table>";
        let md = html_to_markdown(html);
        // First row becomes the header, followed by the separator, then data.
        let expected = "| a | b |\n| --- | --- |\n| 1 | 2 |";
        assert_eq!(md, expected, "{md}");
    }

    #[test]
    fn table_messy_cells_degrade_to_inline() {
        let html = r#"<table>
            <tr><th>Item</th><th>Note</th></tr>
            <tr>
              <td><strong>Big</strong> box</td>
              <td><p>line one</p><p>line two | with pipe</p></td>
            </tr>
        </table>"#;
        let md = html_to_markdown(html);
        // Nested block content collapses to one inline line; pipe is escaped.
        assert!(
            md.contains("| **Big** box | line one line two \\| with pipe |"),
            "{md}"
        );
        // Ragged rows are padded so the grid stays rectangular.
        let ragged =
            html_to_markdown("<table><tr><th>a</th><th>b</th></tr><tr><td>x</td></tr></table>");
        assert!(ragged.contains("| x |  |"), "{ragged}");
    }

    #[test]
    fn text_that_would_pair_with_emitted_syntax_is_escaped() {
        // Each of these pairs a text character with syntax a LATER element emits,
        // so it is only visible once the line is assembled.
        let cases = [
            // a backtick in text would open a span the <code> element closes
            ("<p>a `b <code>c</code> d</p>", "a \\`b `c` d"),
            // a trailing backslash would escape the first `*` of the bold marker
            ("<p>C:\\<b>Users</b></p>", "C:\\\\**Users**"),
            // a trailing `!` would turn the link after it into an image
            (
                r#"<p>Wow!<a href="https://x.com">here</a></p>"#,
                "Wow\\![here](https://x.com)",
            ),
            // a text star pairs with a text star two elements later
            (
                "<p>*note <b>important</b> here*</p>",
                "\\*note **important** here\\*",
            ),
            // a text backtick pair split by an element: a backslash does not stop
            // a backtick from closing a code span, so the OPENER is escaped
            ("<p>a `b <i>x</i> c` d</p>", "a \\`b _x_ c\\` d"),
            // emphasis spans a line break inside one paragraph
            ("<p>*a<br>b*</p>", "*a\nb\\*"),
        ];
        for (html, want) in cases {
            assert_eq!(html_to_markdown(html), want, "{html}");
        }
    }

    #[test]
    fn text_that_cannot_be_syntax_is_left_alone() {
        // The opposite direction: none of these can parse as Markdown, and a
        // backslash in any of them is noise every plain-text consumer pays for.
        for text in [
            "Price: $20.00 or $30 per month",
            "5 * 3 = 15, and 2 * 4 = 8",
            "Use snake_case_names like max_retry_count here",
            "Use the * wildcard to match",
            "if a < b and c > d then",
            "Rock & Roll, AT&T, Q&A",
            "C:\\Users\\me\\file.txt",
            "Section 2.1 covers v1.2",
            "well-known, e-mail - dash",
        ] {
            let html = format!(
                "<p>{}</p>",
                text.replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;")
            );
            assert_eq!(html_to_markdown(&html), text, "{html}");
        }
        assert_eq!(
            html_to_markdown("<p><b>Note</b> * see below</p>"),
            "**Note** * see below"
        );
    }

    #[test]
    fn block_syntax_and_raw_html_in_text_are_escaped() {
        let cases = [
            ("<p># not a heading</p>", "\\# not a heading"),
            ("<p>2024. A great year</p>", "2024\\. A great year"),
            ("<p>- not a list</p>", "\\- not a list"),
            ("<ul><li># tag</li></ul>", "- \\# tag"),
            ("<blockquote>1. first</blockquote>", "> 1\\. first"),
            ("<p>&lt;your tenant&gt; here</p>", "\\<your tenant> here"),
            ("<p>&amp;copy; is an entity</p>", "\\&copy; is an entity"),
            ("<p>the __init__ method</p>", "the __init\\_\\_ method"),
        ];
        for (html, want) in cases {
            assert_eq!(html_to_markdown(html), want, "{html}");
        }
        // Code spans are literal: nothing inside one is escaped.
        assert_eq!(html_to_markdown("<p><code>a*b_c</code></p>"), "`a*b_c`");
    }

    #[test]
    fn a_fragment_end_is_unknown_context_and_escapes_as_if_a_partner_follows() {
        // A whole document ends where the text ends: a lone opener is inert.
        assert_eq!(html_to_markdown("<p>*Limited</p>"), "*Limited");
        // A fragment's value is embedded or joined by its caller: not inert.
        assert_eq!(
            html_fragment_to_markdown("<span>*Limited</span>"),
            "\\*Limited"
        );
        assert_eq!(
            html_fragment_to_markdown("<span>see [draft</span>"),
            "see \\[draft"
        );
        // ...and still leaves text that cannot pair alone.
        assert_eq!(html_fragment_to_markdown("<span>5 * 3</span>"), "5 * 3");
    }
}
