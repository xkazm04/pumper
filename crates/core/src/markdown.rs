//! HTML -> clean Markdown preprocessing. Strips boilerplate (scripts, styles,
//! nav/header/footer chrome) and serializes the meaningful content as Markdown.
//! Useful for storing readable snapshots and, especially, for shrinking a page
//! to the tokens that matter before handing it to the Claude engine.

use ego_tree::NodeRef;
use scraper::node::Node;
use scraper::Html;

/// Tags whose entire subtree is dropped.
const SKIP: &[&str] = &[
    "script", "style", "noscript", "template", "svg", "canvas", "iframe", "head", "nav", "header",
    "footer", "aside", "form", "button", "input", "select", "textarea", "figure", "figcaption",
];

/// Converts an HTML document to Markdown.
pub fn html_to_markdown(html: &str) -> String {
    let doc = Html::parse_document(html);
    let mut out = String::new();
    let mut ctx = Ctx::default();
    walk(doc.tree.root(), &mut out, &mut ctx);
    normalize(&out)
}

#[derive(Default, Clone)]
struct Ctx {
    /// Preserve whitespace inside <pre>.
    pre: bool,
    /// Ordered-list counter stack (None = unordered).
    list_stack: Vec<Option<u32>>,
}

fn walk(node: NodeRef<Node>, out: &mut String, ctx: &mut Ctx) {
    match node.value() {
        Node::Text(text) => push_text(&text.text, out, ctx.pre),
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
                    ctx.list_stack.push(if name == "ol" { Some(1) } else { None });
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
                    let has_text = out[start..].trim().is_empty();
                    if !href.is_empty() && !href.starts_with('#') && !has_text {
                        out.push_str(&format!("]({href})"));
                        out.insert(start, '[');
                    }
                }
                "strong" | "b" => wrap(node, out, ctx, "**"),
                "em" | "i" => wrap(node, out, ctx, "_"),
                "code" if !ctx.pre => wrap(node, out, ctx, "`"),
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
    buf.split_whitespace().collect::<Vec<_>>().join(" ").replace('|', "\\|")
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

fn push_text(text: &str, out: &mut String, pre: bool) {
    if pre {
        out.push_str(text);
        return;
    }
    // Collapse runs of whitespace to a single space, but keep a leading/trailing
    // space if the raw text had one (word boundaries between inline elements).
    let mut prev_ws = out.ends_with([' ', '\n']) || out.is_empty();
    for ch in text.chars() {
        if ch.is_whitespace() {
            if !prev_ws {
                out.push(' ');
                prev_ws = true;
            }
        } else {
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

#[cfg(test)]
mod tests {
    use super::html_to_markdown;

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
        assert!(md.contains("| **Big** box | line one line two \\| with pipe |"), "{md}");
        // Ragged rows are padded so the grid stays rectangular.
        let ragged = html_to_markdown("<table><tr><th>a</th><th>b</th></tr><tr><td>x</td></tr></table>");
        assert!(ragged.contains("| x |  |"), "{ragged}");
    }
}
