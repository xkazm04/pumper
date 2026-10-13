//! Opening a plugin directory and walking its entries used to treat every IO
//! failure as "there are no plugins". A missing directory is that. An
//! unreadable one is not, and a single unreadable entry is not either.

fn strip_comments(src: &str) -> String {
    let mut out = String::new();
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '/' && chars.peek() == Some(&'/') {
            chars.next();
            for n in chars.by_ref() {
                if n == '\n' {
                    out.push('\n');
                    break;
                }
            }
        } else if c == '/' && chars.peek() == Some(&'*') {
            chars.next();
            while let Some(n) = chars.next() {
                if n == '*' && chars.peek() == Some(&'/') {
                    chars.next();
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// The anti-pattern: `read_dir` failing closed into an empty map, and
/// `entries.flatten()` dropping a bad directory entry with no log.
#[test]
fn unreadable_plugin_dir_is_not_reported_as_empty() {
    let code = strip_comments(include_str!("../src/lib.rs"));
    let opens = code.matches("std::fs::read_dir").count();
    assert_eq!(
        opens, 1,
        "both scanners must share one directory open, found {opens}"
    );
    assert!(
        !code.contains(".flatten()"),
        "directory entry errors must not be dropped by flatten"
    );
    assert!(
        code.contains("plugin directory unreadable"),
        "an unreadable directory must be logged, not folded into an empty load"
    );
    assert!(
        code.contains("plugin directory entry unreadable"),
        "an unreadable directory entry must be logged"
    );
}
