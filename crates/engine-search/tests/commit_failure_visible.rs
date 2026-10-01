//! The shutdown arm of the background committer used to discard the commit
//! result. The interval arm already warned. One rule, two sites, one of them
//! silent — a graceful stop could lose the uncommitted tail with no log line.

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

fn shutdown_arm(code: &str) -> String {
    let start = code
        .find("shutdown.notified()")
        .expect("the committer has a shutdown arm");
    let arm = &code[start..];
    let end = arm.find("break;").expect("the shutdown arm breaks");
    arm[..end].to_string()
}

/// The anti-pattern: `let _ = commit_and_reload` on the shutdown arm, so the
/// last chance to flush the tail can fail and leave nothing an operator can read.
#[test]
fn shutdown_commit_failure_is_warned_not_discarded() {
    let code = strip_comments(include_str!("../src/lib.rs"));
    let arm = shutdown_arm(&code);
    assert!(
        !arm.contains("let _"),
        "the shutdown commit result is discarded:\n{arm}"
    );
    assert!(
        arm.contains("warn_commit_failure"),
        "the shutdown arm must report a commit failure through warn_commit_failure:\n{arm}"
    );
}
