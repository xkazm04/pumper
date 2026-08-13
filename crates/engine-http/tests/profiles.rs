//! Live test of the session vault against a local cookie-setting server: two
//! profiles get two jars, the jars are persisted to `<vault>/<name>/cookies.json`,
//! and a **fresh engine** (the "restart") replays those cookies. No network — the
//! server is an in-process axum app on an ephemeral port.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Query;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use pumper_core::config::{CacheConfig, GovernorConfig, HttpConfig, StorageConfig};
use pumper_core::{Governor, HttpCache, HttpClient, HttpRequest, Storage};
use pumper_engine_http::HttpEngine;

/// `GET /login?sid=x` sets a **session** cookie (no Expires) — the login case the
/// vault exists for, and the one an in-memory jar loses on restart.
async fn login(Query(q): Query<HashMap<String, String>>) -> impl IntoResponse {
    let sid = q.get("sid").cloned().unwrap_or_default();
    ([("set-cookie", format!("sid={sid}; Path=/"))], "ok")
}

/// `GET /echo` reflects the `Cookie` header the client sent (or `none`).
async fn echo(headers: HeaderMap) -> String {
    headers
        .get("cookie")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none")
        .to_string()
}

async fn spawn_server() -> String {
    let app = Router::new()
        .route("/login", get(login))
        .route("/echo", get(echo));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    format!("http://{addr}")
}

/// A real `HttpEngine` over a throwaway SQLite cache, rooted at `vault`.
async fn new_engine(root: &Path, vault: PathBuf) -> HttpEngine {
    let storage = Storage::connect(&StorageConfig {
        database_path: root.join("pumper.db"),
        artifacts_dir: root.join("artifacts"),
        ..StorageConfig::default()
    })
    .await
    .expect("storage");
    let cache = Arc::new(HttpCache::new(storage.pool(), &CacheConfig::default()));
    let governor = Arc::new(Governor::new(&GovernorConfig::default()));
    // Leak the pool with the engine for the test's lifetime.
    std::mem::forget(storage);
    HttpEngine::new(&HttpConfig::default(), governor, cache, vault).expect("engine")
}

fn profiled(url: &str, profile: &str) -> HttpRequest {
    let mut req = HttpRequest::get(url);
    req.profile = Some(profile.to_string());
    req
}

#[tokio::test]
async fn profiles_keep_separate_persistent_cookie_jars_across_a_restart() {
    let root = std::env::temp_dir().join(format!(
        "pumper-vault-http-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    let vault = root.join("profiles");
    let base = spawn_server().await;

    let engine = new_engine(&root, vault.clone()).await;

    // Two profiles log in as different users against the same host.
    engine
        .fetch(profiled(&format!("{base}/login?sid=alpha-1"), "alpha"))
        .await
        .expect("alpha login");
    engine
        .fetch(profiled(&format!("{base}/login?sid=beta-1"), "beta"))
        .await
        .expect("beta login");

    // Each profile replays only its own cookie...
    let alpha = engine
        .fetch(profiled(&format!("{base}/echo"), "alpha"))
        .await
        .expect("alpha echo");
    assert_eq!(alpha.body, "sid=alpha-1", "alpha replays its own session");
    let beta = engine
        .fetch(profiled(&format!("{base}/echo"), "beta"))
        .await
        .expect("beta echo");
    assert_eq!(
        beta.body, "sid=beta-1",
        "beta replays its own session (no bleed from alpha)"
    );

    // ...and a profile-less request stays anonymous (the default in-memory jar).
    let anon = engine
        .fetch(HttpRequest::get(format!("{base}/echo")))
        .await
        .expect("anonymous echo");
    assert_eq!(
        anon.body, "none",
        "profile-less requests carry no profile cookies"
    );

    // The debounced write-behind flushes both jars (trailing edge).
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let alpha_jar = vault.join("alpha").join("cookies.json");
    let beta_jar = vault.join("beta").join("cookies.json");
    assert!(
        alpha_jar.is_file(),
        "alpha jar written to {}",
        alpha_jar.display()
    );
    assert!(
        beta_jar.is_file(),
        "beta jar written to {}",
        beta_jar.display()
    );
    let alpha_json = std::fs::read_to_string(&alpha_jar).unwrap();
    let beta_json = std::fs::read_to_string(&beta_jar).unwrap();
    assert!(alpha_json.contains("alpha-1") && !alpha_json.contains("beta-1"));
    assert!(beta_json.contains("beta-1") && !beta_json.contains("alpha-1"));

    // "Restart": a brand-new engine over the same vault (nothing in memory).
    drop(engine);
    let restarted = new_engine(&root, vault.clone()).await;
    let after = restarted
        .fetch(profiled(&format!("{base}/echo"), "alpha"))
        .await
        .expect("alpha echo after restart");
    assert_eq!(
        after.body, "sid=alpha-1",
        "the session cookie survived the restart via the persisted jar"
    );

    // An unsafe profile name is a typed error, never creates a directory, and is
    // TERMINAL for the job: the name is frozen into the job row at enqueue, so
    // retrying it re-refuses identically. It used to be a retryable
    // `Error::Profile`, which burned the whole backoff ladder on a typo.
    let err = restarted
        .fetch(profiled(&format!("{base}/echo"), "../escape"))
        .await
        .expect_err("unsafe profile name must be rejected");
    assert!(
        matches!(err, pumper_core::Error::BadRequest(_)),
        "got {err:?}"
    );
    assert!(
        err.is_terminal_for_job(),
        "a typo'd profile name must not be retried: {err:?}"
    );

    let _ = std::fs::remove_dir_all(&root);
}

fn temp_root(tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "pumper-vault-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    root
}

/// THE silent-degradation bug. A profiled fetch whose jar does not exist used to
/// be **indistinguishable** from a logged-in one: `ProfileJar::load` mapped
/// `NotFound` to an empty store with no signal, so a mistyped
/// `profile: "acme_portl"` fetched the login wall with a `200`, cleared
/// `min_content_chars`, and was stored as a real dataset revision.
///
/// Two things had to change, and both are asserted here: the response now
/// carries the reserved marker out of the engine (the only channel that survives
/// an engine boundary), and the typo no longer **materialises** a profile
/// directory that `GET /profiles` then reports as real.
#[tokio::test]
async fn a_profiled_fetch_with_no_stored_session_is_marked_and_invents_no_profile() {
    let root = temp_root("absent");
    let vault = root.join("profiles");
    let base = spawn_server().await;
    let engine = new_engine(&root, vault.clone()).await;

    let resp = engine
        .fetch(profiled(&format!("{base}/echo"), "acme_portl"))
        .await
        .expect("an absent jar must not fail the fetch — that is how a login starts");
    assert_eq!(resp.body, "none", "the request really did go out anonymous");
    assert_eq!(
        pumper_core::engine::anonymous_profile(&resp.headers),
        Some("acme_portl"),
        "the response must say the named login carried nothing: {:?}",
        resp.headers
    );

    // The typo must not have invented a profile. Before this, `create_dir_all`
    // ran before the open, so `data/profiles/acme_portl/` existed from here on
    // and `GET /profiles` listed it as a real profile.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(
        !vault.join("acme_portl").exists(),
        "a profile with no session must not appear on disk: {}",
        vault.join("acme_portl").display()
    );

    // ...and a fetch that DOES pick up a session stops being marked, on the very
    // next request, without a restart.
    engine
        .fetch(profiled(&format!("{base}/login?sid=real-1"), "acme"))
        .await
        .expect("login");
    let after = engine
        .fetch(profiled(&format!("{base}/echo"), "acme"))
        .await
        .expect("echo");
    assert_eq!(after.body, "sid=real-1");
    assert_eq!(
        pumper_core::engine::anonymous_profile(&after.headers),
        None,
        "a profile that carries a session must not be flagged"
    );
    // And that one IS a real profile, so it is on disk.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(vault.join("acme").join("cookies.json").is_file());

    let _ = std::fs::remove_dir_all(&root);
}

/// THE lost-login bug. `flush_loop` cleared `dirty` **before** saving, so a
/// transient write failure — a Windows sharing violation while a backup or an
/// antivirus holds the file, the exact case that keeps `Error::Profile`
/// retryable — left the flag `false` and the cookie was **never written**: the
/// user stayed logged in for the life of the process and was silently logged out
/// by the restart, with one WARN as the only evidence.
///
/// Blocked here by putting a regular **file** where the profile directory needs
/// to go, so `create_dir_all` fails; removing it must let the pending write land
/// on a later flush cycle.
#[tokio::test]
async fn a_failed_jar_save_is_retried_instead_of_losing_the_login() {
    let root = temp_root("saveretry");
    let vault = root.join("profiles");
    std::fs::create_dir_all(&vault).expect("vault");
    let blocker = vault.join("blocked");
    std::fs::write(&blocker, b"not a directory").expect("blocker");
    let base = spawn_server().await;
    let engine = new_engine(&root, vault.clone()).await;

    engine
        .fetch(profiled(&format!("{base}/login?sid=persist-me"), "blocked"))
        .await
        .expect("login succeeds in memory even though the jar cannot be written");

    // One full debounce: the first save has failed by now.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert!(
        blocker.is_file(),
        "the blocker is still in the way, so nothing can have been written"
    );

    // The transient condition clears — an antivirus lets go, a backup finishes.
    std::fs::remove_file(&blocker).expect("unblock");

    // The write must land on a later cycle, without any new request touching the
    // jar. Before the fix the dirty flag was already false and this never
    // happened, however long you waited.
    let jar = vault.join("blocked").join("cookies.json");
    for _ in 0..12 {
        if jar.is_file() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        jar.is_file(),
        "the pending cookie write was dropped instead of retried: {}",
        jar.display()
    );
    assert!(std::fs::read_to_string(&jar)
        .unwrap()
        .contains("persist-me"));

    let _ = std::fs::remove_dir_all(&root);
}

/// THE clobber sequence. `jar_for` caches the `Arc` and never re-reads disk, and
/// `save` renamed over the path unconditionally — so: start the server while
/// `cookies.json` is missing (in-memory jar empty), an operator restores the file
/// from backup, the next profiled response `touch`es the jar, and the debounced
/// flush **overwrites the restored session with the empty one**, logging
/// `cookie jar saved`.
#[tokio::test]
async fn an_empty_in_memory_jar_cannot_clobber_a_restored_cookie_file() {
    let root = temp_root("clobber");
    let vault = root.join("profiles");
    let base = spawn_server().await;

    // A real jar, produced the way a real one is (a login under a second vault),
    // standing in for the operator's backup.
    let donor_root = temp_root("clobber-donor");
    let donor_vault = donor_root.join("profiles");
    let donor = new_engine(&donor_root, donor_vault.clone()).await;
    donor
        .fetch(profiled(&format!("{base}/login?sid=restored-1"), "vip"))
        .await
        .expect("donor login");
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let backup = donor_vault.join("vip").join("cookies.json");
    assert!(backup.is_file(), "donor jar written");

    // The server starts with NO jar for `vip`, and opens it (empty) on a fetch.
    let engine = new_engine(&root, vault.clone()).await;
    let anon = engine
        .fetch(profiled(&format!("{base}/echo"), "vip"))
        .await
        .expect("first fetch");
    assert_eq!(anon.body, "none", "the in-memory jar really is empty");

    // The operator restores the backup while the process is running.
    std::fs::create_dir_all(vault.join("vip")).expect("profile dir");
    let restored = vault.join("vip").join("cookies.json");
    std::fs::copy(&backup, &restored).expect("restore");

    // Another profiled response touches the (still empty, still cached) jar.
    engine
        .fetch(profiled(&format!("{base}/echo"), "vip"))
        .await
        .expect("second fetch");
    tokio::time::sleep(Duration::from_millis(1_800)).await;

    assert!(
        std::fs::read_to_string(&restored)
            .expect("the restored jar must still exist")
            .contains("restored-1"),
        "an empty in-memory jar overwrote the restored session"
    );

    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&donor_root);
}
