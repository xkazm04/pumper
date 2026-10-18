//! The artifact path-traversal guard, proved to be ONE guard.
//!
//! `crates/core/src/app.rs` has two doors onto the artifacts tree:
//! `AppContext::save_artifact` (write, name composed from job params — census
//! builds `cbp-{naics}.json`) and `AppContext::read_source_artifact` (read,
//! `source_app` / `job_id` / `artifact_path` all lifted out of untrusted record
//! data). Both must reject anything that is not a single safe path segment, or
//! a `..` escapes the per-job directory.
//!
//! The unit test in `app.rs` covers `safe_path_segment` itself, and the read
//! door calls it. The write door used to re-type the same six clauses inline —
//! one rule, two implementations, a test on one of them — so hardening the
//! shared guard would silently have left `save_artifact` on the old rule and
//! every assertion in the suite would still have been green.
//!
//! This test's job is therefore not "does `save_artifact` reject `..`" (the
//! inline copy did too). It is: **is `save_artifact` reaching the SHARED
//! guard**. Gut `safe_path_segment` to `Ok(())` and this file must go red.

use pumper_core::testing::{TempStore, TestContext};

/// Every escape shape the shared guard names, driven through the WRITE door.
///
/// Seed `safe_path_segment` to `Ok(())` and each of these writes a file outside
/// the job's artifacts directory instead of erroring — which is exactly what
/// this test exists to notice.
#[tokio::test]
async fn save_artifact_rejects_every_escape_shape_through_the_shared_guard() {
    let store = TempStore::new("artifact-path-guard").await;
    let dir = store.path().join("artifacts").join("guard").join("job");
    let ctx = TestContext::new(&store.storage, "guard")
        .artifacts_dir(dir.clone())
        .build();

    for bad in [
        "",
        ".",
        "..",
        "a/b",
        "a\\b",
        "/etc/passwd",
        "..\\up",
        "C:\\x",
        "/",
        "../../escaped.json",
    ] {
        let err = ctx
            .save_artifact(bad, b"payload")
            .await
            .expect_err(&format!("must refuse {bad:?}"));
        assert!(
            err.to_string().contains("unsafe artifact name"),
            "the refusal must name the artifact door, not leak some inner io \
             error: {err}"
        );
    }

    // Nothing was created anywhere: a refused name must not have touched the
    // filesystem on its way to the error.
    assert!(
        !dir.exists(),
        "a run whose every artifact name was refused leaves no directory behind"
    );
    assert!(
        !store.path().join("escaped.json").exists(),
        "no write escaped the artifacts directory"
    );
}

/// The write door still accepts the names apps actually use — the guard is a
/// segment check, not a charset policy. `census-density` composes
/// `cbp-{naics}.json` from job params and `xray` composes
/// `network-capture-<sha>.json`; both must clear it.
#[tokio::test]
async fn save_artifact_accepts_the_names_apps_compose_from_params() {
    let store = TempStore::new("artifact-path-guard-ok").await;
    let dir = store.path().join("artifacts").join("guard").join("job");
    let ctx = TestContext::new(&store.storage, "guard")
        .artifacts_dir(dir.clone())
        .build();

    for good in [
        "page1.json",
        "cbp-541.json",
        "network-capture-0123456789abcdef.json",
        "induced-ruleset.json",
        "a.b_c-d",
        ".hidden",
        "café.html",
    ] {
        let path = ctx
            .save_artifact(good, b"payload")
            .await
            .unwrap_or_else(|e| panic!("must accept {good:?}: {e}"));
        assert_eq!(
            path.parent(),
            Some(dir.as_path()),
            "an accepted artifact lands inside the job's own directory"
        );
        assert!(path.exists(), "{good:?} was not written");
    }
}
