//! Panic containment at the **HTTP layer**: a handler that unwinds answers with
//! the service's ordinary error envelope instead of dropping the connection.
//!
//! Distinct from `panic_containment`, which covers the *worker's* half (a
//! panicking app fails its job through the attempt-fenced `fail()` path). The
//! two are independent: the worker runs in a spawned task that never passes
//! through the router's middleware, so neither containment can mask the other.
//!
//! Before this, a panic anywhere on a request path unwound out of the connection
//! task: the client saw a reset with no status and no body — indistinguishable
//! from the process having died — and `tracing` recorded nothing at all.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::Router;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use super::harness::test_state;
use crate::routes;

/// A router carrying the **exact** middleware stack `routes::router` applies —
/// same function, same order — plus routes that unwind on purpose. Building it
/// this way is the point: a hand-assembled stack that merely looks like the real
/// one would prove nothing about the shipped server.
fn panicking_router() -> Router {
    // Named handlers with real return types: the two payload shapes `panic!`
    // produces are a `&'static str` (literal) and a `String` (formatted), and
    // both have to survive the trip to the log.
    async fn boom_literal() -> &'static str {
        panic!("handler hit an unwrap")
    }
    async fn boom_formatted() -> &'static str {
        let missing = "verdict";
        panic!("no {missing} for source {}", 7)
    }
    async fn fine() -> &'static str {
        "ok"
    }
    routes::with_middleware(
        Router::new()
            .route("/boom-literal", get(boom_literal))
            .route("/boom-formatted", get(boom_formatted))
            .route("/fine", get(fine)),
        Vec::new(),
    )
}

async fn get_response(router: &Router, uri: &str) -> (StatusCode, Value) {
    let resp = router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .expect(
            "the request must RESOLVE — a panic escaping the stack shows up here as an error, \
             which over a real socket is the connection reset this layer exists to prevent",
        );
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn a_panicking_handler_answers_a_500_envelope_not_a_connection_reset() {
    let router = panicking_router();
    for uri in ["/boom-literal", "/boom-formatted"] {
        let (status, body) = get_response(&router, uri).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{uri}");
        // The SAME envelope every other failure uses, so a client's error path
        // needs no special case for "the server crashed on this route".
        assert_eq!(body["code"], "internal", "{uri}: {body}");
        assert!(body["error"].is_string(), "{uri}: {body}");
        // And the panic's own text stays server-side: it is a stack detail, and
        // this envelope is the one shown to an unauthenticated caller.
        assert!(
            !body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("unwrap"),
            "{uri}: the panic payload must not be echoed to the client: {body}"
        );
    }
}

/// One panicking route must not poison the rest of the surface, and the layer
/// must be inert on the overwhelming majority of requests that never panic.
#[tokio::test]
async fn containment_does_not_change_a_healthy_response() {
    let router = panicking_router();
    let (status, _) = get_response(&router, "/boom-literal").await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let resp = router
        .clone()
        .oneshot(Request::builder().uri("/fine").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"ok");
}

/// The stack is on the REAL router too — not just on the test's copy of it.
/// A normal request through `routes::router` still behaves exactly as before,
/// which is the "no behaviour change for non-panic paths" half of the contract.
#[tokio::test]
async fn the_real_router_still_serves_its_normal_routes() {
    let (state, _store) = test_state(vec![]).await;
    let router = routes::router(state);
    let (status, body) = get_response(&router, "/health").await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
