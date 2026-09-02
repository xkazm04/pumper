//! Mesh wire format (N16): signed bundle envelopes and the dataset live-set
//! digest, shared by the two sides of a pull.
//!
//! ## Why this lives in core
//!
//! Both sides of a pull need the exact same bytes-to-sign and the exact same
//! key digest: the serving side (the server's `/host-weather/export`,
//! `/recipes/export`, `/datasets/.../manifest`) and the pulling side (the
//! `peer` app). A second implementation of either is a silent interoperability
//! bug waiting to happen, so there is exactly one — here, in the crate every
//! other crate already depends on. `app-peer` re-exports this module as
//! `app_peer::envelope` for its own call sites; nothing re-implements it.
//!
//! ## The envelope
//!
//! ```json
//! { "schema": "pumper.host-weather/2",
//!   "node_id": "<32 hex fingerprint of the signing key>",
//!   "generated_at": "<RFC 3339>",
//!   "payload": { .. },
//!   "sig": "<128 hex ed25519 signature>" }
//! ```
//!
//! The signature covers [`signing_bytes`]: a domain-separated, canonically
//! serialised join of schema, node id, timestamp and payload. Canonical means
//! **key-sorted, recursively** ([`canonical_json`]) — not `serde_json`'s
//! default, which is only sorted because `preserve_order` happens to be off in
//! this workspace today. A verifier that re-serialised with a different key
//! order would reject every genuine bundle, so the ordering is this module's
//! own property and is tested.
//!
//! `sig` absent = an unsigned legacy bundle (`pumper.host-weather/1`). Those are
//! accepted only under an explicit `allow_unsigned` (see [`PeerTrust`]) — never
//! by default once the operator has configured any peer at all.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::recipes::ApiRecipe;
use crate::tiers::WeatherEntry;

/// Host-weather bundle, signed (N16). The payload is `{min_observations,
/// entries: [WeatherEntry]}`.
pub const SCHEMA_WEATHER_V2: &str = "pumper.host-weather/2";
/// Host-weather bundle, unsigned (M01 v1). Flat `{schema, node_id,
/// generated_at, entries}` — NOT an envelope.
pub const SCHEMA_WEATHER_V1: &str = "pumper.host-weather/1";
/// API-recipe bundle, signed. The payload is `{entries: [..]}`.
pub const SCHEMA_RECIPES_V1: &str = "pumper.recipes/1";
/// Domain separator: signing bytes for one schema can never be replayed as
/// signing bytes for another, nor as any other ed25519 message this node signs.
const SIGNING_DOMAIN: &str = "pumper-mesh-envelope-v1";

/// A parsed mesh envelope. `sig` is `None` for a legacy unsigned bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub schema: String,
    #[serde(default)]
    pub node_id: Option<String>,
    #[serde(default)]
    pub generated_at: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub sig: Option<String>,
}

/// What one configured peer is trusted to say.
#[derive(Debug, Clone, Default)]
pub struct PeerTrust {
    /// The peer's ed25519 public key, 64 hex chars. `None` = the operator has
    /// not pinned a key for this peer.
    pub public_key: Option<String>,
    /// Accept an unsigned (`/1`) or unverifiable bundle from this peer.
    /// Default false.
    pub allow_unsigned: bool,
}

/// Why an envelope was refused. Each variant is a distinct, greppable reason —
/// "the bundle was bad" is not an operable diagnosis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshError {
    /// Not the schema this reader imports.
    SchemaMismatch { got: String, want: String },
    /// Unsigned (or unverifiable), and no trust rule allows that.
    UnsignedRefused,
    /// Signed, but the envelope's `node_id` is not the fingerprint of the key
    /// the operator pinned for this peer.
    NodeMismatch { got: String, want: String },
    /// The pinned key is not a usable ed25519 public key.
    BadKey,
    /// The signature is malformed or does not verify over the signing bytes.
    SignatureInvalid,
    /// Structurally unreadable (missing signature fields, bad hex).
    Malformed(String),
}

impl std::fmt::Display for MeshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeshError::SchemaMismatch { got, want } => {
                write!(
                    f,
                    "unknown bundle schema {got:?}; this build imports {want:?}"
                )
            }
            MeshError::UnsignedRefused => write!(
                f,
                "bundle is unsigned or unverifiable and no peer rule allows it (pin the \
                 peer's public_key, or set allow_unsigned = true on its [[peer]] row)"
            ),
            MeshError::NodeMismatch { got, want } => write!(
                f,
                "bundle node_id {got:?} is not the fingerprint {want:?} of the public_key \
                 pinned for this peer"
            ),
            MeshError::BadKey => write!(f, "the pinned peer public_key is not a valid ed25519 key"),
            MeshError::SignatureInvalid => write!(
                f,
                "bundle signature does not verify against the pinned peer key"
            ),
            MeshError::Malformed(why) => write!(f, "malformed mesh envelope: {why}"),
        }
    }
}

/// A bundle that passed the trust check, with an honest verification verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opened {
    pub node_id: Option<String>,
    pub generated_at: Option<String>,
    pub payload: Value,
    /// True only when a pinned key verified the signature. An accepted-but-
    /// unverified bundle (legacy, or no key pinned) reports `false` rather than
    /// borrowing the word "verified" from a check that did not happen.
    pub verified: bool,
}

/// Recursively key-sorted JSON serialisation. Independent of `serde_json`'s
/// map backend, so enabling `preserve_order` anywhere in the tree cannot make
/// two nodes disagree about what bytes a payload is.
pub fn canonical_json(value: &Value) -> String {
    fn walk(v: &Value) -> Value {
        match v {
            Value::Object(map) => {
                let sorted: BTreeMap<&String, Value> =
                    map.iter().map(|(k, v)| (k, walk(v))).collect();
                let mut out = Map::new();
                for (k, v) in sorted {
                    out.insert(k.clone(), v);
                }
                Value::Object(out)
            }
            Value::Array(items) => Value::Array(items.iter().map(walk).collect()),
            other => other.clone(),
        }
    }
    serde_json::to_string(&walk(value)).unwrap_or_default()
}

/// The exact bytes an envelope's signature covers.
pub fn signing_bytes(schema: &str, node_id: &str, generated_at: &str, payload: &Value) -> Vec<u8> {
    format!(
        "{SIGNING_DOMAIN}\n{schema}\n{node_id}\n{generated_at}\n{}",
        canonical_json(payload)
    )
    .into_bytes()
}

/// The node id derived from an ed25519 public key: the first 16 bytes of its
/// SHA-256, hex. Short enough to read in a log line, long enough that finding a
/// second key with the same id is not a thing an attacker does.
pub fn fingerprint(public_key: &[u8]) -> String {
    let digest = Sha256::digest(public_key);
    hex::encode(&digest[..16])
}

/// [`fingerprint`] over a hex-encoded key. `None` when the hex is unreadable.
pub fn fingerprint_hex(public_key_hex: &str) -> Option<String> {
    hex::decode(public_key_hex.trim())
        .ok()
        .map(|k| fingerprint(&k))
}

/// Verifies an ed25519 signature. Split out so the trust policy in
/// [`open_envelope`] is testable without a keypair.
pub fn verify_signature(public_key: &[u8], message: &[u8], signature: &[u8]) -> bool {
    let key = ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, public_key);
    key.verify(message, signature).is_ok()
}

/// Applies the trust policy to a raw bundle body.
///
/// Precedence, in order:
/// 1. schema must be `expected_schema`, or `legacy_schema` when one is offered;
/// 2. a legacy/unsigned bundle needs `trust.allow_unsigned`;
/// 3. a signed bundle with a pinned key must carry that key's fingerprint as
///    `node_id` AND verify;
/// 4. a signed bundle with NO pinned key is accepted `verified: false` only
///    when `trust.allow_unsigned` — a signature nobody can check is worth
///    exactly as much as no signature, and saying otherwise is the whole
///    failure mode this item exists to close.
pub fn open_envelope(
    body: &Value,
    expected_schema: &str,
    legacy_schema: Option<&str>,
    trust: &PeerTrust,
) -> Result<Opened, MeshError> {
    let schema = body
        .get("schema")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let is_legacy = legacy_schema.is_some_and(|l| l == schema);
    if schema != expected_schema && !is_legacy {
        return Err(MeshError::SchemaMismatch {
            got: schema,
            want: expected_schema.to_string(),
        });
    }
    let node_id = body
        .get("node_id")
        .and_then(Value::as_str)
        .map(str::to_string);
    let generated_at = body
        .get("generated_at")
        .and_then(Value::as_str)
        .map(str::to_string);
    // A legacy bundle is flat: its whole body IS the payload.
    let payload = if is_legacy {
        body.clone()
    } else {
        body.get("payload").cloned().unwrap_or(Value::Null)
    };
    let sig = body.get("sig").and_then(Value::as_str);

    let checkable = match (sig, trust.public_key.as_deref()) {
        (Some(s), Some(k)) => Some((s, k)),
        _ => None,
    };
    let Some((sig_hex, key_hex)) = checkable else {
        if !trust.allow_unsigned {
            return Err(MeshError::UnsignedRefused);
        }
        return Ok(Opened {
            node_id,
            generated_at,
            payload,
            verified: false,
        });
    };

    let key = hex::decode(key_hex.trim()).map_err(|_| MeshError::BadKey)?;
    if key.len() != 32 {
        return Err(MeshError::BadKey);
    }
    let want = fingerprint(&key);
    let got = node_id.clone().unwrap_or_default();
    if got != want {
        return Err(MeshError::NodeMismatch { got, want });
    }
    let generated = generated_at
        .clone()
        .ok_or_else(|| MeshError::Malformed("a signed envelope must carry generated_at".into()))?;
    let sig_bytes = hex::decode(sig_hex.trim()).map_err(|_| MeshError::SignatureInvalid)?;
    let msg = signing_bytes(&schema, &got, &generated, &payload);
    if !verify_signature(&key, &msg, &sig_bytes) {
        return Err(MeshError::SignatureInvalid);
    }
    Ok(Opened {
        node_id,
        generated_at,
        payload,
        verified: true,
    })
}

// ── dataset live-set digest ─────────────────────────────────────────────────

/// Rolling hash over a dataset's LIVE key set: SHA-256 over the sorted keys,
/// newline-joined, hex. Order-independent by construction (the sort), so an
/// origin and a mirror that hold the same keys agree regardless of the order
/// their stores happened to hand them over in.
///
/// Deliberately over keys ONLY, not values: this digest answers "is the mirror
/// holding records the origin no longer has", which is the ghost question. A
/// value-sensitive digest would go red on every ordinary update and make the
/// reconcile pass cry wolf.
pub fn manifest_digest(keys: &[String]) -> String {
    let mut sorted: Vec<&str> = keys.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted.dedup();
    let mut hasher = Sha256::new();
    for k in &sorted {
        hasher.update(k.as_bytes());
        hasher.update(b"\n");
    }
    hex::encode(hasher.finalize())
}

/// Keys the mirror holds that the origin's live set does not — the ghosts a
/// hard delete on the origin leaves behind (the origin's feed never carried a
/// `removed` revision for them, so no puller could ever have learned of them).
///
/// Pure and set-based so the reconcile decision is testable without two nodes.
pub fn ghost_keys(local_live: &[String], origin_live: &[String]) -> Vec<String> {
    let origin: std::collections::HashSet<&str> = origin_live.iter().map(String::as_str).collect();
    let mut ghosts: Vec<String> = local_live
        .iter()
        .filter(|k| !origin.contains(k.as_str()))
        .cloned()
        .collect();
    ghosts.sort();
    ghosts.dedup();
    ghosts
}

// ── bundle shapes ───────────────────────────────────────────────────────────

/// Reads `entries` out of an OPENED weather payload.
///
/// Typed only after the envelope verified, never before: the signature is over
/// the bytes, so letting serde read the body first would put the parser ahead of
/// the verifier.
pub fn weather_entries(payload: &Value) -> std::result::Result<Vec<WeatherEntry>, String> {
    let raw = payload
        .get("entries")
        .ok_or_else(|| "bundle payload has no `entries` array".to_string())?;
    serde_json::from_value(raw.clone()).map_err(|e| format!("bundle `entries` is unreadable: {e}"))
}

/// Keeps only the recipe fields a peer can act on.
///
/// Two columns are deliberately dropped: `validated` is a claim about a replay
/// THIS node made from ITS egress IP, and `consecutive_failures` counts strikes
/// against a host from here. A peer adopting either would inherit a verdict it
/// never earned. The origin's flag travels as `validated_at_origin` —
/// provenance, not permission.
pub fn exportable_recipe(row: &Value) -> Option<Value> {
    let host = row.get("host").and_then(Value::as_str)?;
    let url_template = row.get("url_template").and_then(Value::as_str)?;
    if host.trim().is_empty() || url_template.trim().is_empty() {
        return None;
    }
    Some(json!({
        "host": host,
        "url_template": url_template,
        "params": row.get("params").cloned().unwrap_or(Value::Null),
        "json_paths": row.get("json_paths").cloned().unwrap_or(Value::Null),
        "score": row.get("score").cloned().unwrap_or(Value::Null),
        "validated_at_origin": row.get("validated").cloned().unwrap_or(Value::Bool(false)),
    }))
}

/// Turns one bundle entry into a LOCAL candidate.
///
/// Lossy in one direction on purpose: every imported recipe lands
/// `validated = false`, whatever the origin claimed. The local validator proves
/// it here, cheaply, exactly as it would prove a locally-discovered candidate.
pub fn importable_recipe(entry: &Value) -> std::result::Result<ApiRecipe, String> {
    let host = entry
        .get("host")
        .and_then(Value::as_str)
        .map(|h| h.trim().to_lowercase())
        .filter(|h| !h.is_empty())
        .ok_or_else(|| "recipe entry has no host".to_string())?;
    let url_template = entry
        .get("url_template")
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|u| !u.trim().is_empty())
        .ok_or_else(|| format!("recipe entry for {host} has no url_template"))?;
    if !(url_template.starts_with("http://") || url_template.starts_with("https://")) {
        return Err(format!(
            "recipe entry for {host}: url_template {url_template:?} is not an http(s) URL"
        ));
    }
    let json_paths: Vec<String> = entry
        .get("json_paths")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    Ok(ApiRecipe {
        // Empty: the store mints a LOCAL id. Carrying the origin's would make
        // two nodes' primary keys collide the first time both discovered the
        // same endpoint independently.
        id: String::new(),
        host,
        url_template,
        params: entry.get("params").cloned().unwrap_or(Value::Null),
        json_paths,
        score: entry.get("score").and_then(Value::as_f64).unwrap_or(0.0),
        validated: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_json_sorts_keys_at_every_depth_not_only_the_top() {
        let a = json!({"b": {"z": 1, "a": 2}, "a": [ {"y": 1, "x": 2} ]});
        let b = json!({"a": [ {"x": 2, "y": 1} ], "b": {"a": 2, "z": 1}});
        assert_eq!(canonical_json(&a), canonical_json(&b));
        assert_eq!(
            canonical_json(&a),
            r#"{"a":[{"x":2,"y":1}],"b":{"a":2,"z":1}}"#
        );
    }

    #[test]
    fn signing_bytes_are_domain_separated_per_schema_not_shared() {
        let p = json!({"n": 1});
        assert_ne!(
            signing_bytes(SCHEMA_WEATHER_V2, "n", "t", &p),
            signing_bytes(SCHEMA_RECIPES_V1, "n", "t", &p),
            "one schema's signature must not be replayable as another's"
        );
    }

    #[test]
    fn fingerprint_is_key_specific_and_short() {
        let a = fingerprint(&[1u8; 32]);
        let b = fingerprint(&[2u8; 32]);
        assert_ne!(a, b);
        assert_eq!(a.len(), 32);
        assert_eq!(
            fingerprint_hex(&hex::encode([1u8; 32])).as_deref(),
            Some(a.as_str())
        );
        assert!(fingerprint_hex("not hex").is_none());
    }

    /// The keypair used by the signing tests. Generated here rather than
    /// hard-coded so the test proves the real sign/verify path, not a fixture.
    fn keypair() -> (Vec<u8>, ring::signature::Ed25519KeyPair) {
        use ring::signature::KeyPair;
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = ring::signature::Ed25519KeyPair::generate_pkcs8(&rng).expect("generate");
        let kp = ring::signature::Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse");
        let public = kp.public_key().as_ref().to_vec();
        (public, kp)
    }

    fn sealed(schema: &str, payload: Value) -> (Value, String) {
        let (public, kp) = keypair();
        let node_id = fingerprint(&public);
        let generated_at = "2026-09-01T00:00:00Z";
        let sig = kp.sign(&signing_bytes(schema, &node_id, generated_at, &payload));
        (
            json!({
                "schema": schema,
                "node_id": node_id,
                "generated_at": generated_at,
                "payload": payload,
                "sig": hex::encode(sig.as_ref()),
            }),
            hex::encode(public),
        )
    }

    #[test]
    fn a_genuine_signed_bundle_opens_verified() {
        let (env, key) = sealed(SCHEMA_WEATHER_V2, json!({"entries": [{"host": "a.com"}]}));
        let trust = PeerTrust {
            public_key: Some(key),
            allow_unsigned: false,
        };
        let opened = open_envelope(&env, SCHEMA_WEATHER_V2, None, &trust).expect("opens");
        assert!(opened.verified);
        assert_eq!(opened.payload["entries"][0]["host"], "a.com");
    }

    #[test]
    fn a_forged_payload_is_refused_not_opened() {
        let (mut env, key) = sealed(SCHEMA_WEATHER_V2, json!({"entries": [{"host": "a.com"}]}));
        // The classic attack: keep the signature, swap the content.
        env["payload"] = json!({"entries": [{"host": "evil.example", "penalty_ms": 999999}]});
        let trust = PeerTrust {
            public_key: Some(key),
            allow_unsigned: false,
        };
        assert_eq!(
            open_envelope(&env, SCHEMA_WEATHER_V2, None, &trust),
            Err(MeshError::SignatureInvalid)
        );
    }

    #[test]
    fn a_bundle_signed_by_another_key_is_refused_even_with_a_matching_node_id() {
        let (mut env, _key) = sealed(SCHEMA_WEATHER_V2, json!({"entries": []}));
        let (other_public, _other) = keypair();
        // Attacker pins their own node_id AND we pin their key: the signature
        // was made by neither, so it must not verify.
        env["node_id"] = json!(fingerprint(&other_public));
        let trust = PeerTrust {
            public_key: Some(hex::encode(&other_public)),
            allow_unsigned: false,
        };
        assert_eq!(
            open_envelope(&env, SCHEMA_WEATHER_V2, None, &trust),
            Err(MeshError::SignatureInvalid)
        );
    }

    #[test]
    fn a_signed_bundle_from_an_unpinned_node_is_a_mismatch_not_a_pass() {
        let (env, _key) = sealed(SCHEMA_WEATHER_V2, json!({"entries": []}));
        let (other_public, _o) = keypair();
        let trust = PeerTrust {
            public_key: Some(hex::encode(&other_public)),
            allow_unsigned: false,
        };
        match open_envelope(&env, SCHEMA_WEATHER_V2, None, &trust) {
            Err(MeshError::NodeMismatch { .. }) => {}
            other => panic!("expected NodeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn an_unsigned_bundle_is_refused_unless_allow_unsigned_not_accepted_by_default() {
        let legacy = json!({
            "schema": SCHEMA_WEATHER_V1,
            "node_id": "abc",
            "entries": [{"host": "a.com"}],
        });
        let strict = PeerTrust::default();
        assert_eq!(
            open_envelope(&legacy, SCHEMA_WEATHER_V2, Some(SCHEMA_WEATHER_V1), &strict),
            Err(MeshError::UnsignedRefused)
        );
        let lenient = PeerTrust {
            public_key: None,
            allow_unsigned: true,
        };
        let opened = open_envelope(
            &legacy,
            SCHEMA_WEATHER_V2,
            Some(SCHEMA_WEATHER_V1),
            &lenient,
        )
        .expect("legacy opens under allow_unsigned");
        assert!(!opened.verified, "an unsigned bundle is never 'verified'");
        assert_eq!(opened.payload["entries"][0]["host"], "a.com");
    }

    #[test]
    fn a_signature_nobody_can_check_is_not_a_verification() {
        let (env, _key) = sealed(SCHEMA_WEATHER_V2, json!({"entries": []}));
        // Signed, but no key pinned: strict refuses, lenient accepts UNVERIFIED.
        assert_eq!(
            open_envelope(&env, SCHEMA_WEATHER_V2, None, &PeerTrust::default()),
            Err(MeshError::UnsignedRefused)
        );
        let opened = open_envelope(
            &env,
            SCHEMA_WEATHER_V2,
            None,
            &PeerTrust {
                public_key: None,
                allow_unsigned: true,
            },
        )
        .expect("opens");
        assert!(!opened.verified);
    }

    #[test]
    fn a_legacy_schema_is_not_accepted_where_none_is_offered() {
        let legacy = json!({"schema": SCHEMA_WEATHER_V1, "entries": []});
        match open_envelope(
            &legacy,
            SCHEMA_RECIPES_V1,
            None,
            &PeerTrust {
                public_key: None,
                allow_unsigned: true,
            },
        ) {
            Err(MeshError::SchemaMismatch { .. }) => {}
            other => panic!("expected SchemaMismatch, got {other:?}"),
        }
    }

    #[test]
    fn manifest_digest_is_order_independent_but_membership_sensitive() {
        let a = manifest_digest(&["b".into(), "a".into(), "c".into()]);
        let b = manifest_digest(&["c".into(), "b".into(), "a".into()]);
        assert_eq!(a, b, "the digest must not depend on row order");
        assert_ne!(
            a,
            manifest_digest(&["a".into(), "b".into()]),
            "a missing key must move the digest"
        );
        // A duplicate is not a difference: a live set is a set.
        assert_eq!(
            a,
            manifest_digest(&["a".into(), "a".into(), "b".into(), "c".into()])
        );
    }

    #[test]
    fn ghosts_are_local_only_keys_not_every_difference() {
        let local = vec!["a".to_string(), "b".to_string(), "ghost".to_string()];
        let origin = vec!["a".to_string(), "b".to_string(), "fresh".to_string()];
        assert_eq!(ghost_keys(&local, &origin), vec!["ghost".to_string()]);
        // A key the origin has and the mirror does not is NOT a ghost — it is a
        // pull the mirror has not made yet, and tombstoning it would be wrong.
        assert!(ghost_keys(&origin, &origin).is_empty());
    }

    #[test]
    fn an_imported_recipe_is_never_validated_however_loudly_the_bundle_claims_it() {
        let entry = json!({
            "host": "API.Example",
            "url_template": "https://api.example/v1?q={q}",
            "json_paths": ["$.items[*].title"],
            "score": 0.9,
            "validated": true,
            "validated_at_origin": true,
        });
        let r = importable_recipe(&entry).expect("imports");
        assert!(!r.validated);
        assert_eq!(r.host, "api.example");
        assert!(
            r.id.is_empty(),
            "a local id avoids a cross-node PK collision"
        );
    }

    #[test]
    fn a_non_http_template_is_refused_not_stored() {
        let err =
            importable_recipe(&json!({"host": "a.example", "url_template": "file:///etc/passwd"}))
                .expect_err("must refuse");
        assert!(err.contains("not an http(s) URL"), "{err}");
        assert!(importable_recipe(&json!({"url_template": "https://a/"})).is_err());
        assert!(importable_recipe(&json!({"host": "a"})).is_err());
    }

    #[test]
    fn an_exported_recipe_drops_this_nodes_local_verdicts() {
        let row = json!({
            "id": "local-uuid",
            "host": "api.example",
            "url_template": "https://api.example/v1",
            "validated": true,
            "validation_reason": "replay ok",
            "consecutive_failures": 3,
        });
        let out = exportable_recipe(&row).expect("exports");
        assert!(out.get("id").is_none());
        assert!(out.get("validated").is_none());
        assert!(out.get("consecutive_failures").is_none());
        assert_eq!(out["validated_at_origin"], true);
        assert!(exportable_recipe(&json!({"host": "a", "url_template": " "})).is_none());
    }
}
