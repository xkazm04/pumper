//! Node identity (N16): the ed25519 keypair this deployment signs mesh bundles
//! with, and the `node_id` every bundle carries.
//!
//! ## What changed, and what did not
//!
//! Before this, `node_id` was a `DefaultHasher` of the database path
//! (`routes/host_weather.rs`, v1) — enough to tell two deployments apart in a
//! log line, and explicitly "not a security boundary (nothing is signed)". It
//! is now the **fingerprint of a public key**: 16 bytes of SHA-256 over the
//! ed25519 public key, hex ([`app_peer::envelope::fingerprint`]). The old value
//! survives for one release as `legacy_id` on `GET /node` and in the export
//! bundles, so an operator upgrading a fleet can map the id they had pinned in
//! notes/dashboards onto the new one instead of guessing.
//!
//! ## The key file
//!
//! `data/node.key` — literally, the sibling of the SQLite database, since that
//! is what "this deployment's state" already means here (`[storage]
//! database_path`). It holds the hex of a PKCS#8 v2 document. It is created on
//! first use with `0600` where the platform has such a thing, and it is NEVER
//! silently regenerated: a key file that exists but cannot be parsed is a hard
//! error, because quietly minting a new identity would change this node's id
//! and make every peer that pinned it refuse every bundle it sends — a
//! failure that would look like "the mesh stopped working" and be diagnosed
//! nowhere near here.
//!
//! ## Why a process-wide map rather than a field on `AppState`
//!
//! Identity is per-deployment, and `AppState` is where per-deployment things
//! live — but `state.rs` is outside this item's file scope (wave-2 partition),
//! so the identity is memoised in a process-global map keyed by the key file's
//! path. That keying is not a detail: the two-node e2e runs an origin and a
//! mirror in ONE process, and a `OnceLock<NodeIdentity>` would have given them
//! the same key, which is exactly the setup under which a forged-bundle test
//! passes for the wrong reason. Keyed by path, two nodes in one process get two
//! identities, as two nodes must.

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use app_peer::envelope::{fingerprint, signing_bytes};
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{json, Value};

use crate::state::AppState;

/// File name of the key, next to the SQLite database.
const KEY_FILE: &str = "node.key";

/// This node's signing identity.
pub(crate) struct NodeIdentity {
    keypair: Ed25519KeyPair,
    public_key: Vec<u8>,
    node_id: String,
    legacy_id: String,
    key_path: PathBuf,
    /// True when this call is the one that created the key file.
    created: bool,
}

impl NodeIdentity {
    /// The key fingerprint — this node's id on the mesh.
    pub(crate) fn node_id(&self) -> &str {
        &self.node_id
    }

    /// The pre-N16 id (hash of the database path), kept for one release so an
    /// operator can map old pins onto new ones.
    pub(crate) fn legacy_id(&self) -> &str {
        &self.legacy_id
    }

    /// Hex-encoded ed25519 public key — what a peer pins as `public_key`.
    pub(crate) fn public_key_hex(&self) -> String {
        hex::encode(&self.public_key)
    }

    pub(crate) fn key_path(&self) -> &Path {
        &self.key_path
    }

    pub(crate) fn created(&self) -> bool {
        self.created
    }

    /// Wraps `payload` in a signed mesh envelope.
    pub(crate) fn seal(&self, schema: &str, payload: Value) -> Value {
        let generated_at = chrono::Utc::now().to_rfc3339();
        let sig = self.keypair.sign(&signing_bytes(
            schema,
            &self.node_id,
            &generated_at,
            &payload,
        ));
        json!({
            "schema": schema,
            "node_id": self.node_id,
            "generated_at": generated_at,
            // Provenance for a fleet mid-upgrade: the id a peer may still have
            // pinned from the unsigned era. Inside the signature, so it cannot
            // be swapped in transit.
            "legacy_id": self.legacy_id,
            "payload": payload,
            "sig": hex::encode(sig.as_ref()),
        })
    }
}

/// The pre-N16 node id: a `DefaultHasher` of the database path.
///
/// Byte-for-byte the v1 expression from `routes/host_weather.rs`, kept as a
/// named function so the two can be proven identical rather than assumed —
/// `legacy_id` is only useful if it is the value operators actually saw.
pub(crate) fn legacy_node_id(database_path: &Path) -> String {
    let mut h = DefaultHasher::new();
    database_path.hash(&mut h);
    format!("{:016x}", h.finish())
}

/// Where this deployment's key lives: `node.key` beside the database.
pub(crate) fn node_key_path(database_path: &Path) -> PathBuf {
    database_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("data"))
        .join(KEY_FILE)
}

type IdentityCache = Mutex<HashMap<PathBuf, Arc<NodeIdentity>>>;

fn cache() -> &'static IdentityCache {
    static CACHE: OnceLock<IdentityCache> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// This node's identity, loading or creating the key file on first call.
///
/// Memoised per key path (see the module docs). A poisoned cache mutex is
/// recovered from rather than propagated: identity is read on request paths,
/// and a panic elsewhere must not turn every later `GET /node` into a 500.
pub(crate) fn identity(state: &AppState) -> anyhow::Result<Arc<NodeIdentity>> {
    let db = state.config.storage.database_path.clone();
    let path = node_key_path(&db);
    let mut guard = cache().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(found) = guard.get(&path) {
        return Ok(found.clone());
    }
    let id = Arc::new(load_or_create(&path, &db)?);
    guard.insert(path, id.clone());
    Ok(id)
}

/// Reads the key file, or mints one. Never regenerates over an unreadable file.
fn load_or_create(path: &Path, database_path: &Path) -> anyhow::Result<NodeIdentity> {
    let (pkcs8, created) = match std::fs::read_to_string(path) {
        Ok(text) => {
            let bytes = hex::decode(text.trim()).map_err(|e| {
                anyhow::anyhow!(
                    "node key {} is not readable hex ({e}). REFUSING to mint a new identity: \
                     that would change this node's id and every peer pinning it would reject \
                     its bundles. Restore the file from backup, or delete it deliberately.",
                    path.display()
                )
            })?;
            (bytes, false)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let rng = ring::rand::SystemRandom::new();
            let doc = Ed25519KeyPair::generate_pkcs8(&rng)
                .map_err(|_| anyhow::anyhow!("could not generate an ed25519 keypair"))?;
            let bytes = doc.as_ref().to_vec();
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            std::fs::write(path, hex::encode(&bytes))?;
            restrict(path);
            (bytes, true)
        }
        Err(e) => return Err(e.into()),
    };
    let keypair = Ed25519KeyPair::from_pkcs8(&pkcs8).map_err(|_| {
        anyhow::anyhow!(
            "node key {} is not a valid ed25519 PKCS#8 document. REFUSING to mint a new \
             identity over it — see the file's own docs.",
            path.display()
        )
    })?;
    let public_key = keypair.public_key().as_ref().to_vec();
    Ok(NodeIdentity {
        node_id: fingerprint(&public_key),
        legacy_id: legacy_node_id(database_path),
        public_key,
        keypair,
        key_path: path.to_path_buf(),
        created,
    })
}

/// Owner-only permissions where the platform has them. A best-effort tighten:
/// failing to chmod must not stop a node from booting, and on Windows the
/// containing `data/` directory's ACL is the real control.
fn restrict(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use app_peer::envelope::{open_envelope, MeshError, PeerTrust};

    fn tempdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pumper-node-{tag}-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn the_key_is_created_once_and_reloaded_not_regenerated() {
        let dir = tempdir("reload");
        let db = dir.join("pumper.db");
        let path = node_key_path(&db);
        let first = load_or_create(&path, &db).expect("mint");
        assert!(first.created(), "the first call creates the key");
        let second = load_or_create(&path, &db).expect("reload");
        assert!(!second.created(), "the second call reloads it");
        assert_eq!(
            first.node_id(),
            second.node_id(),
            "a restart must not change this node's identity"
        );
        assert_eq!(first.public_key_hex(), second.public_key_hex());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_corrupt_key_is_an_error_not_a_new_identity() {
        let dir = tempdir("corrupt");
        let db = dir.join("pumper.db");
        let path = node_key_path(&db);
        std::fs::write(&path, "zzzz not hex zzzz").expect("write");
        let err = match load_or_create(&path, &db) {
            Err(e) => e,
            Ok(_) => panic!("a corrupt key file must not mint a new identity"),
        };
        assert!(
            err.to_string().contains("REFUSING"),
            "the error must say it refused rather than quietly re-keying: {err}"
        );
        // And the file is untouched, so a restore is still possible.
        assert_eq!(
            std::fs::read_to_string(&path).expect("still there").trim(),
            "zzzz not hex zzzz"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn two_nodes_in_one_process_get_two_identities() {
        let dir = tempdir("twonodes");
        let a = load_or_create(
            &node_key_path(&dir.join("a/pumper.db")),
            &dir.join("a/pumper.db"),
        )
        .expect("a");
        let b = load_or_create(
            &node_key_path(&dir.join("b/pumper.db")),
            &dir.join("b/pumper.db"),
        )
        .expect("b");
        assert_ne!(
            a.node_id(),
            b.node_id(),
            "identity is per deployment; sharing one would make a forgery test pass \
             for the wrong reason"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_sealed_bundle_verifies_under_its_own_key_and_not_another() {
        let dir = tempdir("seal");
        let db = dir.join("pumper.db");
        let me = load_or_create(&node_key_path(&db), &db).expect("mint");
        let other_db = dir.join("other/pumper.db");
        let other = load_or_create(&node_key_path(&other_db), &other_db).expect("mint other");

        let env = me.seal("pumper.host-weather/2", json!({"entries": []}));
        let mine = PeerTrust {
            public_key: Some(me.public_key_hex()),
            allow_unsigned: false,
        };
        assert!(
            open_envelope(&env, "pumper.host-weather/2", None, &mine)
                .expect("opens")
                .verified
        );
        let theirs = PeerTrust {
            public_key: Some(other.public_key_hex()),
            allow_unsigned: false,
        };
        match open_envelope(&env, "pumper.host-weather/2", None, &theirs) {
            Err(MeshError::NodeMismatch { .. }) => {}
            other => panic!("another node's key must not open this bundle: {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `legacy_id` is only worth carrying if it is the value operators actually
    /// saw before N16 — the v1 expression, inline, byte for byte.
    #[test]
    fn legacy_id_reproduces_the_v1_expression_not_a_lookalike() {
        let path = PathBuf::from("data/pumper.db");
        let mut h = DefaultHasher::new();
        path.hash(&mut h);
        assert_eq!(legacy_node_id(&path), format!("{:016x}", h.finish()));
    }

    #[test]
    fn the_key_sits_beside_the_database_and_falls_back_to_data() {
        assert_eq!(
            node_key_path(Path::new("var/lib/pumper/pumper.db")),
            PathBuf::from("var/lib/pumper").join(KEY_FILE)
        );
        // A bare filename has no parent directory to speak of; `data/` is the
        // documented default state directory, not the process CWD.
        assert_eq!(
            node_key_path(Path::new("pumper.db")),
            PathBuf::from("data").join(KEY_FILE)
        );
    }
}
