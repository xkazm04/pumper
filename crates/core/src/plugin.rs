//! Sandboxed plugin capability. Apps run named WebAssembly modules over
//! documents; the implementation (`engine-wasm`) executes them with a CPU-fuel
//! budget and a hard memory cap, with no ambient authority (no filesystem or
//! network unless granted). This makes it safe to run **untrusted,
//! hot-swappable** extraction/transform logic in-process — a capability Python
//! has no equivalent for (`exec`/`RestrictedPython` are escapable; real
//! isolation needs a separate process/container).
//!
//! `core` defines only the trait; the wasmtime dependency lives in `engine-wasm`
//! so the runtime stays out of the shared crate.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::PluginFailure;
use crate::{Error, Result};

/// What one plugin call **cost**, as measured by the host that ran it.
///
/// The sandbox enforced a CPU-fuel budget and a memory cap from the day it
/// existed and reported neither, so nobody could see how close a plugin ran to
/// its limits — the `plugin` app's observatory said so in its own module docs
/// and substituted wall-clock elapsed time for cost. Wall clock measures the
/// machine's load as much as the plugin's appetite; fuel is deterministic, which
/// is exactly what a "did this get more expensive?" comparison needs.
///
/// Every field is `Option` because "this host does not meter" is a different
/// fact from "this call was free", and a zeroed cost that reads as free is the
/// specific lie this type exists to avoid.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize)]
pub struct PluginRunStats {
    /// CPU fuel consumed: the budget minus what remained when the call returned.
    pub fuel_used: Option<u64>,
    /// The budget it ran against, so "how close to the ceiling" is answerable
    /// from the same object rather than from a second config lookup.
    pub fuel_budget: Option<u64>,
    /// Linear-memory high-water in bytes. Wasm memory only grows within a store
    /// and every call gets a fresh store, so the size after the call IS this
    /// call's high-water — no sampling needed.
    pub memory_bytes: Option<usize>,
    /// The cap that memory ran against.
    pub memory_cap_bytes: Option<usize>,
}

impl PluginRunStats {
    /// A host that does not meter. Distinct from a metered zero.
    pub const fn unmetered() -> Self {
        Self {
            fuel_used: None,
            fuel_budget: None,
            memory_bytes: None,
            memory_cap_bytes: None,
        }
    }

    /// Whether this carries a real measurement — i.e. whether a consumer may use
    /// fuel as its cost signal rather than falling back to wall clock.
    pub fn is_metered(&self) -> bool {
        self.fuel_used.is_some()
    }

    /// How much of the fuel budget this call used, in `[0, 1]`. `None` when
    /// unmetered or when the budget is zero (which would make the ratio a
    /// division by nothing rather than "100% used").
    pub fn fuel_fraction(&self) -> Option<f64> {
        match (self.fuel_used, self.fuel_budget) {
            (Some(used), Some(budget)) if budget > 0 => Some(used as f64 / budget as f64),
            _ => None,
        }
    }
}

#[async_trait]
pub trait Plugins: Send + Sync {
    /// Runs plugin `name` over `input` with a `params` envelope, returning its
    /// JSON output. Enforces the configured fuel and memory limits; a runaway
    /// plugin traps rather than hanging the host. `params` lets one plugin be
    /// reused across jobs with different config (e.g. a different selector)
    /// instead of recompiling a module per variation; a plugin that only exports
    /// the legacy `extract` receives just the document and ignores `params`.
    async fn run(&self, name: &str, input: &str, params: &Value) -> Result<Value>;

    /// [`run`](Plugins::run), plus what the call cost.
    ///
    /// A separate method with a default impl rather than a widened `run` return
    /// type, deliberately: the hook path (`crates/server/src/triggers.rs`) and
    /// every stub host in the test suite want the value and nothing else, and
    /// making them all unpack a tuple they discard would be churn that buys
    /// nothing. Hosts that cannot meter — `NoPlugins`, in-process stubs — get
    /// this default and implement nothing; the wasmtime host overrides it and
    /// routes its own `run` through it.
    ///
    /// Note the bound: stats describe a call that **returned**. A call that
    /// trapped propagates the error, and the fuel it burned on the way is not
    /// carried (see `docs/features/extraction.md`).
    async fn run_metered(
        &self,
        name: &str,
        input: &str,
        params: &Value,
    ) -> Result<(Value, PluginRunStats)> {
        Ok((
            self.run(name, input, params).await?,
            PluginRunStats::unmetered(),
        ))
    }

    /// Names of currently loaded plugins.
    fn list(&self) -> Vec<String>;

    /// Whether `name` is currently loaded — i.e. whether [`run`](Plugins::run)
    /// would find a module at all, as opposed to failing with "unknown
    /// plugin". Callers whose failure semantics are FAIL-OPEN (trigger hooks)
    /// need this: a trap and a plugin that was never deployed both end as a
    /// passed-through event, and only the second one means "your build/install
    /// step never ran". The default answers from [`list`](Plugins::list);
    /// hosts with an index should override it — this sits on the per-event
    /// hot path.
    fn has(&self, name: &str) -> bool {
        self.list().iter().any(|n| n == name)
    }

    /// Per-plugin metadata for `GET /plugins`: each entry is at least
    /// `{"name": ...}`, enriched with a plugin's self-describing manifest
    /// (`{name, version, description, params_schema, output_schema}`) when it
    /// exports `describe`. Default: name-only entries from [`list`].
    fn manifests(&self) -> Vec<Value> {
        self.list()
            .into_iter()
            .map(|name| serde_json::json!({ "name": name }))
            .collect()
    }

    /// Rescans the plugin directory (hot-swap); returns the loaded count.
    async fn reload(&self) -> Result<usize>;
}

/// Fallback host used when WASM plugins are disabled.
pub struct NoPlugins;

#[async_trait]
impl Plugins for NoPlugins {
    async fn run(&self, name: &str, _input: &str, _params: &Value) -> Result<Value> {
        // `Disabled`, not `Unknown`: the name is irrelevant here — NO name would
        // resolve — and the fix is `[plugins] enabled = true`, not a build step.
        // Callers that report missing hooks use the distinction to avoid telling
        // an operator to rebuild a plugin they deliberately switched off.
        Err(Error::plugin(
            PluginFailure::Disabled,
            name,
            "the plugin subsystem is disabled ([plugins] enabled = false)",
        ))
    }
    fn list(&self) -> Vec<String> {
        Vec::new()
    }
    async fn reload(&self) -> Result<usize> {
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::{NoPlugins, PluginRunStats, Plugins};
    use crate::error::PluginFailure;

    /// The distinction the whole type exists for: a host that does not measure
    /// must not report a cost of zero, which reads as "this ran for free".
    #[test]
    fn unmetered_is_not_a_metered_zero() {
        let none = PluginRunStats::unmetered();
        assert!(!none.is_metered());
        assert_eq!(none.fuel_used, None);
        assert_eq!(none.fuel_fraction(), None);

        let free = PluginRunStats {
            fuel_used: Some(0),
            fuel_budget: Some(1_000),
            ..PluginRunStats::unmetered()
        };
        assert!(free.is_metered(), "a measured 0 IS a measurement");
        assert_eq!(free.fuel_fraction(), Some(0.0));
        assert_ne!(free, none);
    }

    #[test]
    fn fuel_fraction_reports_headroom_and_refuses_a_zero_budget() {
        let s = PluginRunStats {
            fuel_used: Some(750),
            fuel_budget: Some(1_000),
            ..PluginRunStats::unmetered()
        };
        assert_eq!(s.fuel_fraction(), Some(0.75));
        // A zero budget makes the ratio meaningless, not "100% used".
        let s = PluginRunStats {
            fuel_used: Some(0),
            fuel_budget: Some(0),
            ..PluginRunStats::unmetered()
        };
        assert_eq!(s.fuel_fraction(), None);
    }

    /// The default impl must stay honest for a host that cannot meter: same
    /// error, same value, and no invented cost.
    #[tokio::test]
    async fn the_default_metered_impl_reports_no_cost_and_keeps_the_error() {
        let err = NoPlugins
            .run_metered("anything", "doc", &serde_json::Value::Null)
            .await
            .expect_err("plugins are disabled");
        assert_eq!(err.plugin_failure(), Some(PluginFailure::Disabled));
    }
}

// ---- Capabilities (N10) ----------------------------------------------------
//
// The sandbox's default is, and stays, ZERO ambient authority: a module that
// declares no imports gets an empty linker and cannot reach the filesystem or
// the network. What N10 adds is a way for a plugin to ASK, in its own
// `describe()` manifest, for a bounded slice of the outside world — and for the
// host to grant exactly that slice and nothing else.
//
// The enforcement shape matters more than the vocabulary: the manifest is read
// first, the linker is then built from it, and a module that imports a host
// function it did not declare **fails to link at load**. It never half-runs, and
// there is no code path where an undeclared import resolves to a stub.

/// What a plugin declared it needs, parsed out of `describe().capabilities`.
///
/// The `Default` is the pre-N10 sandbox: no network, no key-value store. Every
/// grant is opt-in on BOTH sides — the plugin declares it and the operator
/// allows it (`[plugins] allow_http_hosts`) — because a manifest is written by
/// whoever wrote the plugin, and a capability model where the subject grants
/// itself the capability is not one.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct PluginCapabilities {
    /// Outbound HTTP, bounded by host and method. `None` = no network at all.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpCapability>,
    /// A per-plugin key/value namespace (`pumper_kv_get` / `pumper_kv_put`).
    pub kv: bool,
}

/// The declared HTTP slice: which hosts, which methods. Both are closed lists —
/// an absent/empty `hosts` is not "any host", it is "no HTTP".
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct HttpCapability {
    /// Hostnames this plugin may reach. An entry starting with `.` matches that
    /// domain and its subdomains (`.example.com` covers `api.example.com` and
    /// the bare `example.com`); anything else is an exact, case-insensitive
    /// hostname. `*` is deliberately NOT a value here: a plugin asking for the
    /// whole internet is a plugin whose manifest says nothing.
    pub hosts: Vec<String>,
    /// Uppercase HTTP methods. Defaults to `["GET"]` when the manifest omits
    /// them — the least authority that still makes the capability useful.
    pub methods: Vec<String>,
}

impl PluginCapabilities {
    /// Everything this vocabulary can express. Used ONLY to build the load-time
    /// `describe()` probe's linker, where every import is present so a
    /// capability-using module can be *described*, and every import traps
    /// because the probe has no bridge behind it. Never used to run a plugin.
    pub fn everything() -> Self {
        Self {
            http: Some(HttpCapability {
                hosts: Vec::new(),
                methods: Vec::new(),
            }),
            kv: true,
        }
    }

    pub fn declares_http(&self) -> bool {
        self.http.is_some()
    }

    /// Whether anything at all was granted — the flag `GET /plugins` reads to
    /// tell a plain transformer from a connector.
    pub fn is_empty(&self) -> bool {
        self.http.is_none() && !self.kv
    }
}

/// Reads a `capabilities` block out of a plugin's `describe()` manifest.
///
/// Refuses by NAME rather than degrading silently: a manifest that meant to
/// declare HTTP and misspelled it would otherwise produce a plugin that fails
/// to link with a message about imports, three steps away from the typo. The
/// caller logs the refusal and gives the plugin no capabilities, so the failure
/// is still closed — it is just also *explained*.
pub fn parse_capabilities(manifest: &Value) -> std::result::Result<PluginCapabilities, String> {
    let Some(block) = manifest.get("capabilities") else {
        return Ok(PluginCapabilities::default());
    };
    if block.is_null() {
        return Ok(PluginCapabilities::default());
    }
    let Some(block) = block.as_object() else {
        return Err("`capabilities` must be an object".into());
    };
    let mut caps = PluginCapabilities::default();
    for key in block.keys() {
        if !matches!(key.as_str(), "http" | "kv") {
            return Err(format!(
                "unknown capability '{key}' (this host grants `http` and `kv`)"
            ));
        }
    }
    match block.get("kv") {
        None | Some(Value::Null) => {}
        Some(Value::Bool(b)) => caps.kv = *b,
        Some(other) => return Err(format!("`capabilities.kv` must be a boolean, got {other}")),
    }
    match block.get("http") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => {}
        Some(Value::Object(http)) => {
            let hosts = string_list(http.get("hosts")).map_err(|e| format!("http.hosts: {e}"))?;
            if hosts.is_empty() {
                return Err(
                    "`capabilities.http.hosts` must list at least one host — an \
                            empty list is not 'any host', and there is no wildcard"
                        .into(),
                );
            }
            if hosts.iter().any(|h| h == "*") {
                return Err(
                    "`*` is not a host: name the hosts this plugin talks to, or \
                            `.example.com` for a domain and its subdomains"
                        .into(),
                );
            }
            let mut methods = string_list(http.get("methods"))
                .map_err(|e| format!("http.methods: {e}"))?
                .into_iter()
                .map(|m| m.to_ascii_uppercase())
                .collect::<Vec<_>>();
            if methods.is_empty() {
                methods.push("GET".into());
            }
            caps.http = Some(HttpCapability {
                hosts: hosts.into_iter().map(|h| h.to_ascii_lowercase()).collect(),
                methods,
            });
        }
        Some(other) => {
            return Err(format!(
                "`capabilities.http` must be an object {{hosts, methods}}, got {other}"
            ))
        }
    }
    Ok(caps)
}

/// A JSON array of non-empty strings, or a named refusal. An absent key is an
/// empty list, never an error — the caller decides whether empty is legal.
fn string_list(value: Option<&Value>) -> std::result::Result<Vec<String>, String> {
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let s = item
                    .as_str()
                    .ok_or_else(|| format!("every entry must be a string, got {item}"))?;
                let s = s.trim();
                if !s.is_empty() {
                    out.push(s.to_string());
                }
            }
            Ok(out)
        }
        Some(other) => Err(format!("must be an array of strings, got {other}")),
    }
}

/// Why one outbound request was refused before a socket existed.
///
/// A typed refusal rather than a string because the two allow-lists mean
/// different things to whoever reads the log: `HostNotDeclared` is a plugin
/// that reached past its own manifest (a bug, or an attack), while
/// `HostNotAllowed` is a plugin behaving exactly as declared on a deployment
/// that never opted in (an operator's `[plugins] allow_http_hosts` away).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpDenial {
    /// The manifest declared no `http` capability at all.
    NoCapability,
    /// Not a parseable absolute URL.
    BadUrl(String),
    /// Not `http`/`https` — no `file:`, no `data:`, no custom scheme.
    Scheme(String),
    /// A URL with no host.
    NoHost,
    /// The host is outside the plugin's OWN declared list.
    HostNotDeclared(String),
    /// The host is declared by the plugin but not allowed by the operator.
    HostNotAllowed(String),
    /// The method is outside the plugin's declared list.
    MethodNotDeclared(String),
}

impl std::fmt::Display for HttpDenial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoCapability => write!(
                f,
                "this plugin declares no `http` capability, so it has no network"
            ),
            Self::BadUrl(url) => write!(f, "not an absolute URL: {url}"),
            Self::Scheme(s) => write!(f, "scheme '{s}' is not allowed (http/https only)"),
            Self::NoHost => write!(f, "the URL has no host"),
            Self::HostNotDeclared(h) => write!(
                f,
                "host '{h}' is not in this plugin's declared capabilities.http.hosts"
            ),
            Self::HostNotAllowed(h) => write!(
                f,
                "host '{h}' is not in the operator's [plugins] allow_http_hosts"
            ),
            Self::MethodNotDeclared(m) => write!(
                f,
                "method '{m}' is not in this plugin's declared capabilities.http.methods"
            ),
        }
    }
}

/// The gate every plugin HTTP call passes, as a pure function of the manifest,
/// the operator's list, and the request.
///
/// Both lists must pass. That is the whole design: the manifest bounds what the
/// plugin can ask for (and is written by the plugin's author), the operator list
/// bounds what this deployment will do (and is written by whoever runs it).
/// Either one alone is a capability model with a hole in it.
pub fn authorize_http(
    caps: &PluginCapabilities,
    operator_allow: &[String],
    method: &str,
    url: &str,
) -> std::result::Result<(), HttpDenial> {
    let Some(http) = &caps.http else {
        return Err(HttpDenial::NoCapability);
    };
    let parsed = url::Url::parse(url).map_err(|_| HttpDenial::BadUrl(url.to_string()))?;
    let scheme = parsed.scheme();
    if !matches!(scheme, "http" | "https") {
        return Err(HttpDenial::Scheme(scheme.to_string()));
    }
    let host = parsed
        .host_str()
        .ok_or(HttpDenial::NoHost)?
        .to_ascii_lowercase();
    let method = method.trim().to_ascii_uppercase();
    if !http.methods.iter().any(|m| m == &method) {
        return Err(HttpDenial::MethodNotDeclared(method));
    }
    if !host_matches_any(&host, &http.hosts) {
        return Err(HttpDenial::HostNotDeclared(host));
    }
    if !host_matches_any(&host, operator_allow) {
        return Err(HttpDenial::HostNotAllowed(host));
    }
    Ok(())
}

/// Host-list matching, shared by both lists so a pattern cannot mean one thing
/// in the manifest and another in the config.
///
/// `*` is honored ONLY here, and therefore only from the operator side —
/// `parse_capabilities` refuses it in a manifest. An operator writing
/// `allow_http_hosts = ["*"]` is deliberately delegating the whole decision to
/// the plugins' manifests, which is a choice they can make about their own box;
/// a plugin cannot make it for them.
pub fn host_matches_any(host: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|p| {
        let p = p.trim().to_ascii_lowercase();
        if p == "*" {
            return true;
        }
        if let Some(domain) = p.strip_prefix('.') {
            return host == domain || host.ends_with(&format!(".{domain}"));
        }
        host == p
    })
}

/// One outbound request a plugin asked for (`pumper_http_request`'s JSON input).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct PluginHttpRequest {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
}

fn default_method() -> String {
    "GET".into()
}

/// What came back (`pumper_http_request`'s JSON output).
///
/// `error` and `status` are mutually exclusive by construction: a refusal or a
/// transport failure has no status, and inventing `0` for it would put "the
/// request never happened" and "the server said nothing" in the same bucket.
#[derive(Debug, Clone, serde::Serialize)]
pub struct PluginHttpResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl PluginHttpResponse {
    pub fn ok(status: u16, body: String) -> Self {
        Self {
            status: Some(status),
            body: Some(body),
            error: None,
        }
    }

    pub fn failed(error: impl Into<String>) -> Self {
        Self {
            status: None,
            body: None,
            error: Some(error.into()),
        }
    }
}

/// The host side of the granted capabilities: the actual network and the actual
/// store, behind a trait so `engine-wasm` keeps depending on `core` alone.
///
/// The engine enforces ([`authorize_http`]); this trait only *performs*. Keeping
/// the decision out of the implementation is what makes the decision testable
/// without a socket, and what stops a second implementation from quietly
/// shipping a weaker gate.
#[async_trait]
pub trait PluginCapabilityHost: Send + Sync {
    /// Performs an already-authorized request through the server's metered HTTP
    /// chokepoint (governor, response cache, per-host profiles, body caps).
    async fn http_request(&self, plugin: &str, req: PluginHttpRequest) -> PluginHttpResponse;

    /// Reads `key` from `plugin`'s own namespace. `None` = absent.
    async fn kv_get(&self, plugin: &str, key: &str) -> Option<String>;

    /// Writes `key` in `plugin`'s own namespace.
    async fn kv_put(&self, plugin: &str, key: &str, value: &str)
        -> std::result::Result<(), String>;
}

#[cfg(test)]
mod capability_tests {
    use super::*;
    use serde_json::json;

    fn caps(manifest: serde_json::Value) -> PluginCapabilities {
        parse_capabilities(&manifest).expect("valid manifest")
    }

    /// The pre-N10 sandbox is what a manifest without a `capabilities` block
    /// still gets — every shipped plugin predates the key.
    #[test]
    fn a_manifest_without_capabilities_gets_none() {
        let c = caps(json!({"kind": "transform"}));
        assert!(c.is_empty());
        assert!(!c.declares_http());
        assert!(!c.kv);
    }

    #[test]
    fn a_declared_http_capability_defaults_to_get_only() {
        let c = caps(json!({"capabilities": {"http": {"hosts": ["API.example.com"]}}}));
        let http = c.http.expect("declared");
        assert_eq!(http.methods, vec!["GET"]);
        // Hosts are normalized so a manifest's casing cannot dodge the match.
        assert_eq!(http.hosts, vec!["api.example.com"]);
    }

    /// The refusals that keep a typo from becoming a mysterious link error, and
    /// the one that keeps a manifest from asking for the whole internet.
    #[test]
    fn a_wildcard_or_empty_host_list_is_refused_not_widened() {
        assert!(parse_capabilities(&json!({"capabilities": {"http": {"hosts": []}}})).is_err());
        assert!(parse_capabilities(&json!({"capabilities": {"http": {"hosts": ["*"]}}})).is_err());
        assert!(parse_capabilities(&json!({"capabilities": {"nework": true}})).is_err());
        assert!(parse_capabilities(&json!({"capabilities": {"kv": "yes"}})).is_err());
        assert!(parse_capabilities(&json!({"capabilities": []})).is_err());
    }

    #[test]
    fn an_undeclared_host_is_not_fetched() {
        let c = caps(json!({"capabilities": {"http": {"hosts": ["api.notion.com"]}}}));
        let allow = vec!["*".to_string()];
        assert_eq!(
            authorize_http(&c, &allow, "GET", "https://evil.example/steal"),
            Err(HttpDenial::HostNotDeclared("evil.example".into()))
        );
        assert!(authorize_http(&c, &allow, "GET", "https://api.notion.com/v1/pages").is_ok());
    }

    /// The half of the gate the plugin author does not control: declaring a host
    /// gets you nothing on a deployment that never allowed it.
    #[test]
    fn a_declared_host_is_not_reached_without_the_operator_allow_list() {
        let c = caps(json!({"capabilities": {"http": {"hosts": ["api.notion.com"]}}}));
        assert_eq!(
            authorize_http(&c, &[], "GET", "https://api.notion.com/v1/pages"),
            Err(HttpDenial::HostNotAllowed("api.notion.com".into())),
            "the default [plugins] allow_http_hosts is empty, and empty means none"
        );
        let allow = vec![".notion.com".to_string()];
        assert!(authorize_http(&c, &allow, "GET", "https://api.notion.com/v1/pages").is_ok());
    }

    #[test]
    fn an_undeclared_method_is_not_sent() {
        let c = caps(
            json!({"capabilities": {"http": {"hosts": ["api.example.com"], "methods": ["post"]}}}),
        );
        let allow = vec!["*".to_string()];
        assert!(authorize_http(&c, &allow, "POST", "https://api.example.com/x").is_ok());
        assert_eq!(
            authorize_http(&c, &allow, "DELETE", "https://api.example.com/x"),
            Err(HttpDenial::MethodNotDeclared("DELETE".into()))
        );
    }

    /// The sandbox's first outbound capability must not become a filesystem one
    /// through a scheme the HTTP client would happily accept.
    #[test]
    fn a_non_http_scheme_is_not_a_request() {
        let c = caps(json!({"capabilities": {"http": {"hosts": ["localhost"]}}}));
        let allow = vec!["*".to_string()];
        assert_eq!(
            authorize_http(&c, &allow, "GET", "file:///etc/passwd"),
            Err(HttpDenial::Scheme("file".into()))
        );
        assert!(matches!(
            authorize_http(&c, &allow, "GET", "/relative/path"),
            Err(HttpDenial::BadUrl(_))
        ));
    }

    /// No capability at all is refused BEFORE the URL is even parsed — the
    /// common case, and the one that must never depend on a parser's opinion.
    #[test]
    fn no_capability_means_no_request_at_all() {
        assert_eq!(
            authorize_http(
                &PluginCapabilities::default(),
                &["*".to_string()],
                "GET",
                "https://api.example.com/"
            ),
            Err(HttpDenial::NoCapability)
        );
    }

    #[test]
    fn a_domain_pattern_covers_its_subdomains_and_nothing_else() {
        let pats = vec![".example.com".to_string()];
        assert!(host_matches_any("example.com", &pats));
        assert!(host_matches_any("api.example.com", &pats));
        assert!(!host_matches_any("notexample.com", &pats));
        assert!(!host_matches_any("example.com.evil.net", &pats));
    }
}
