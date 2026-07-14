# Engine Capability Traits — refactor + bug-hunt findings

> Total: 5 findings (Critical: 0, High: 1, Medium: 2, Low: 2)
> Files scanned: `crates/core/src/engine.rs`, `crates/core/src/plugin.rs`, `crates/core/src/search.rs` (scoped); confirmed against `crates/core/src/cache.rs`, `crates/engine-http/src/lib.rs`, `crates/engine-claude/src/lib.rs`, `crates/engine-search/src/lib.rs`, `crates/server/src/routes.rs`

## 1. `HttpRequest` `headers`/`proxy` vary the response but are excluded from the cache identity
- **Severity**: High
- **Lens**: bug-hunter
- **Category**: state-corruption / wrong-result (cache contamination)
- **File**: `crates/core/src/engine.rs:88-89` (`headers`), `:128-129` (`proxy`); manifested in `crates/core/src/cache.rs:40-49` (`HttpCache::key`) + `crates/engine-http/src/lib.rs:365-374` (`cacheable`)
- **Scenario**: `HttpCache::key` hashes only `method + url + body`. `cacheable()` correctly excludes profiled requests (session isolation) but says nothing about `headers` or `proxy`, and both are applied to the real request (`engine-http/src/lib.rs:347-349` sends every `req.headers` entry; the client is built per `(proxy, profile)` pair). So two cacheable GETs to the same URL that differ only in headers or proxy collide on one cache key. Concrete repros: (a) content negotiation — fetch `https://api.example.com/data` with `Accept: application/json`, then later with `Accept: text/csv`; the second request gets a cache HIT and receives the JSON body. Same for `Accept-Language: de` vs `en`. (b) per-request `proxy` override to reach geo-variant content — fetch a price/availability page via a US proxy (cached), then via an EU proxy; the EU request is served the US-cached body. (c) an `Authorization` header set without a session profile caches an authenticated body that is then served to an anonymous same-URL GET.
- **Root cause**: The design comment at `engine-http/src/lib.rs:365-368` reasons only about *profile* isolation and assumes method+url+body uniquely identifies a response, but `HttpRequest` deliberately carries response-varying inputs (`headers`, `proxy`) with no reflection in the cache identity — the classic missing-`Vary` mistake.
- **Impact**: Wrong result served from cache; format/locale/geo mismatch; possible auth-body leak to an anonymous caller.
- **Fix sketch**: Either fold the response-varying inputs into `HttpCache::key` (hash a canonicalized `headers` map and the effective `proxy`), or extend `cacheable()` to refuse caching when non-default `headers`/`proxy` are present (mirroring the existing `profile.is_none()` guard).

## 2. Unknown `ResearchRequest.role` is silently ignored, dropping the intended preset
- **Severity**: Medium
- **Lens**: bug-hunter
- **Category**: silent-failure / contract-ambiguity
- **File**: `crates/core/src/engine.rs:246-249` (contract); confirmed at `crates/engine-claude/src/lib.rs:28-46` (`resolve`)
- **Scenario**: `role` is documented as "Named preset from `[claude.roles]`". The implementor resolves it with `req.role.as_deref().and_then(|r| self.cfg.roles.get(r))` (`engine-claude/src/lib.rs:29`). A typo'd or removed role name (`Some("reserch")`) makes `.get()` return `None`, and `.and_then` silently yields `None`, so `model`/`effort`/`max_budget_usd` all fall through to config defaults instead of the requested preset. The caller believes a preset is in force but it is not.
- **Root cause**: The trait leaves "unknown role" behavior unspecified, and the implementor treats "role not found" identically to "no role requested" — a lookup miss is swallowed rather than surfaced as an error.
- **Impact**: Wrong model/effort silently used; more importantly the role's `max_budget_usd` ceiling is skipped, so if `cfg.max_budget_usd` is `None` a run intended to be budget-capped runs uncapped (cost overrun).
- **Fix sketch**: Have `resolve` return a typed error when `req.role` is `Some(name)` and `self.cfg.roles` has no such key, or document on the trait that an unknown role is a hard error — do not conflate "absent" with "unknown".

## 3. `#[serde(default)]` + "stays deserializable" comments on structs that never derive `Deserialize`
- **Severity**: Medium
- **Lens**: code-refactor
- **Category**: dead-code / misleading-docs
- **File**: `crates/core/src/engine.rs:158` & `:166-169` (`HttpResponse::cache_hit`); `:216` & `:222-235` (`RenderedPage::nav_timed_out` / `selector_found` / `blocked_resources`)
- **Scenario**: `HttpResponse` derives `#[derive(Debug, Clone, Serialize)]` and `RenderedPage` derives `#[derive(Debug, Clone, Default, Serialize)]` — neither derives `Deserialize`. Yet `cache_hit` carries `#[serde(default)]` with the comment "keeps older serialized responses … deserializable", and all three `RenderedPage` fields carry `#[serde(default)]` "so older payloads deserialize". With no `Deserialize` impl these attributes are completely inert, and the described backward-compat mechanism does not exist. The actual cache path reconstructs `HttpResponse` field-by-field from DB columns and sets `cache_hit: true` by hand (`cache.rs:66-72`) — it never round-trips through serde deserialization.
- **Root cause**: Attributes and doc comments were copied from the `Deserialize`-deriving request structs (`HttpRequest`, `RenderRequest`) onto response structs that are serialize-only, asserting a compatibility contract the derives don't provide.
- **Impact**: Wasted maintenance; a maintainer relying on the stated "old payloads still deserialize" invariant would be misled (any future attempt to actually deserialize these types fails to compile until `Deserialize` is added).
- **Fix sketch**: Drop the four inert `#[serde(default)]` attributes and the compat sentences, or add `#[derive(Deserialize)]` to both structs if a genuine deserialize path is intended — but don't leave the claim without the derive.

## 4. `SearchRequest.limit` has no engine-side ceiling and `limit == 0` still samples the corpus
- **Severity**: Low
- **Lens**: bug-hunter
- **Category**: edge-case / contract-ambiguity
- **File**: `crates/core/src/search.rs:39-56` (contract); confirmed at `crates/engine-search/src/lib.rs:247`, `:256`, `:276`
- **Scenario**: `SearchRequest::new(q, limit)` accepts any `usize` with no documented bound, and the engine computes `sample_size = req.limit.max(FACET_SAMPLE)` then calls `TopDocs::with_limit(sample_size)` (`engine-search/src/lib.rs:247-249`), which pre-allocates a top-K heap sized to the limit. The HTTP surface clamps to 100 (`server/src/routes.rs:2001`), but any in-process caller (an app crate, `worker.rs`) constructing `SearchRequest::new(q, very_large)` drives an unbounded allocation. Separately, `limit == 0` returns zero hits (`if i < req.limit` is never true, `:276`) yet still executes a search over up to `FACET_SAMPLE` (1000) docs and computes facets — a silent surprise for a "give me 0" request.
- **Root cause**: The trait pushes the entire cap responsibility onto every caller; the engine has no internal ceiling and no floor semantics documented for `0`.
- **Impact**: Potential large allocation from a trusted-but-careless in-process caller; mild wasted work / surprising facet-only behavior at `limit == 0`.
- **Fix sketch**: Document (and/or enforce in the engine) a sane maximum for `limit`, and clarify the `limit == 0` contract — either treat it as "no hits, no work" or reject it.

## 5. `NoSearch` fallback silently succeeds while `NoPlugins` fallback errors — inconsistent disabled-capability contract
- **Severity**: Low
- **Lens**: code-refactor
- **Category**: contract-inconsistency
- **File**: `crates/core/src/search.rs:96-113` (`NoSearch`) vs `crates/core/src/plugin.rs:31-45` (`NoPlugins`)
- **Scenario**: When plugins are disabled, `NoPlugins::run` returns `Err(Error::App("plugins are disabled…"))` — a loud, discoverable failure. When search is disabled, `NoSearch::index` returns `Ok(())` (silently discarding the batch) and `NoSearch::query` returns an empty `SearchResponse`. A pipeline that indexes into a disabled search backend and later queries gets zero results with no signal that indexing never happened.
- **Root cause**: The two "capability disabled" fallbacks chose opposite philosophies (fail-closed vs fail-open) with nothing in the trait docs stating which is intended for `Search`. (Fail-open for a best-effort side effect may well be deliberate — this is flagged for a consistency decision, not asserted as a defect.)
- **Impact**: Wasted maintenance / debugging confusion; a "why is search empty?" investigation with no error to grep for.
- **Fix sketch**: Pick one convention and document it on the traits — if `Search` is intentionally fail-open, add a doc line on `Search`/`NoSearch` saying so; if not, make `NoSearch::index`/`query` return a typed "search disabled" error like `NoPlugins`.
