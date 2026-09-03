# microsoft/mcp @ `bc2a3b4` vs pumper's `/mcp` — a peer comparison

**Source:** `microsoft/mcp`, read-only at `C:/t/msmcp`. A vendor MCP monorepo: a
shared `core/Microsoft.Mcp.Core` framework, ~50 tool areas under `tools/`, three
servers (`Azure.Mcp.Server`, `Fabric.Mcp.Server`, `Template.Mcp.Server`), plus an
optional distributed-session package.

**Us:** `crates/server/src/mcp/` (1,957 + 338 + 216 lines), the tool-facing
config in `crates/core/src/config.rs`, the job-token mint in
`crates/core/src/agent_tools.rs`, the suites in `crates/server/src/e2e/mcp*.rs`.

Both publish a tool surface to LLM agents. That is the whole basis for the
comparison — the domains have nothing to do with each other, and where the two
designs diverge it is almost always because a *force* differs, not because one
side missed something. Where a force differs, the verdict is `different forces`
and there is nothing to do. Where we are simply behind, the verdict says so.
Where we are ahead, §"What pumper does better" says that, at length, because a
study that only lists things to copy is a wish list, not a study.

Every anchor below was opened. Counts were counted, not estimated.

---

## Catalog and projection

### The tool-count budget is a host budget, and it is shared
- **source:** VS Code Copilot caps a request at 128 tools **summed across every
  installed server** — `servers/Azure.Mcp.Server/TROUBLESHOOTING.md:121-134`
  names the failure ("You may not include more than 128 tools in your request")
  and its cause ("Combining multiple comprehensive toolsets (like GitHub MCP
  'all' + Azure MCP 'all') exceeds this limit"). So the source's tool count is
  not its own business: it is spending a budget its neighbours also spend.
- **pumper:** `tools/list` publishes **8 tools** in the shipped default and **19
  at maximum** — pinned exactly, in order, by
  `crates/server/src/e2e/mcp.rs:177-192` (`list_apps`, `query_dataset`,
  `search`, `wait_job`, `wait_workflow`, `list_pending_transactions`,
  `market_profile`, `fetch`), plus 10 more behind `allow_enqueue`
  (`crates/server/src/mcp/mod.rs:292-618`) and `approve_transaction` behind
  `allow_approve` + `[transact] allow_live`.
- **verdict:** keep ours
- **why:** 8 of a shared 128 is not a budget problem, it is a rounding error. The
  source needed a projection mechanism because ~50 areas × N commands overflowed
  the host; we do not have that population and inventing the mechanism now would
  be a knob with nothing behind it. The number to watch is the *pinned list* in
  the e2e test, and it is already the instrument.

### Four projections of one command tree, chosen by a startup flag
- **source:** `core/Microsoft.Mcp.Core/src/Areas/Server/Options/ModeTypes.cs:9-36`
  — `single` (one `azure` tool routing everything), `namespace` (one tool per
  service, the default), `all` (one tool per command), `consolidated` (curated
  groupings). Selected by `--mode`
  (`.../Options/ServerStartOptions.cs:32-33`), with `--namespace` and `--tool`
  as orthogonal filters, and `--namespace`/`--tool` refused together
  (`.../Commands/ServerStartCommand.cs:254-263`).
- **pumper:** no projection knob. The listing varies on exactly three booleans —
  `mcp.enabled`, `mcp.allow_enqueue`, `mcp.allow_approve`
  (`crates/core/src/config.rs:244-267`) — and those are *authority* switches, not
  presentation modes.
- **verdict:** keep ours
- **why:** a projection is a second name for every operation, and a second name
  is a second contract to keep stable and a second thing an agent can be
  confused by. The source pays that because it must. Our switches change *what
  an agent may do*, which is a different axis and the one that matters here; a
  read-only pumper offering 8 tools needs no compression.

### The default projection was flipped as a breaking change
- **source:** `servers/Azure.Mcp.Server/CHANGELOG.md:2684-2686` — "Changed the
  default startup mode to list tools at the namespace level instead of at an
  individual level, reducing total tool count from around 128 tools to 25. Use
  `--mode all` to restore the previous behavior." Filed under **Breaking
  Changes**, with an escape hatch named in the same sentence.
- **pumper:** no analogous event. Tool surface has only ever grown, always by
  appending — the e2e comments read "appended last, per the wave-2 shared-surface
  rule" (`crates/server/src/mcp/mod.rs:648-650`) and the test pins `fetch` as
  last (`crates/server/src/e2e/mcp.rs:258-262`).
- **verdict:** adopt (the discipline, not the change)
- **why:** the transferable part is that *a default change to the listing is a
  breaking change and is written down as one, with the restoring flag in the same
  line*. We have no changelog discipline for the MCP surface at all — see
  "Contract evolution" below. This is the cheapest thing on the list.

### Listing results are cached, and the cache is a claim about callers
- **source:** `.../Runtime/McpRuntime.cs:193-194` marks every `ListTools` result
  `CacheScope.Public` with a 1h TTL, justified inline as "safe to cache
  publicly, as they contain no sensitive information". The namespace loader
  additionally memoizes its own result in-process
  (`.../ToolLoading/NamespaceToolLoader.cs:55,106-108,159`).
- **pumper:** `server_tools(state)` is rebuilt on every `tools/list`
  (`crates/server/src/mcp/mod.rs:121,167`), and reads `state.config` while doing
  it — `wait_job`'s description interpolates the live
  `mcp.wait_job_max_secs` (`:270-278`).
- **verdict:** keep ours
- **why:** the source's justification holds only while the listing never varies
  by caller entitlement — and its own listing *does* vary by `ReadOnly` and
  `IsHttpMode` (`NamespaceToolLoader.cs:122,128`), which are process-wide
  configuration rather than per-caller, so it survives. Ours varies on
  process-wide config too, so we could cache. We should not: rebuilding a
  20-element `Vec<Value>` costs nothing on a local-first single-binary service,
  and a cache is a place for the listing and the dispatch to drift apart.

### Listing withholds; it does not stub
- **source:** in read-only mode a namespace whose commands are all writes is
  skipped entirely from the listing (`NamespaceToolLoader.cs:122-127`), not
  listed-and-refused.
- **pumper:** same shape — the gated tools are never pushed onto the vector
  (`crates/server/src/mcp/mod.rs:291,375,478,606`), and the doc says why
  explicitly for the hardest case: `approve_transaction` "is not even listed
  until **both** are on, because a tool that is offered and then refuses every
  call reads to an agent as a broken server rather than as a policy"
  (`docs/features/mcp.md:40-45`).
- **verdict:** keep ours
- **why:** independently reached, and we wrote down the reason, which the source
  did not. Convergence here is corroboration, not a gap.

---

## Tool identity and naming

### A rename-stable identity, separate from the name
- **source:** every command carries an `Id` — a GUID — emitted in `_meta` under
  `MicrosoftMcpToolId` on results (`core/.../Helpers/McpHelper.cs:20,46-54`) and
  readable back off `_meta` (`:61-71`). Uniqueness is enforced by a build script
  that scans `tools/**/*Command.cs` for `public override string Id => "..."` and
  fails on any duplicate (`eng/scripts/Test-ToolId.ps1:19,26-38,42-47`), wired
  into `eng/scripts/Analyze-Code.ps1:77` and `eng/scripts/Preflight.ps1:148`.
  The rename checklist explicitly says the GUID changes only when the *meaning*
  changes, not when the name does (`docs/tool-rename-checklist.md:23`).
- **pumper:** no counterpart — verified: `grep -n '_meta'` over
  `crates/server/src/mcp/*.rs` returns nothing, and every result is
  `{content, structuredContent, isError}` with no metadata envelope
  (`crates/server/src/mcp/mod.rs:770-782`). The tool's name *is* its identity.
- **verdict:** keep ours
- **why:** the identity exists to survive renames across a fleet of downstream
  consumers who pinned names. We have one consumer of the MCP surface we do not
  control (whatever agent an operator points at it) and one we do (our own
  self-hosted Claude subprocess, which names tools as
  `mcp__pumper__fetch` in `crates/core/src/config.rs:1747`). A stable GUID buys
  nothing until something downstream keys off it, and it costs a second name for
  every tool forever. Revisit if a published SDK ever pins tool names.

### Downstream name pins are executable, not documentary
- **source:** `servers/Azure.Mcp.Server/tests/.../VisualStudioToolNameTests.cs:21-29`
  pins two exact strings (`get_azure_bestpractices_get`,
  `extension_cli_generate`) because "Visual Studio has hard-coded dependencies on
  these tool names in FirstPartyToolsProvider.cs", with the PR link in the
  doc comment. Renaming either fails a build, not a review.
- **pumper:** the closest analogue is the e2e list assertion
  (`crates/server/src/e2e/mcp.rs:177-192`) and `EXPECTED_SEARCH_PARAMS`
  (`:479-492`), which pin the whole surface rather than a named consumer's slice.
- **verdict:** keep ours
- **why:** ours is strictly stronger for our situation — pinning *everything* is
  affordable at 19 tools and 12 params, and does not require knowing who
  downstream is. The source pins two names because pinning 128 would freeze the
  repo. Our test also carries the *reason* in a comment ("The anti-pattern this
  pins: the tool advertising a strict subset of `GET /search`",
  `crates/server/src/e2e/mcp.rs:475-478`), which the VS test does too. Same
  idiom, different scale, both correct.

### Naming grammar
- **source:** underscore-joined command hierarchy with a server prefix —
  `azmcp_storage_account_list`, derived mechanically from group + leaf `Name`
  properties (`docs/tool-rename-checklist.md:9`). Group segments are *not*
  derived from the command's `Name` and must be changed by hand in `*Setup.cs`
  (`:22`) — a documented foot-gun.
- **pumper:** flat verb-object names typed as literals in one `Vec`
  (`crates/server/src/mcp/mod.rs:170-668`): `list_apps`, `query_dataset`,
  `search`, `enqueue_job`, `create_trigger`, `market_profile`, `fetch`.
- **verdict:** keep ours
- **why:** a derived hierarchy is what you build when tool names must be
  generated from ~50 areas without collisions. Nineteen hand-typed names have no
  collision problem and no hidden derivation step to get wrong. The source's own
  checklist documents that its derivation leaks — that is the cost we are not
  paying.

---

## Argument validation at the door

### Unknown arguments: rejected there, silently ignored here
- **source:** `core/.../Commands/CommandExtensions.cs:58-62` collects every key
  that matched no option into `invalidKeys` and continues; `:97-102` then fails
  the whole call with `"Request rejected due to unknown arguments: {keys}"`,
  naming them. Nothing is executed on a partial match.
- **pumper:** every tool advertises `"additionalProperties": false` in its
  `inputSchema` (`crates/server/src/mcp/mod.rs:175,196,262,...`) — and **nothing
  enforces it**. Argument reads are `args.get(key).and_then(...)`
  (`tool_search`, `:900-923`; `require_str`, `:1136-1141`), so `{"q": "x",
  "limitt": 5}` runs a default-limit search and reports success. The
  `tool_body` path (`:1409-1411`) deserializes into the HTTP request bodies,
  and serde's default is also to ignore unknown fields —
  `deny_unknown_fields` appears exactly once in the workspace and is a
  deliberate *refusal* to use it elsewhere (`crates/core/src/engine.rs:654`).
- **verdict:** adopt
- **why:** this is the seeded point that survived contact, and it is the sharpest
  finding in the study. We *publish a strict contract and do not keep it*. A
  typo'd argument is the single most common agent error, and today it produces a
  plausible wrong answer instead of a correctable refusal — the exact failure
  mode `search`'s own description works so hard to prevent for empty results
  ("an empty `hits` list is NOT evidence the records do not exist",
  `:206-211`). The fix is small and local: one `reject_unknown_args(args,
  &[...])` helper called at the top of `tools_call`, driven off the schema we
  already emit.

### Spelling forgiven, structure not
- **source:** accepted keys match case-insensitively against option names *and*
  aliases, with a hyphen-stripping fallback so `resourceGroup` reaches
  `--resource-group` (`CommandExtensions.cs:44-56`). Strict about membership,
  generous about shape.
- **pumper:** exact-match only — `args.get("amount_gte")` finds nothing under
  `amountGte`.
- **verdict:** adapt
- **why:** the pairing is the insight: you can only afford to be strict about
  unknown keys if you are generous about the forms of the known ones, otherwise
  strictness becomes a camelCase-vs-snake_case tax on every agent. If we adopt
  the rejection above, we should land the case/separator fallback in the same
  change, not as a follow-up — shipping the strictness alone would make the
  surface worse for a week.

### Nested payloads are schema-validated, with pointer paths
- **source:** option values are converted to CLI tokens and re-parsed by
  System.CommandLine (`CommandExtensions.cs:104`); type errors surface as parse
  errors. There is no JSON Schema validation of a nested payload because there
  are no nested payloads — arguments are flat.
- **pumper:** `enqueue_job`'s `params` is validated against the app's own
  declared `params_schema` by `jsonschema`, and every violation is rendered as
  `params<json-pointer>: <detail>`
  (`crates/server/src/mcp/mod.rs:1223-1247`), asserted end to end at
  `crates/server/src/e2e/mcp.rs:385-402` (the refusal names `params/rows` and
  `query`, and *nothing is enqueued*).
- **verdict:** keep ours
- **why:** this is the harder half of the problem and we do it and they do not
  have to. It also makes the gap above stranger, not better: the nested payload
  an agent supplies is checked to the pointer, while the envelope carrying it is
  not checked at all.

### One door, one validation
- **source:** the MCP tool and the CLI are the *same* `Command` object, parsed
  through the same options (`CommandExtensions.cs:33-36,104`). Divergence is
  structurally impossible.
- **pumper:** the same idea, reached differently — MCP tools call the HTTP
  route's own handlers: `tool_search` builds through
  `routes::build_search_request` and renders through `routes::run_search`
  (`:903,929`), `market_profile` goes through
  `routes::query::find_market_profile` with the comment "so the two surfaces
  cannot disagree about which row exists or what a miss means" (`:788-791`), and
  `tool_body`'s doc says "The bodies ARE the schema: reusing them is what makes
  'same door, same validation' true rather than aspirational"
  (`:1404-1407`).
- **verdict:** keep ours
- **why:** independent convergence on the strongest available form. Ours is
  slightly weaker (the reuse is by convention at each call site, not by
  construction) but the compensating instrument exists: `EXPECTED_SEARCH_PARAMS`
  fails when the tool's advertised surface drifts from the route's.

---

## Annotations, consent and blast radius

### Six annotation axes, two of them non-standard
- **source:** `core/.../Commands/ToolMetadata.cs` carries `Destructive`
  (default **true**, `:30`), `Idempotent` (`:55`), `OpenWorld` (default **true**,
  `:79`), `ReadOnly` (`:107`) — the spec's four — plus `Secret` (`:135`) and
  `LocalRequired` (`:163`). Each serializes not as a bare boolean but as a
  `{value, description}` pair with prose written for both polarities
  (`:33-40,109-116,137-144,168-175`), so a client rendering a consent dialog has
  a sentence to show. Dangerous defaults are the *safe* reading: an unannotated
  tool is assumed destructive and open-world.
- **pumper:** no counterpart — verified: no `annotations`, `readOnlyHint`,
  `destructiveHint`, `idempotentHint`, `openWorldHint` or `_meta` key appears
  anywhere in `crates/server/src/mcp/`. Every tool definition is
  `{name, description, inputSchema}` and nothing else.
- **verdict:** adapt
- **why:** we would not want six axes and we would not want them per-tool-object;
  what we want is the *four spec hints*, because a client that honours them can
  render the difference between `search` and `approve_transaction` without
  reading our prose. We already know each tool's answer with certainty — the
  dispatch arms in `tools_call` (`:723-767`) are literally a partition into
  read-only / spending / irreversible. The two non-standard axes map to facts we
  also already have (`Secret` → nothing we return is a credential;
  `LocalRequired` → *everything* here is local-required, which is the whole
  product). Small, mechanical, high-value: it turns policy we enforce
  server-side into policy the client can also see.

### Annotation equality as a merge precondition — enforced only in DEBUG
- **source:** when commands are merged into one consolidated tool, every member's
  metadata must match the consolidated tool's, on all six axes
  (`core/.../Discovery/ConsolidatedToolDiscoveryStrategy.cs:107`). The error
  message prints both metadata sets in full (`:109-116`). But the enforcement is
  `#if DEBUG` → `throw`, `#else` → `LogWarning` (`:117-122`): in a release build
  a mismatch is a log line, and a read-only-annotated tool can quietly contain a
  destructive command.
- **pumper:** no consolidation, so no counterpart — the closest analogue is that
  every gated tool's *dispatch arm* carries the same guard as its listing
  condition, and the e2e proves both directions
  (`crates/server/src/e2e/mcp.rs:194-217,335-366`).
- **verdict:** different forces
- **why:** worth recording as a lesson rather than a gap. The pattern —
  a safety invariant checked hard in the build the authors run and softly in the
  build users get — is exactly the shape our own `flake-check` three-state
  contract exists to prevent (`CLAUDE.md`, `just flake-check`: "0 clean / 2
  findings / **3 cannot check**"). We do not have their problem, but if we ever
  gate an invariant behind `#[cfg(debug_assertions)]` this is the anti-pattern to
  name.

### Consent read from the payload, not the envelope
- **source:** `core/.../ToolLoading/BaseToolLoader.cs:302-306` — the elicitation
  response's transport `Action` is not trusted alone, because "a client that
  submits the form returns Action == 'accept' even when the user picked 'Reject'
  (their selection lives in Content['decision'])" (`:295-301`). Approval requires
  `IsAccepted && decisionProvided && decision == "accept"`. Any failure path is a
  refusal with `IsError = true` (`:307-317`), and so is a thrown exception during
  elicitation (`:322-330`).
- **pumper:** no elicitation at all — `initialize_result` advertises
  `{"tools": {}, "resources": {}}` and nothing else
  (`crates/server/src/mcp/mod.rs:147`), and the client's capabilities are never
  read.
- **verdict:** keep ours
- **why:** this is a genuinely excellent bug fix and it is not our bug, because
  we made a different and — for a local-first service — better choice: consent
  in pumper is **operator consent at configuration time**, not user consent at
  call time. `approve_transaction` cannot be called into existence by a
  cooperative dialog; it requires two config keys set before the process started
  (`crates/core/src/config.rs:244-267`, `crates/server/src/mcp/mod.rs:744-748`).
  A round trip that a misbehaving client can spoof the answer to is strictly
  weaker than a switch a client cannot reach. Keep.

### Absent capability = reject
- **source:** `BaseToolLoader.cs:239-247` — if the client does not support
  elicitation, a secret/destructive tool call is refused outright, with the
  reason in the error text. Not "proceed and log".
- **pumper:** structurally the same conclusion by a different route: no capability
  negotiation exists, so an actuating tool that is not enabled is simply absent
  and, if called by name, returns a refusal that *names the config key*
  (`crates/server/src/mcp/mod.rs:733-737,749-754,763-767`; asserted at
  `crates/server/src/e2e/mcp.rs:194-217`).
- **verdict:** keep ours
- **why:** same principle — the safe default when you cannot establish consent is
  refusal — reached without needing the capability check. Our refusals arguably
  do better on one axis: they tell the caller *which switch* to ask the operator
  for, and the approve refusal ends with "Nothing was submitted", which the e2e
  pins (`crates/server/src/e2e/mcp.rs:216`). An agent that cannot tell a refusal
  from a failed action will retry.

### Blast radius is graded, and the grades are not nested
- **source:** the escape hatch is `--dangerously-disable-elicitation`, which
  downgrades every consent gate to a `LogWarning` and proceeds
  (`BaseToolLoader.cs:233-238`). One flag, all tools.
- **pumper:** three independent switches, and the doc argues the
  non-nesting explicitly: `allow_approve` is "a **third** switch rather than a
  reuse of `allow_enqueue`, because the two authorities are not the same
  size" (`docs/features/mcp.md:40-45`; the same argument in code at
  `crates/core/src/config.rs:257-263`). The e2e enforces it: opting into
  enqueue must *not* surface `approve_transaction`
  (`crates/server/src/e2e/mcp.rs:236-240`, "allow_enqueue must not imply the
  authority to release an irreversible action").
- **verdict:** keep ours
- **why:** a single dangerous flag that disables consent for every tool is the
  design our three switches exist to avoid, and we have a test asserting the
  non-implication rather than a comment hoping for it. This is one of the places
  we are ahead.

---

## Error channels

### Two channels, and the split is the same on both sides
- **source:** a *tool* failure is a `CallToolResult` with `IsError = true` and
  human-readable content (`NamespaceToolLoader.cs:383-397,404-414`;
  `BaseToolLoader.cs:242-247`), so the model can read and react. Framework-level
  failures throw and surface as protocol errors
  (`McpRuntime.cs:115-120`).
- **pumper:** identical split, and documented at the function:
  "`tools/call`: runs a tool and wraps the outcome per MCP — a *tool* failure is
  a `result` with `isError: true` (the agent can read and react), while an
  unknown tool or unusable arguments are protocol errors"
  (`crates/server/src/mcp/mod.rs:695-698`). Implemented at `:770-782` and pinned
  both ways at `crates/server/src/e2e/mcp.rs:465-475` (`-32601` for an unknown
  method, `-32602` for an unknown tool).
- **verdict:** keep ours
- **why:** convergent and equally correct. Ours is additionally *tested at the
  boundary*, which is the part that rots first.

### An unknown tool is a protocol error, not a tool error
- **source:** an unknown *sub-command* inside a namespace tool is not an error at
  all — it re-routes to `InvokeToolLearn` so the model can discover the right one
  (`NamespaceToolLoader.cs:374-378`). Unknown top-level tools never reach the
  loader.
- **pumper:** `other => return rpc_error(id, -32602, &format!("unknown tool
  '{other}'"))` (`crates/server/src/mcp/mod.rs:768`).
- **verdict:** keep ours
- **why:** their `learn` redirect is a consequence of routing: a namespace tool
  hides N commands behind one name, so a wrong sub-command is a *discovery*
  failure and deserves a discovery answer. Our names are the surface, so a wrong
  name is a wrong call. But there is a transferable half here we already have
  under a different name — `list_apps` is exactly a `learn` tool for the app
  layer (`:170-176`).

### Refusals carry the status the other surface would have used
- **source:** no counterpart — a tool error is prose; the HTTP status of the
  underlying Azure call is not systematically re-surfaced in the tool error text.
- **pumper:** `door_error` renders a route's `ApiError` as `[{status}] {message}`
  (`crates/server/src/mcp/mod.rs:1399-1402`), and the job-token refusals do the
  same through `refuse(...)`, "tagged with the HTTP code it would have carried on
  the REST surface so the two surfaces agree about what happened"
  (`crates/server/src/mcp/jobtoken.rs:78-87`).
- **verdict:** keep ours
- **why:** ours, and a small one worth naming: an agent that gets `[unauthorized]`
  knows to stop; one that gets `[503]` knows to retry. Prose does not carry that.

---

## Auth and token scoping

### Inbound × outbound is a matrix with a refused cell
- **source:** `docs/Authentication.md:119-124` — a four-row table of inbound
  (Delegated / Application) × outbound (On-Behalf-Of / Hosting Environment
  Identity). Three combinations supported; **Application × On-Behalf-Of** is
  refused with the reason inline: "Application bearer token carries no user
  identity, so there is nothing to exchange in an On-Behalf-Of flow". The
  choosing guidance below it (`:130-137`) grades the survivors on per-user RBAC
  and audit-trail granularity.
- **pumper:** two independent axes with no matrix — inbound is
  `[auth] mode = "open" | "keys"` (`docs/features/auth.md:17-31`), and outbound
  attribution is the job token (below). They do not interact except that the job
  token is deliberately *not* a substitute for the API key
  (`crates/core/src/agent_tools.rs:19-26`).
- **verdict:** adapt
- **why:** we do not need the matrix; we need the *artifact*. A short table in
  `docs/features/mcp.md` saying which `[auth] mode` × `[mcp] allow_*` × job-token
  combinations are meaningful, with the refused cells and their reasons, would
  document a thing an operator currently has to derive from three files. Cheap,
  and the reasoning already exists in scattered doc comments.

### Job-scoped attribution tokens
- **source:** no counterpart. The source's credentials are Entra tokens with
  request-lifetime scope; there is no notion of a token bound to a unit of work
  the server itself created, and the acknowledged consequence is that outbound
  tokens are obtained **without** the RFC 8707 `resource` parameter because Entra
  does not support it — "This may result in overly-broad token scope"
  (`docs/Authentication.md:172-184`), mitigated only by advice ("Use narrow OAuth
  scopes", "Monitor token usage patterns").
- **pumper:** `crates/core/src/agent_tools.rs` mints a 256-bit token bound to one
  `job_id` with a TTL (`:156-164`), revoked by `Drop` rather than by a sweeper
  (`:143-149`, "'expires with the job' is enforced by ownership rather than by a
  sweeper that may not run"), carried in `x-pumper-job-token` — "Deliberately NOT
  `Authorization`: that header carries the API key in `keys` mode, and one header
  cannot mean two credentials without the loop breaking in exactly one of the two
  auth modes" (`:35-38`). Resolution is a four-arm `TokenVerdict`
  (`Missing | Unknown | Expired | Valid`, `:66-75`) where every non-`Valid` arm
  has its own prose refusal (`:79-97`), and the server additionally refuses a
  token whose job is no longer `Running` (`crates/server/src/mcp/jobtoken.rs:74-76,119`).
- **verdict:** keep ours
- **why:** **we are ahead, and by more than the seed suggested.** The source has
  an acknowledged over-broad-token problem it cannot fix at its layer; we solved
  the same class of problem — a credential that outlives the work it was for — by
  scoping the credential to the work instead of to the caller. Three details are
  better than merely correct: (1) `Expired` and `Unknown` are kept distinct
  "because the two mean different things to whoever is reading the logs after a
  leak" (`agent_tools.rs:99-105`); (2) a zero TTL mints a born-expired token, an
  honest off-switch that also proves the deadline is checked
  (`:154-155`, test at `:237-241`); (3) the *job status* is re-checked at spend
  time, closing `a_finished_job_not_billed_for_a_late_fetch`
  (`jobtoken.rs:62-73`). This is the strongest single design in our MCP layer.

### The token authorizes attribution, not access
- **source:** no counterpart — every credential in the source authorizes access.
- **pumper:** stated as the module's thesis: "It authorizes *attribution*, not
  access: it says 'the fetch this MCP call is making belongs to job X'. It is not
  an API key and does not satisfy `[auth] mode = 'keys'`"
  (`crates/core/src/agent_tools.rs:19-23`). So in `keys` mode a subprocess needs
  *both* the operator key and the job token.
- **verdict:** keep ours
- **why:** the separation is what makes the token safe to write into a scratch
  config file the subprocess can read: leaking it buys an attacker the right to
  spend a specific already-running job's remaining budget through our own
  governed fetcher, and nothing else. Collapsing the two would have made it a
  key.

---

## Transport and statelessness

### Stateless POST as the default posture
- **source:** stdio is the default transport (`ServerStartOptions.cs:18-19`);
  HTTP is compile-gated to the Docker distribution
  (`ServerStartCommand.cs:236-244`). Session affinity across replicas is a
  *separate optional package* whose README opens by telling you not to use it:
  "not required for MCP 2026-07-28 stateless protocol compliance… prefer
  header-based stateless routing"
  (`core/Microsoft.ModelContextProtocol.HttpServer.Distributed/README.md:6-17`).
- **pumper:** streamable-HTTP only, hand-rolled, and stateless by construction —
  "the MCP spec explicitly permits a stateless server that answers each
  `POST /mcp` with a single `application/json` response (no SSE required)"
  (`crates/server/src/mcp/mod.rs:6-8`), with "POST stays stateless — the stream
  is a one-way event feed, not a session" (`:29-30`).
- **verdict:** keep ours
- **why:** same destination, and we arrived without needing a package to opt out
  of. One binary on one box has no affinity problem to have.

### Hand-rolled protocol vs. an SDK
- **source:** built on `ModelContextProtocol` (the C# SDK) — `Tool`,
  `CallToolResult`, `ElicitRequestParams`, `RequestContext<T>` are all SDK types
  (`BaseToolLoader.cs`, `McpHelper.cs:8`). Protocol churn is absorbed upstream,
  at the price of adapting the SDK's transport to their host
  (and of inheriting its bugs — the elicitation `Action` bug above is an SDK-shaped
  hazard).
- **pumper:** five methods implemented by hand with the reason recorded:
  "the surface Pumper needs is a small, stable JSON-RPC vocabulary… hand-rolling
  those five methods over the existing `AppState` is less code — and far less
  version churn — than adapting rmcp's transport layer to this router. The whole
  protocol lives in this module; swapping in a crate later is a local change"
  (`crates/server/src/mcp/mod.rs:4-11`).
- **verdict:** keep ours
- **why:** the decision is written down with its exit condition, which is the part
  that makes it a decision rather than an accident. The cost is real and we
  should name it: we will not get new spec features for free, and
  `SUPPORTED_PROTOCOL_VERSIONS` (`:50`) is a list we must maintain by hand.

### Protocol version negotiation
- **source:** `McpHelper.cs:22-27` notes that in the 2026-07-28 stateless
  protocol "there is no initialize handshake, so `request.Server.ClientInfo` is
  null for every request", and clients instead embed identity per-request under
  `io.modelcontextprotocol/clientInfo` in `_meta`. The source reads it from
  there.
- **pumper:** `initialize_result` echoes the client's requested version when it
  is one of ours and otherwise offers the newest
  (`crates/server/src/mcp/mod.rs:141-148`), asserted both ways at
  `crates/server/src/e2e/mcp.rs:121-148`. We do not read client identity at all.
- **verdict:** keep ours
- **why:** the negotiation is correct and tested. Not reading client identity is
  the right call for a local-first service — there is one operator and the
  process already knows who is running it — and reading it would be the first
  caller-varying input into a listing we would then have to stop caching. (See
  also "Trace metadata" below: identity we do not read is identity we cannot be
  spoofed about.)

---

## Observability and trace metadata

### Caller-supplied propagation metadata is validated or refused
- **source:** `McpRuntime.cs:138-177` records a caller's `traceparent` **only** if
  it matches the W3C regex `^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`
  (`:123-125`), records `tracestate` only under 512 chars (`:127-128,153-157`),
  and refuses `baggage` outright with the reason inline: "it is an unbounded
  cross-service propagation bag and recording it verbatim would allow callers to
  write arbitrary data into telemetry" (`:162-163`). Two VS Code-specific
  `_meta` fields are recorded unvalidated (`:165-175`).
- **pumper:** we record **nothing** from caller-supplied headers or `_meta` —
  verified: `traceparent`, `tracestate` and `baggage` do not appear anywhere in
  `crates/`. The only header read on the MCP path is `x-pumper-job-token`
  (`crates/server/src/mcp/mod.rs:75`; `jobtoken.rs:43-50`), and the only tracing
  in the module is two `tracing::warn!` calls (`:1234`, `:1648`). HTTP-level
  tracing is `tower_http::TraceLayer` on the router
  (`docs/features/observability.md:19`).
- **verdict:** different forces
- **why:** the seed framed this as something to check, and the check says we have
  no gap to close — we have no distributed trace to join. A single binary with a
  Sentry sink and a Prometheus endpoint (`docs/features/observability.md:5-10`)
  is not a span in someone else's trace. **But the technique is worth banking**:
  if we ever accept a correlation id from an agent, the rule is *validate the
  shape, cap the length, and refuse the unbounded bag* — a caller-supplied string
  written into telemetry unvalidated is a log-injection surface, and that hazard
  is not distributed-tracing-specific.

### Per-call telemetry naming the tool's annotations
- **source:** `NamespaceToolLoader.cs:379` stamps
  `Activity.Current?.SetTag(TagName.ToolAnnotations,
  McpHelper.CreateToolAnnotationTelemetry(cmd))` before dispatch — so a trace can
  answer "how many destructive calls did this client make" without joining to a
  catalog.
- **pumper:** no per-tool-call span at all. `tools_call` emits no structured
  event; the only record that an MCP call happened is whatever `TraceLayer`
  logged for `POST /mcp`, which cannot distinguish `search` from
  `approve_transaction`.
- **verdict:** adopt
- **why:** we can currently not answer "which MCP tools has this operator's agent
  actually called, and how often" from anything but a packet capture — every call
  is one indistinguishable `POST /mcp`. A single `tracing::info!` span per
  dispatch carrying `tool`, outcome and duration would be a few lines and would
  make `/metrics` able to carry a per-tool counter. It also pairs with the
  annotations point: once tools are annotated, the annotation is the natural tag.

### Errors are attributed on the span, then rethrown
- **source:** `McpRuntime.cs:114-119` sets `ActivityStatusCode.Error` plus
  exception type and stack trace as span tags before rethrowing, and does the
  same for the listing path (`:200-205`).
- **pumper:** tool errors become `isError: true` strings with no telemetry
  side-effect; an operator sees the refusal only if the agent surfaces it.
- **verdict:** adapt
- **why:** the transferable half is that a refusal is an *event*, not just a
  return value. Our most interesting refusals — every arm of `TokenVerdict`, the
  three `allow_*` refusals — are exactly the ones an operator would want counted,
  and today none of them increments anything. Fold this into the span above
  rather than as its own change.

---

## Testing strategy

### Recording through the production transport, compiled out of release
- **source:** the recording proxy is installed by the *production*
  `IHttpClientFactory` configurator, inside `#if DEBUG`
  (`core/.../Services/Http/HttpClientFactoryConfigurator.cs:67-79`) — so tests
  exercise the real handler chain, and the recording seam cannot ship. Tests
  must obtain clients from the factory to benefit
  (`docs/recorded-tests.md:46`).
- **pumper:** the VCR seam is `AppContext::fetch` / `AppContext::research` and it
  is *the only seam* — a fact that is itself pinned by a test
  (`crates/core/src/vcr.rs:56-62`, "Every one of those call sites is pinned in
  `crates/core/tests/fetch_chokepoint.rs`"). It is not compiled out; it is a
  runtime mode on the job (`record: true` / `replay_of: <job_id>`, `:3,12`).
- **verdict:** keep ours
- **why:** a pinned chokepoint is stronger than a debug-only injection, because
  it fails at build time when someone adds a bypass rather than silently
  recording less. And because our replay is an operator-facing feature (replay a
  job, not just a test), compiling it out would remove a product capability.

### Fidelity is declared per app, not assumed
- **source:** fidelity is assumed uniform — every recorded test goes through the
  proxy, and a tool that bypasses `IHttpClientFactory` simply fails in playback,
  discovered by running it (`docs/recorded-tests.md:46`).
- **pumper:** `ReplayFidelity` is a three-valued declaration
  (`Full | Partial | Unreplayable`, `crates/core/src/vcr.rs:171-194`) with a
  per-app table of 20+ entries and a *reason string* on each
  (`REPLAY_BYPASS_APPS`, `:216-320`); an `Unreplayable` app's `replay_of` job is
  refused before anything runs (`:69-71`), rather than replaying half a run and
  calling it deterministic.
- **verdict:** keep ours
- **why:** this is the honesty control the source does not have, and it exists
  because we admitted the seam is not universal ("Such traffic is invisible to
  the cassette **in both directions**", `:64-65`). A recorded-test system that
  cannot say which tests are actually recorded is one where a live call hides
  inside a green playback run.

### Sanitizers, matchers, and where recordings live
- **source:** sanitizers are declarative and overridable per test class —
  general/header/URI regex sets (`RecordedCommandTestsBase.cs:32,37,42-54,58`),
  with one worked example (the `WWW-Authenticate` tenant GUID) carrying three
  lines of comment about why a naive replacement breaks the tool. Recordings are
  **externalized** to `Azure/azure-sdk-assets`, referenced by an `assets.json`
  tag and sparse-cloned into `.assets/<hash>/` on demand
  (`docs/recorded-tests.md:28,48-56`). Variables that differ between record and
  playback go through `RegisterOrRetrieveVariable` (`:135-145`).
- **pumper:** cassettes are per-job NDJSON under `data/artifacts/`
  (`crates/core/src/vcr.rs:5,21-22`), never committed, and *retention-exempt* so
  they outlive releases (`crates/core/src/retention.rs:19,104,126-128`). There
  is **no sanitizer layer** — a cassette records `{url, method, req_hash, status,
  headers, engine, body}` (`vcr.rs:4-6`) verbatim, headers included. There is no
  freshness or staleness story either: a cassette is valid until a version bump
  refuses it (`:26-27`), and nothing ages one out or re-records it.
- **verdict:** adapt
- **why:** two different findings under one heading. (a) The **externalization**
  is a solution to a problem we do not have — our cassettes never enter git, so
  repo weight is not a force; keep ours. (b) The **sanitizer gap is real**: our
  cassettes are "designed to outlive releases and to be readable (and editable)
  by anything on the box" (`vcr.rs:21-23`) and they store response headers from
  authenticated fetches under session profiles. That is a credential-shaped
  artifact with a deliberately long life and no redaction pass. This is the
  second-sharpest finding in the study after unknown-argument rejection.

### The cassette is verified, not trusted
- **source:** no counterpart — recordings are trusted; integrity comes from the
  assets repo being version-controlled.
- **pumper:** the loader re-derives what it can rather than believing the file: a
  `GET` entry's `req_hash` is **recomputed** from its own method+url and a
  mismatch fails the whole load, because "an entry filed under a hash it does not
  hash to would be served for a request it is not a recording of"
  (`crates/core/src/vcr.rs:28-31`); an unknown `CASSETTE_VERSION` is "a named
  refusal, not a silent per-line skip" (`:26-27`); torn tail lines are counted
  and reported, not dropped (`:32-33`); and a replay MISS is a typed terminal
  error, never a silent live fetch (`:13-17`).
- **verdict:** keep ours
- **why:** ours, decisively, and it follows from a threat the source does not
  have (a plain file on the operator's box) plus a discipline it does not apply
  (never let a fidelity failure degrade quietly). The attempt-selection rule
  (`CassetteStart`, `:35-52`) — the cassette survives exactly when a durable
  checkpoint does — is a subtlety the source has no equivalent of.

### Playback timing neutralization
- **source:** `core/.../Services/Http/RecordingRedirectHandler.cs:64-82` strips
  `Retry-After`, `x-ms-retry-after-ms` and `retry-after-ms` and rewrites them to
  zero **in playback only** (`if (_playbackTesting)`), so retry paths execute
  without sleeping.
- **pumper:** replay already achieves this by construction — "Replay runs touch
  no engine, obey no politeness delay, and spend $0 (every metered seam records a
  `vcr_replay` cost event at 0.0)" (`crates/core/src/vcr.rs:15-17`).
- **verdict:** keep ours
- **why:** same outcome, one layer up: we skip the sleep because we skip the
  engine, rather than by rewriting a header on the way back. Recording a zero
  cost event rather than no cost event is the better detail — a replay that
  spends nothing and *says so* is auditable against the original run.

---

## Contract evolution and deprecation

### No deprecation window, and it is deliberate
- **source:** renames are hard breaks. `docs/tool-rename-checklist.md:3` — "Tool
  names form part of the MCP protocol surface… so renames are **breaking
  changes**" — and the checklist's remedy is a six-section migration
  (source, docs, recordings, unit tests, changelog, consumers), not an alias or a
  window. Verified: `grep -ril "deprecat" docs/` returns **zero** files across
  the 12 docs and 3 subdirectories in `docs/`.
- **pumper:** also no deprecation mechanism, and also no rename checklist —
  the closest thing is the append-only convention enforced by the ordered e2e
  assertion (`crates/server/src/e2e/mcp.rs:177-192,258-262`).
- **verdict:** keep ours (the policy) / adopt (the compensation)
- **why:** the *policy* is right for both of us — an alias for a tool name is a
  second name an agent can learn, and agents do not read migration notices. But
  the source compensates for hard breaks with three things we lack: a batching
  discipline (breaks land in marked releases), a mandatory changelog entry
  (`docs/changelog-entries.md`), and executable consumer pins. We have the third.
  We should have the second.

### The generated contract does not cover the MCP surface
- **source:** the tool surface is documented in
  `servers/Azure.Mcp.Server/docs/azmcp-commands.md`, kept current by a checklist
  item (`docs/tool-rename-checklist.md:32`), and additionally shaped by
  `consolidated-tools.json` (`:34`).
- **pumper:** `clients/openapi.json` is generated from the router and a
  `cargo test` asserts the committed copy matches (`CLAUDE.md`, `just openapi`) —
  and it contains **zero** occurrences of `mcp` (verified by count). Every REST
  route flows into three generated SDKs; `/mcp` flows into none of them. The MCP
  surface's only machine-readable definition is the `Vec<Value>` in
  `server_tools`, and its only pin is the e2e list.
- **verdict:** adapt
- **why:** this is a structural asymmetry, not an oversight — JSON-RPC over one
  POST route genuinely does not fit an OpenAPI document, so "add it to openapi"
  is the wrong fix. The right one is smaller: emit the `tools/list` payload as a
  committed artifact under `clients/` and diff it in CI, exactly as
  `just clients-check` diffs the generated SDKs. Then a tool rename, a
  description change or a schema edit shows up as a reviewable diff instead of as
  a passing test with a new string in it.

### Structured output is a mode there and an invariant here
- **source:** `--structured-output-mode` is tri-state:
  unset (content only), `duplicated` (content retained), or `compact`
  (`ServerStartOptions.cs:112-117`) — because some clients choke on
  `structuredContent` and some want it without the duplicated text.
- **pumper:** always both. Every successful call returns `content[0].text` (the
  JSON stringified) *and* `structuredContent`
  (`crates/server/src/mcp/mod.rs:770-776`); errors return content only.
- **verdict:** keep ours
- **why:** the mode exists to serve a client population we do not have. Always
  emitting both is the compatible choice, and the duplication costs bytes on a
  loopback connection. If a client ever chokes, the fix is one `if`, not a
  released flag.

---

## Configuration and dangerous options

### Dangerous options are named dangerous
- **source:** four options carry the prefix in their own names —
  `DangerouslyDisableHttpIncomingAuth` (`ServerStartOptions.cs:60-61`),
  `DangerouslyDisableElicitation` (`:67-68`),
  `DangerouslyWriteSupportLogsToDir` (`:83-84`),
  `DangerouslyDisableRetryLimits` (`:90-91`) — so `--help` and every shell
  history line reads as a warning.
- **pumper:** `allow_enqueue`, `allow_approve`, `allow_live` — permissive names,
  neutral tone. The warning lives in the doc comments
  (`crates/core/src/config.rs:257-263`) and the refusal text, not the key.
- **verdict:** adapt
- **why:** worth taking the *idea* and not the prefix. Renaming config keys is a
  breaking change for every operator's `config.toml` and buys little for a file
  you edit once. But `[transact] allow_live` is genuinely the most dangerous key
  in the repo — it authorizes an irreversible action under the operator's
  logged-in identity — and it is named like a feature flag. At minimum the
  `just`/startup banner should say, on boot, which dangerous switches are on. A
  process that is one flag from submitting live forms should say so out loud once
  per start.

### Refused combinations, checked at startup
- **source:** `--dangerously-disable-http-incoming-auth` with stdio transport is
  a startup error naming the fix (`ServerStartCommand.cs:227-232`); HTTP
  transport outside the Docker build is a startup error
  (`:236-244`); `--namespace` with `--tool` is a startup error (`:254-263`).
  **But** — correcting the seed — only *one* of the four `Dangerously*` options
  is combination-checked; `DangerouslyDisableElicitation`,
  `DangerouslyWriteSupportLogsToDir` and `DangerouslyDisableRetryLimits` are
  accepted in any combination, including all at once.
- **pumper:** `Config::validate` refuses a negative `max_job_budget_usd` **only
  when `[mcp] enabled`** (`crates/core/src/config.rs:1136-1145`), and the test
  proves the conditionality in both directions plus the zero case
  (`:2539-2552`). `approve_transaction`'s two-switch requirement is enforced at
  dispatch rather than at startup (`crates/server/src/mcp/mod.rs:744-748`).
- **verdict:** keep ours
- **why:** we already have the good half — validation that binds only when the
  subsystem is on, tested for the "disabled ⇒ rule doesn't bind" direction, which
  is the one that usually rots. And ours is more consistent than theirs: the
  seeded claim that the source refuses dangerous options "in incompatible
  combinations" is true of exactly one option out of four.

### Ships disabled, and ships read-only
- **source:** ships **open**. `--read-only` defaults to `null`/false
  (`ServerStartOptions.cs:46-47`), the default mode exposes 25 namespace tools
  covering writes and deletes (`CHANGELOG.md:2686`), and `Destructive` defaults
  to `true` for an unannotated tool (`ToolMetadata.cs:30`). Safety comes from the
  elicitation round trip at call time, not from the shipped posture.
- **pumper:** ships **closed, twice**. `McpConfig::default()` is
  `enabled: false, allow_enqueue: false, allow_approve: false`
  (`crates/core/src/config.rs:237-247`), and
  `mcp_ships_disabled_and_read_only` asserts it with the reason in the test body:
  "An agent-actuatable surface must be double opt-in: mount, then enqueue"
  (`:2526-2536`).
- **verdict:** keep ours
- **why:** the forces genuinely differ — a cloud service that ships inert is a
  cloud service nobody can use, so the source buys back safety with consent at
  call time. But a local-first binary has no such pressure, and default-closed is
  the right posture for one: the operator who flips a switch has read the switch.
  The part that is ours rather than merely different is that the default is
  *asserted by a test with its rationale inline*, so a future default change must
  argue with the test.

---

## Packaging and distribution

### One artifact per host vs. one binary
- **source:** ships everywhere at once — NuGet/dotnet tool, a Docker image with
  its own `ENABLE_HTTP` compile flag (`Dockerfile`;
  `ServerStartCommand.cs:238-243`), `smithery.yaml` for the Smithery registry,
  `server.json`, an `mcpb/` bundle, a `vscode/` extension slice, and `azd`
  templates for two hosted auth topologies (`docs/Authentication.md:138-146`).
- **pumper:** one binary, three bins, no `default-run`, run from the repo root
  (`CLAUDE.md` §Commands), MCP reached at `http://localhost:8088/mcp` with a
  four-line `.mcp.json` snippet (`docs/features/mcp.md:50-62`).
- **verdict:** different forces
- **why:** distribution surface is a function of who installs you. Nothing here
  transfers; recorded so the study does not read as if we forgot to look.

### A compile flag gates a transport
- **source:** HTTP transport exists only in the Docker distribution, enforced at
  startup with a message naming the distribution
  (`ServerStartCommand.cs:236-244`) and at compile time via `#if ENABLE_HTTP`.
  Two artifacts, two capability sets, one codebase.
- **pumper:** no build-time capability gating anywhere on this path; every
  capability is a runtime config key.
- **verdict:** keep ours
- **why:** compile-time capability sets mean the binary an operator runs is not
  the binary the tests ran, which is the same family of hazard as the
  `#if DEBUG` annotation check above. Runtime switches with startup validation
  give us one artifact whose behaviour is fully described by its config — worth
  more to us than the attack-surface reduction, since our HTTP listener is
  loopback by default anyway.

---

## Tests to initiate

Paired, with the instrument and the number that moves.

1. **Unknown-argument rejection, both directions.**
   - *Instrument:* new `#[tokio::test]` in `crates/server/src/e2e/mcp.rs`.
   - *A:* `tools/call` `search` with `{"q":"x","limitt":5}` → today
     `isError == false` and the call runs at the default limit. After the fix:
     `isError == true` and the message contains `limitt`.
   - *B:* the same call with `{"q":"x","limit":5}` → `isError == false` and the
     recording harness's captured `SearchRequest.limit == 5` (the
     `mcp_state_recording` fixture already exists,
     `crates/server/src/e2e/mcp.rs:81`).
   - *Number:* count of accepted-but-unmatched argument keys across the suite,
     from 1 (today, silently) to 0.

2. **Advertised schema equals enforced schema.**
   - *Instrument:* a table-driven test walking every entry of `server_tools`,
     reading `inputSchema.properties` keys, and calling each tool once with a
     bogus extra key.
   - *A:* every tool declaring `additionalProperties: false` refuses.
   - *B:* every tool accepts each of its declared keys without a
     "unknown argument" refusal — the guard against a hand-written allowlist
     drifting from the schema.
   - *Number:* tools where advertised strictness ≠ enforced strictness, from
     **19/19** to 0.

3. **Per-tool call telemetry exists and distinguishes.**
   - *Instrument:* `tracing_test`-style subscriber capture around two
     `handle_rpc` calls.
   - *A:* a `search` call emits one span with `tool="search"`, `outcome="ok"`.
   - *B:* a refused `approve_transaction` emits `tool="approve_transaction"`,
     `outcome="refused"` — distinguishable from A.
   - *Number:* distinguishable MCP call events per 100 calls, from 0 to 100.

4. **Cassette redaction.**
   - *Instrument:* `crates/core/tests/vcr.rs`.
   - *A:* record a fetch whose response carries `set-cookie` and
     `www-authenticate`; assert the on-disk NDJSON contains neither value.
   - *B:* replay that same cassette and assert the `FetchOutcome` is still
     served (redaction must not break `req_hash` resolution, which keys on
     method+url only — `crates/core/src/vcr.rs:28-31`).
   - *Number:* credential-bearing header values persisted to a
     retention-exempt file, from unbounded to 0.

5. **The tool-surface diff is reviewable.**
   - *Instrument:* a committed `clients/mcp-tools.json` plus a `cargo test`
     comparing it to `server_tools` under a fixed config, in the style of the
     existing `just openapi` assertion.
   - *A:* renaming a tool or editing a description fails the test with a diff.
   - *B:* regenerating and committing makes it pass.
   - *Number:* MCP surface changes that reach `main` without a reviewable diff,
     from all of them to 0.

---

## Ranked features pumper could gain

1. **★ THIS RUN'S PROPOSAL — reject unknown `tools/call` arguments, with a
   case/separator-tolerant match for the known ones.**
   In scope because the schema that promises it is already written
   (`additionalProperties: false` on all 19 tools) and the project's stated
   doctrine is that an overstated contract is worse than a missing one — the
   manifest says exactly that about a phantom `prePush` rung
   (`.ai/manifest.yaml`, `controls:`). One helper in
   `crates/server/src/mcp/mod.rs`, called once at the top of `tools_call`,
   driven off the schemas already in `server_tools`. Tests 1 and 2 above.
   Ships strictness and tolerance together, per the "Spelling forgiven"
   point — landing the strictness alone would be a regression for a week.

2. **Redact credential-bearing headers before they reach a cassette.**
   In scope because cassettes are deliberately long-lived, plain-text, and
   readable by anything on the box (`crates/core/src/vcr.rs:21-23`) — the doc
   already treats the file as untrusted on *load*; the same reasoning applies on
   *write*. Ranked second only because it needs a judgement call about which
   headers, which is a decision, not a mechanic.

3. **Annotate the four spec hints on every tool.**
   In scope because the dispatch already partitions the tools by exactly these
   properties (`crates/server/src/mcp/mod.rs:723-767`), so the annotations are
   transcription, not new analysis. Lets a client render blast radius without
   parsing our prose.

4. **One span per `tools/call`, with the tool name and outcome.**
   In scope because `tracing` and `/metrics` are already wired
   (`docs/features/observability.md:5-10`) and the module currently emits two
   warns and nothing else. Unlocks a per-tool counter on the existing endpoint.

5. **A committed `tools/list` artifact, diffed in CI.**
   In scope because `just clients-check` already establishes the idiom for the
   REST surface, and `just inventory` establishes that non-Rust gates belong in
   `ci`. Closes the one contract in the repo with no generated counterpart.

6. **An auth/switch combination table in `docs/features/mcp.md`.**
   In scope because doc-sync is an existing habit with a hook behind it, and the
   reasoning is already written — it is just scattered across `config.rs`,
   `agent_tools.rs` and `auth.md`.

7. **A changelog convention for MCP surface changes.**
   In scope, lowest ranked because the append-only rule plus the ordered e2e
   assertion already prevents the silent break; this is about telling operators,
   which matters more once there is a second operator.

Exactly one — **item 1** — is marked as this run's proposal, per the one-per-project cap.

---

## What pumper does better

Specific, and not grudging.

1. **Job-scoped, ownership-revoked attribution tokens.** The source has an
   *acknowledged* over-broad-token problem it cannot fix at its layer — RFC 8707
   resource indicators are unsupported by its own identity provider, so tokens go
   out unscoped and the documented mitigation is "monitor token usage patterns"
   (`docs/Authentication.md:172-184`). We solved the same class of problem by
   scoping the credential to the *unit of work* instead of to the caller: a
   256-bit token bound to one `job_id`, revoked by `Drop` rather than by a
   sweeper that may not run (`crates/core/src/agent_tools.rs:143-149`), carried
   in a deliberately non-`Authorization` header so two credentials never collide
   (`:35-38`), re-checked against the job's *status* at spend time so a late call
   cannot bill a finalized job (`crates/server/src/mcp/jobtoken.rs:62-76`), and
   resolved into four arms that keep `Expired` distinct from `Unknown`
   "because the two mean different things to whoever is reading the logs after a
   leak" (`agent_tools.rs:99-105`). Five tests, each named for the anti-pattern
   it forbids (`:184-241`). This is better than the peer, not merely different.

2. **A replay system that declares its own fidelity and refuses rather than
   degrades.** `ReplayFidelity` is three-valued with a per-app table and a reason
   string per entry (`crates/core/src/vcr.rs:171-194,216-320`); an unreplayable
   app's replay is refused *before anything runs* (`:69-71`); a replay MISS is a
   typed terminal error, never a silent live fetch (`:13-17`); the cassette is
   verified on load, not trusted — version-gated with a named refusal,
   `req_hash` recomputed from the entry's own method+url, torn tail lines counted
   rather than dropped (`:26-33`); and which attempt's recording survives is
   decided by one stated rule rather than by last-writer-wins
   (`:35-52`). The source's recorded tests are excellent engineering, but a
   playback run there cannot tell you *how much of it was actually recorded*.
   Ours can, per app, by name, with the reason.

3. **Graded authority that is asserted, not assumed, and refusals that name the
   fix.** Three separate switches whose non-nesting is argued in prose
   (`docs/features/mcp.md:40-45`), enforced at both the listing and the dispatch,
   and — the part that makes it durable — *tested for the implication that must
   not hold*: "allow_enqueue must not imply the authority to release an
   irreversible action" (`crates/server/src/e2e/mcp.rs:236-240`). Every refusal
   names the config key the operator must set, and the irreversible one ends
   "Nothing was submitted", pinned by the test (`:216`). Compare the source's
   single `--dangerously-disable-elicitation`, which disables consent for every
   tool at once and is accepted in any combination with the other three dangerous
   flags. Ours is the better shape and it is defended by tests rather than by
   review.

Three more that did not make the podium but are real: the tool surface is
**pinned in order, exhaustively** (`crates/server/src/e2e/mcp.rs:177-192`) where
the source can only afford to pin two names; nested payloads are **JSON-Schema
validated to the pointer** with nothing enqueued on failure (`:385-402`); and
every refusal carries **the HTTP status the REST surface would have used**
(`crates/server/src/mcp/jobtoken.rs:78-87`), so an agent can tell "stop" from
"retry" without parsing prose.

---

**Verdicts:** 42 points — keep ours 28, adapt 7, adopt 3 (one of them scoped to
the discipline rather than the change), different forces 3, plus 1 split
(`keep ours` on the policy, `adopt` on the compensation) under "No deprecation
window".
