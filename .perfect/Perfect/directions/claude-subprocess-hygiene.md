---
slug: claude-subprocess-hygiene
type: perfect/direction
context: "[[claude-engine]]"
lens: robustness
status: shipped
size: M
proposed: 2026-08-12
accepted: 2026-08-12
shipped: 2026-08-12
commit: b0d363c
---

## What & why
Three hygiene holes in what the engine hands the subprocess. (1) `--json-schema`
passes raw JSON (full of quotes) as a **cmd.exe** argument on Windows — six
production apps use schemas; cmd re-parses with its own rules (`%` expansion, `&`,
`^`, quote semantics), so an unlucky schema or system-prompt mangles the argument
or worse; the code comments the hazard for `--append-system-prompt` and then takes
the same path for schemas without a word. (2) The `model` param flows from
`POST /jobs` body → `--model <value>` completely unvalidated (free string,
`additionalProperties: true`), and a typo'd `role` silently falls through to
defaults (documented 2026-07-14, still live). (3) The subprocess inherits the
server's CWD and full env with `bare = false`: in dev every scraping research call
loads THIS repo's 226-line CLAUDE.md + skills into its system prompt and fires the
doc-sync Stop hook — paid tokens and latency spent on agent config that has nothing
to do with the job. The user moment: "my extraction schema broke the call with an
unreadable cmd.exe error / my research answers cost more and mention repo
conventions."

## Evidence
- `crates/engine-claude/src/lib.rs:80-83` — schema as inline arg; `:84-90` — the
  comment admitting cmd.exe mangling for system prompts; `:94-100` — the cmd /C path.
- `crates/apps/research/src/lib.rs:232,246` — `model` free-string reaches `--model`;
  role enum-constrained ONLY in the research app, free in the other seven.
- `crates/engine-claude/src/lib.rs:29` — unknown role name → `None` → silent
  fall-through to defaults (bughunt engine-capability-traits.md:16-21, un-actioned).
- `crates/engine-claude/src/lib.rs:94-102` — no `current_dir`, full env inherited;
  `crates/core/src/config.rs:1271` — `bare: false` default; `.claude/settings.json`
  Stop hook fires per call in dev.

## Acceptance criteria
- [ ] A named, tested, pure guard function validates every arg destined for the
      cmd.exe path: values containing cmd metacharacters that cannot survive the
      shim (`%`, `&`, `|`, `^`, `<`, `>`, `"` per actual cmd re-parsing rules) and
      args exceeding a measured safe length are REFUSED with a clear typed error
      naming the offending flag — refuse loudly, never mangle. Test named for the
      anti-pattern (e.g. `cmd_args_refused_not_mangled`). Builder should first
      check (live, `claude --help` on this box) whether schema/system-prompt can
      travel by file or stdin instead — if yes, prefer moving them off argv
      entirely and the guard covers what remains.
- [ ] An unknown `role` is an ERROR naming the known roles, not a silent
      fall-through to defaults (`resolve()` seam; typed, tested). Requests with no
      role keep working unchanged.
- [ ] `model` values are validated against a conservative pattern
      (e.g. `^[A-Za-z0-9._:-]+$`) at the engine door — a garbage model string gets
      a typed refusal, not a subprocess parse error.
- [ ] The subprocess runs with an explicit `current_dir` that is NOT the server's
      CWD — a dedicated dir under the storage root (create if missing) so CLAUDE.md
      discovery, hooks, and skills come from a neutral, empty context. Document the
      new behavior + the `bare` interplay in docs/features/fetching.md. (Changing
      the `bare` DEFAULT is out of scope — the CWD isolation captures most of the
      win without changing CLI feature semantics.)
- [ ] `resolve()` precedence gets its first tests (request > role > config, per
      field independently).

## Risks / non-goals
- Non-goal: sandboxing the subprocess's tool access (`skip_permissions` posture is
  documented deployment policy — do not change it here).
- Non-goal: an allowlist of model IDs (a pattern check, not a catalog).
- Risk: CWD change could affect a deployment that relied on repo-CWD `.claude/`
  context intentionally — say so in the doc and name the escape hatch (running with
  a config-declared dir if one is ever needed; do NOT add a config key
  speculatively).

## Build record
Continuation builder (opus, Lot C2, session -4 brief) shipped `b0d363c`. Review
verdict KEEP (session -5, diff read in full): `check_shim_argv` refuses the MEASURED
cmd.exe hostiles (`% & | < > ^` + NL/CR — each behavior probed through a real
`cmd /C` round-trip, not taken from a table) and budgets the rendered line at 8000
chars (measured cliff: 8008 through, 8108 refused), naming the offending flag; the
binary path itself is vetted too. **Accepted deviation from the criteria:** `"` is
deliberately NOT refused — measurement showed a real JSON schema round-trips
byte-exact and refusing quotes would break all six schema-using apps; the criterion
said "per actual cmd re-parsing rules" and this IS the actual rule. System prompts
left argv entirely (`--append-system-prompt-file`, verified live in `claude --help`
by builder AND Director; Drop-cleaned scratch file, pid+counter named). Unknown
`role` → error naming configured roles; `model` gated by `[A-Za-z0-9._:-]{1,128}`;
both pre-spawn (Spawn class — nothing ran, no ledger row). CWD isolation:
`isolation_dir` is `serde(skip)`, DERIVED as `<storage root>/claude-cwd` in
state.rs, with a test pinning that it is not a config key; docs state the honest
limits (CLAUDE.md discovery walks upward; `[claude] bare` is the complete lever).
`resolve()` precedence tested per field. Fake-CLI harness extended — argv recorded
two ways (`%~1` splits on commas, `%*` is byte-exact) because neither alone is
honest on Windows. Wave gate: `just ci` exit 0 on the wave tip (session -5).
