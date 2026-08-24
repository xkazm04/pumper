---
type: tiger/models
app: pumper
price_basis: "config.toml [claude.roles.*] + measured cost_usd from job results; no price table in code"
snapshot: 2026-08-24
---

# Model x thinking matrix - pumper

## The axis is native

`[claude.roles.*]` in `config.toml` defines **named presets**; `ResearchRequest` accepts
`model`, `effort` and `max_budget_usd` overrides per job. A benchmark cell is one POST with an
overridden param - no code change, no API key.

```toml
[claude]
binary = "claude"
timeout_secs = 600
# effort = "high"          # global default reasoning effort
# max_budget_usd = 2.0     # global hard spend ceiling per run
bare = false

[claude.roles.research]
model  = "claude-sonnet-5"
effort = "high"

[claude.roles.compose]
model  = "claude-opus-4-8"
effort = "xhigh"
```

Resolution happens in `ClaudeEngine::resolve`. Precedence: **explicit request field -> role
preset -> `[claude]` global default.** A Lens-C recommendation that changes a default is a
one-key edit - name the exact key (e.g. `[claude.roles.research].model`).

## Cost basis

There is no price table in this codebase; the local `claude` CLI bills and reports
`cost_usd` + turns in its JSON output, which is propagated into the job result. So:

- **Measured** cost per cell = the job result's `cost_usd`. Prefer it over any estimate.
- **Estimated** list prices, when a cell has not been run, are a dated public snapshot and must
  be labelled *estimate*.

Latency per cell comes from the job's timestamps.

## Benchmark rollups

_No cells run yet._ One row per (call-site, model, effort) with `{quality, costUsd, latencyMs,
verdict}`, keyed so a re-run of an identical (call-site, model, effort, input-hash) is free.

| call site | model | effort | quality | cost (USD) | latency (ms) | verdict |
|---|---|---|---|---|---|---|

## Standing judging rules

- **More effort is not better.** On long-form output, quality has been measured to *invert*
  above medium effort. Length is not insight; never recommend an effort upgrade on prose
  evidence alone.
- **A hard output cap collapses the effort axis.** Do not benchmark effort on a call site
  pinned by a tight `json_schema` or a low `max_turns` - you would pay for reasoning you do
  not get.
- **When every cell disappoints, suspect the prompt framing before the model** - and in this
  repo, if the target is a structured page, suspect the **tier** before the prompt. The finding
  may be "use `engine-http` + a `core::extract` rule set", which costs nothing per run.
- Forced ranking with a named separator per adjacent pair; absolute 0-5 scoring saturates.
- Judge with a model other than the one under test.
