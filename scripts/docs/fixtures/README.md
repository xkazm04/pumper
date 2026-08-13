# check-doc-sync fixtures

Redacted Claude Code transcript fixtures for `../check-doc-sync.test.mjs`
(`just doc-sync`). Each file is one `.jsonl` transcript in the shape Claude
Code actually records — the envelope key sets were lifted from real
transcripts under `~/.claude/projects/C--Users-mkdol-dolla-pumper/`
(CLI version `2.1.228`); only `message.content`, `toolUseResult`, `uuid`s and
file paths are synthetic.

`{{REPO}}` stands in for the absolute repo root so the fixtures stay portable;
the test substitutes it before running the hook.

The load-bearing detail these fixtures exist to pin: **tool results are
recorded as `type:"user"` + `message.role:"user"` entries** carrying a
`toolUseResult` key and `tool_result` content blocks. Treating them as the
user-turn boundary is what made this hook silently detect nothing for its
entire life. `shape-canary.test` in the test file re-checks that key set
against the real transcripts on this machine when they are present, so a
transcript-format change surfaces as a failing test rather than as another
silent hook.

| fixture | what it pins |
| --- | --- |
| `turn-with-edits.jsonl` | an `Edit` **after** tool results is still detected → fires |
| `turn-without-edits.jsonl` | a read-only turn stays silent |
| `turn-docs-touched.jsonl` | source + its feature doc in the same turn stays silent |
| `turn-previous-turn-edits.jsonl` | edits before the last genuine prompt are NOT this turn's |
| `turn-outside-repo.jsonl` | an edit in a sibling checkout is not this repo's feature source |
| `turn-test-only.jsonl` | a test-only edit is skipped by `SKIP_PATTERNS` |
