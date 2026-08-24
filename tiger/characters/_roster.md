---
type: tiger/roster
app: pumper
---

# Character roster - pumper

_Not yet drafted. `/tiger init` asks how many (1 / 5 / 10) and derives them._

pumper's Characters are **other agents and apps on this machine** plus the humans behind them -
the consumers named in `ONBOARDING.md` section 4 and in `catalog/data-sources.toml`
(`confidence`, `category`). Derive them from real consumers, not a generic roster. There is no
`uat/` overlay in this repo to reuse.

Each Character carries, per the lane's schema: who they are / background / voice, jobs to be
done (what they hire the MODEL OUTPUT for), a senior-quality bar, time-saved as a NUMBER
(manual-research minutes -> with-pumper minutes), and scored acceptance criteria applied to the
OUTPUT:

- [ ] grounded in MY real context (names the supplied URL / entity / dataset, no placeholders)
- [ ] senior-grade (specific, correct, citable, not generic)
- [ ] machine-usable (parses against the declared `json_schema`; keys stable across runs)
- [ ] worth the latency/cost (vs the http or browser tier doing it deterministically)

`maps_to` binds each Character to the call sites their JTBD hits (see `../README.md`).
Must-pass judges in **bold**.

| character | AI-surface angle | maps_to | use_case |
|---|---|---|---|
