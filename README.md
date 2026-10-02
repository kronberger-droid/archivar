# archivar

An agent-mediated knowledge store. Text is kept as blocks with stable IDs,
every change goes through a proposal and lands in an append-only event log,
and agents can propose but never approve changes to canonical content.

Design notes live in the vault: `Work Projects/Lab File Storage/Design - Agent Mediated Storage.md`.

## Status

Slice 1, the trust core, as a CLI:

- markdown ingest and `get`, lossless round trip
- tiers (`raw`, `canonical`, `working`, `derived`) and a role x tier ACL in `core.acl`
- `propose` / `review` / `commit` / `reject`, all-or-nothing with per-block conflicts
- `history` per block, `blame` per document, naming principal and agent
- `query`: read-only SQL against the published `read` schema

Slice 2, relations:

- `link` / `unlink` between documents and blocks, each relation with its own
  tier, asserter, agent and confidence
- agents link into `derived`; only a human hand can `promote` a link to `canonical`
- `related`: walk the graph by depth, direction and tier

Not yet: the MCP server, the Word codec, purge.

## Development

```nu
nix develop
pg-up                 # throwaway Postgres in .pg/, socket only
cargo test            # one fresh database per test
```

## Trying it

```nu
archivar init
archivar principal add martin --role editor
archivar principal add claude --role agent

let doc = archivar --as martin ingest notes/cell.md --tier canonical | from json
archivar blocks $doc | from json

# Claude proposes on Martin's behalf; only Martin can commit it.
let p = archivar --as martin --agent claude propose update <block-id> "Rated for 250 bar." | from json
archivar review $p
archivar --as martin commit $p

archivar blame $doc | from json

# Claude guesses a link; Martin makes it canonical.
let other = archivar --as martin ingest notes/gasket.md --tier canonical | from json
let r = archivar --as martin --agent claude link $doc $other --kind cites --confidence 0.8 | from json
archivar --as martin promote $r
archivar related $doc --depth 2 --tier canonical | from json
archivar query "select principal, agent, kind from events order by seq" | from json
```

`ARCHIVAR_PRINCIPAL` and `ARCHIVAR_AGENT` replace `--as` and `--agent`.
