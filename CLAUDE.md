# CLAUDE.md — ai-staff project context

> **Read this first.** This file is your primary context for every session.
> It is written for Claude and covers everything you need to work effectively
> on this codebase without re-deriving architecture from scratch.

---

## Quick orientation

**What this is:** `ai-staff` — a personal AI technical staff tool.  
CLI command: `crew`. npm package: (open-source, planned).  
13 Claude-powered agents, orchestrated locally, zero cloud infra.

**Verify the build works before touching anything:**
```bash
cd ~/Full\ Stack\ AI\ Ninja\ Team/ai-staff
npm run build          # must print three tsc lines with no errors
crew --version         # must print 0.1.0
crew agents list       # must print all 12 specialist agents
```

**Monorepo layout:**
```
packages/
  core/     @ai-staff/core    — types, db, runner, config, memory, cost
  agents/   @ai-staff/agents  — 13 AgentDefinition objects
  cli/      @ai-staff/cli     — crew CLI (commander)
tsconfig.base.json            — shared TS config (CommonJS, ES2022, strict)
CLAUDE.md                     — this file
```

---

## Architecture

### The 13 agents

| Tier | ID | Export name | Role |
|------|-----|-------------|------|
| 0 | `chief` | `ChiefAgent` | Orchestrator / router — primary interface |
| 1 | `cto` | `CTOAgent` | Architecture, tech stack decisions |
| 1 | `pm` | `PMAgent` | Product, roadmap, prioritisation |
| 1 | `ai-researcher` | `AIResearcherAgent` | Papers, models, SOTA |
| 2 | `backend` | `BackendAgent` | APIs, databases, services |
| 2 | `frontend` | `FrontendAgent` | UI, DX, accessibility |
| 2 | `ml` | `MLEngineerAgent` | Training, inference, pipelines |
| 2 | `reviewer` | `ReviewerAgent` | Code review (adversarial capable) |
| 3 | `infra` | `InfraAgent` | DevOps, IaC, SLOs |
| 3 | `security` | `SecurityAgent` | OWASP, threat modelling (adversarial) |
| 3 | `evaluator` | `EvaluatorAgent` | Evals, benchmarks, LLM-as-judge |
| 3 | `docs` | `DocsAgent` | Docs, ADRs, runbooks |

**Chief of Staff (Tier 0)** is the only agent the user should talk to directly for routing queries. Specialists are called explicitly via `crew ask <id>`.

### Data flow for a single `crew ask` call

```
crew ask cto "..."
  → runAsk()
    → await initDb()          ← sql.js WASM loads, opens/creates memory.db
    → runAgent(CTOAgent, opts) ← async generator
      → retrieveContext()     ← FTS5 search on memories table
      → client.messages.stream()  ← Anthropic API, adaptive thinking
      → yield 'text' / 'thinking' / 'cost' / 'done' events
      → write cost_ledger row
      → extractMemoriesAsync()  ← background Haiku call, writes memories
    → renderStream()          ← prints to terminal with chalk
```

### Memory scopes

| Scope | Key | Who uses it |
|-------|-----|-------------|
| `project` | project name (basename of cwd) | All agents — persists per-project facts |
| `global` | `__global__` | Chief — cross-project lessons |
| `agent-persona` | agent id | Per-agent preference memory (future) |

Memory extraction happens after every `runAgent()` call via a background Haiku pass. It is **non-blocking and non-fatal** — failure is silently swallowed.

---

## Build system

### CRITICAL: package build order is enforced

```bash
npm run build   # runs: core → agents → cli (in that exact order)
```

**Never** run `tsc --build` on agents or cli before core is built. The workspace symlink for `@ai-staff/core` points to `packages/core/dist/`, which must exist.

TypeScript project references are set up:
- `core/tsconfig.json` → `composite: true`
- `agents/tsconfig.json` → `composite: true`, `references: [{path: "../core"}]`
- `cli/tsconfig.json` → `references: [{path: "../core"}, {path: "../agents"}]`

### After modifying core types

Any change to `packages/core/src/types/index.ts` requires:
1. `npm run build -w @ai-staff/core` first
2. Then build agents/cli — they'll pick up new `.d.ts` files from core's `dist/`

### NEVER do this

```bash
# WRONG — agents runs before core in alphabetical workspace order:
npm run build --workspaces --if-present

# WRONG — adds a path override that causes rootDir violations:
# "paths": { "@ai-staff/core": ["../core/src/index.ts"] }
# (This was the bug we fixed. Agents must resolve @ai-staff/core via node_modules.)
```

---

## Database layer (`packages/core/src/db/index.ts`)

### Why sql.js (not better-sqlite3)

`better-sqlite3` requires native compilation via `node-gyp`. On macOS with Anaconda installed, GNU libtool shadows macOS libtool and breaks the build. On Linux CI, additional system packages are required. `sql.js` is pure WASM — zero native compilation, works on any system.

### How sql.js works here

1. **WASM loads once** on first `await initDb()` — reads `sql-wasm.wasm` from node_modules via `require.resolve()`
2. **Entire DB is in-memory** — loaded from `~/.ai-staff/data/memory.db` on open, or created fresh if not found
3. **Write-through to disk** — `_persist()` is called after every mutation (INSERT/UPDATE/DELETE). During transactions, it's suppressed and called once at COMMIT.

### API contract (mirrors better-sqlite3)

```typescript
await initDb()                    // async — call once at startup, idempotent
const db = getMemoryDb()          // synchronous after initDb() completes
db.prepare(sql).run(...params)    // write (auto-persists)
db.prepare(sql).all(...params)    // read → Record<string, unknown>[]
db.prepare(sql).get(...params)    // read → Record<string, unknown> | undefined
db.transaction(fn)(arg)           // wraps fn in BEGIN/COMMIT (one persist at end)
db.exec(sql)                      // multi-statement DDL (no params)
db.pragma(str)                    // PRAGMA — WAL is silently ignored (in-memory)
```

### CRITICAL: initDb() must be awaited

`initDb()` is async. Any code that calls `getMemoryDb()` / `getDb()` must have awaited `initDb()` first. Failing to do this throws:
```
Database not ready. Call `await initDb()` before using the database.
```

Places that await initDb():
- `runAgent()` — awaits at the very start of the generator
- `commands/ask.ts` → `runAsk()`
- `commands/init.ts` → `runInit()`
- `commands/history.ts` → `runHistory()`

### FTS5 (full-text search on memories)

sql.js is compiled with FTS5 enabled. The `memories_fts` virtual table and its sync triggers are created as part of `initMemorySchema()`. Memory retrieval in `packages/core/src/memory/retrieval.ts` uses `MATCH` queries against this table.

---

## Agent naming convention

**RULE: Export names must exactly match the pattern below. Mismatches cause TS2724 at build time.**

| Agent file | Exported const name |
|-----------|-------------------|
| `chief.ts` | `ChiefAgent` |
| `cto.ts` | `CTOAgent` |
| `pm.ts` | `PMAgent` |
| `ai-researcher.ts` | `AIResearcherAgent` |
| `backend.ts` | `BackendAgent` |
| `frontend.ts` | `FrontendAgent` |
| `ml.ts` | `MLEngineerAgent` |
| `reviewer.ts` | `ReviewerAgent` |
| `infra.ts` | `InfraAgent` |
| `security.ts` | `SecurityAgent` |
| `evaluator.ts` | `EvaluatorAgent` |
| `docs.ts` | `DocsAgent` |

When adding a new agent: create the file, add the export to `index.ts` under both the `export { ... }` section AND the `ALL_AGENTS` array. Run `npm run build` to validate immediately.

---

## SDK patterns and type workarounds

### Prompt caching (`cache_control`)

The `cache_control` field on `TextBlockParam` is defined in the SDK's beta namespace (`resources/beta/messages/messages.d.ts`) but not in the main namespace. We use a local type extension:

```typescript
type CacheableTextBlock = Anthropic.TextBlockParam & {
  cache_control?: { type: 'ephemeral' };
};
// Cast when passing to stream():
system: systemBlocks as Anthropic.TextBlockParam[]
```

**Never add `@ts-ignore` to suppress this** — the local type extension is the correct approach.

### Adaptive thinking

The SDK typedef for `thinking` only allows `'enabled' | 'disabled'`. The API also accepts `'adaptive'` (self-budgeting). We cast:

```typescript
thinking: { type: 'adaptive' } as unknown as Anthropic.ThinkingConfigParam
```

When the SDK types are updated to include `'adaptive'`, remove this cast. Check by removing it and running `npm run build -w @ai-staff/core`.

### `cache_read_input_tokens`

Present in the API response at runtime but not in the `Usage` typedef:

```typescript
cacheReadTokens =
  (event.message.usage as unknown as Record<string, number | undefined>)
    .cache_read_input_tokens ?? 0;
```

Same: check when upgrading SDK; remove the double-cast if Usage now includes the field.

### Where all three live

`packages/core/src/agents/runner.ts` — search for `as unknown` to find all three locations.

---

## AgentEvent discriminated union

All types in `AgentEvent` (`packages/core/src/types/index.ts`):

```typescript
type AgentEvent =
  | { type: 'text';     agentId; text: string }
  | { type: 'thinking'; agentId; text: string }
  | { type: 'tool_use'; agentId; toolName: string }
  | { type: 'cost';     agentId; costUsd; model; inputTokens; outputTokens; cacheReadTokens }
  | { type: 'done';     agentId; result: AgentRunResult }
  | { type: 'error';    agentId; message: string }
```

The `renderStream()` function in `packages/cli/src/renderer.ts` handles all types exhaustively via a `switch` with an `assertNever()` default. If you add a new event type but don't handle it in the renderer, TypeScript will error at compile time — not at runtime.

**NEVER use if/else chains for event types.** Always use the exhaustive switch.

---

## Config and file locations

```
~/.ai-staff/
  .env              ANTHROPIC_API_KEY=sk-ant-...  (chmod 600)
  config.json       { anthropicApiKey, defaultModel, dataDir, currentProject, defaultProjectRoot }
  data/
    memory.db       SQLite (sql.js format) — memories, sessions, cost_ledger, etc.
```

### Config functions (`packages/core/src/config/index.ts`)

| Function | Behaviour |
|----------|-----------|
| `loadConfig()` | Reads config.json. **Throws** if not found — only call after `crew init` |
| `getDefaultConfig()` | Returns partial defaults — safe to call before init |
| `configExists()` | Non-throwing check — use as guard before `loadConfig()` |
| `saveConfig(cfg)` | Writes config.json |
| `ensureDataDir()` | Creates `~/.ai-staff/data/` if not present |

**Pattern for commands that work with or without init:**
```typescript
const config = configExists() ? loadConfig() : { ...getDefaultConfig(), anthropicApiKey: process.env.ANTHROPIC_API_KEY! };
```

---

## CLI commands

| Command | File | Description |
|---------|------|-------------|
| `crew init` | `commands/init.ts` | Prompts for API key, writes `.env` + `config.json`, awaits `initDb()` |
| `crew ask <id> "<q>"` | `commands/ask.ts` | Loads env, resolves agent, awaits `initDb()`, calls `runAgent()`, pipes to `renderStream()` |
| `crew agents list` | `commands/agents.ts` | Prints all agents grouped by tier — no DB needed |
| `crew history` | `commands/history.ts` | Awaits `initDb()`, queries `sessions` table |

---

## runAgent() signatures

```typescript
// Convenience (CLI uses this):
runAgent(agent: AgentDefinition, opts: Omit<AgentRunOptions, 'agent'>)

// Full form:
runAgent(opts: AgentRunOptions)
```

Discriminated at runtime by `'systemPrompt' in agentOrOpts`.  
`sessionId` is auto-generated via `randomUUID()` if not provided.  
`project` defaults to `'default'` if not provided.

---

## TypeScript configuration rules

**Target:** `CommonJS` module — this is intentional and must not change without migrating all dependencies.

**Why CommonJS and not ESM:**
- `chalk@4` (CommonJS-only for v4) 
- `ora@5` (CommonJS-only for v5)
- `better-sqlite3` — now replaced with `sql.js`, but the project stays CJS

**If you ever see ESM-only errors** (e.g., "must use import to load ES module"), the likely cause is a dependency that requires ESM (chalk v5, ora v8, etc.). Check the package.json version first.

**skipLibCheck is true** — necessary because `@types/sql.js` references `EmscriptenModule` types that sometimes conflict. Do not remove this.

---

## Known tech debt (honest accounting)

| Item | Location | Risk | Fix when |
|------|----------|------|----------|
| `as unknown as Anthropic.ThinkingConfigParam` | `runner.ts` | Low — API stable | SDK adds `'adaptive'` to types |
| `as unknown as Record<string, number \| undefined>` for `cache_read_input_tokens` | `runner.ts` | Low — field is always present | SDK updates `Usage` typedef |
| `as unknown as ProjectContext[]` for db result cast | `memory/store.ts` | Low — schema is fixed | Add a proper row mapper function |
| sql.js write-through on every mutation | `db/index.ts` | Low at current scale | Only matters at thousands of writes/session |
| No per-package `typecheck` script | root | Low | Add `"typecheck": "tsc --noEmit"` to each package.json |

---

## Adding a new agent (step-by-step)

1. Create `packages/agents/src/<id>.ts`:
   ```typescript
   import type { AgentDefinition } from '@ai-staff/core';
   // Convention: export name = PascalCase of role acronym + "Agent"
   export const MyNewAgent: AgentDefinition = {
     id: 'my-new',
     name: 'My New Agent',
     emoji: '🔧',
     tier: 2,
     systemPrompt: `...`,
     boardroomWeight: 0.7,
   };
   ```
2. Add to `packages/agents/src/index.ts`:
   - `export { MyNewAgent } from './my-new.js';`
   - Add `MyNewAgent` to `ALL_AGENTS` array
3. Add `'my-new'` to `AgentId` union in `packages/core/src/types/index.ts`
4. Run `npm run build` — all three packages must be error-free

---

## Common debugging

**Build fails with `TS6059: File '...packages/core/src/...' is not under rootDir`**  
→ An agents or cli tsconfig has a `paths` override pointing to core's `src/`. Remove the `paths` key. Core must be resolved via `node_modules`.

**`Database not ready` error at runtime**  
→ `getMemoryDb()` was called before `await initDb()`. Add `await initDb()` before the first db access.

**`crew ask` exits silently with no output**  
→ Usually an uncaught error in the stream generator swallowed by `renderStream()`. Add a temp `console.error` inside the `catch (err)` block in `ask.ts` to surface it.

**`tsc` errors in agents after modifying core types**  
→ Build core first: `npm run build -w @ai-staff/core`. Then rebuild agents: `npm run build -w @ai-staff/agents`.

**sql.js `run()` throws on multi-statement DDL**  
→ This should not happen — `run()` without params accepts multi-statement SQL. If it does, split statements and call `run()` individually.

---

## Planned features (do not implement without confirming with user)

- `crew board "<topic>"` — multi-agent boardroom discussion mode
- `crew plan "<goal>"` — sprint planning with PM + CTO
- Research DB (`research.db`) — arxiv indexer + nightly digest cron
- Prompts DB (`prompts.db`) — prompt regression benchmark harness
- Web UI (`packages/web/`) — `@ai-staff/web` package, React
- Tool use for agents — file read/write, web search, code execution
- Inter-agent handoff via `HandoffMessage` protocol (types already defined)
- Chief of Staff routing logic (currently manual `crew ask <id>` only)

---

## Environment

- **Node.js** ≥ 20.0.0 required (see root `package.json` `engines` field)
- **npm workspaces** — always run install/build commands from the repo root
- **Anthropic SDK** `@anthropic-ai/sdk@^0.39.0` (actually resolves 0.78.0 at time of writing)
- **sql.js** `^1.x` — WASM binary at `node_modules/sql.js/dist/sql-wasm.wasm`
- **chalk** `^4.1.2` — v4 only (CJS). Do NOT upgrade to v5 (ESM-only)
- **ora** `^5.4.1` — v5 only (CJS). Do NOT upgrade to v6+ (ESM-only)

---

*Last updated: 2026-03-09. Update this file whenever the architecture changes, a new agent is added, a type workaround is resolved, or a significant decision is made.*

---

## Post-launch fixes (2026-03-09)

### store.ts: named params → positional params

`writeMemory()`, `writeDecision()`, `setProjectContext()` previously used `@name` syntax in SQL and passed objects to `.run()`. Our sql.js wrapper only handles positional arrays. Rewrote all three to use `?` positional params — compatible with any SQLite driver.

**Rule going forward:** Never use named params (`@name`, `:name`, `$name`) in `store.ts` or anywhere calling `db.prepare().run()`. Always use `?` positional params and pass values as spread args.

### runner.ts: graceful config loading

`loadConfig()` throws if `~/.ai-staff/config.json` doesn't exist (i.e., before `crew init`). Changed to:
```typescript
const config = configExists() ? loadConfig() : getDefaultConfig();
```
This lets users run `crew ask` with `ANTHROPIC_API_KEY` set in their shell without having done `crew init` first.

### First-run checklist

Before relying on this tool for real work, run through:
1. `crew init` — sets API key and creates DB
2. `crew ask cto "What architecture should I use for a real-time chat app?"` — verifies API, streaming, DB write, memory extraction
3. `crew history` — verifies sessions table read
4. Check model IDs (`claude-opus-4-6`, `claude-sonnet-4-6`, `claude-haiku-4-5`) are accepted by the API — if you get a model error, update `ModelId` in `packages/core/src/types/index.ts`
