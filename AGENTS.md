# Repository Instructions

These instructions apply to the entire repository. Read this file before making code, test, documentation, deployment, or UI changes.

## First Steps

- Check the working tree first with `git status --short`.
- Search with `rg` / `rg --files`; exclude `node_modules/` unless the task explicitly targets vendored frontend dependencies.
- Read the smallest relevant source files before editing. Do not rely only on documentation.
- Preserve user changes. Do not revert unrelated modified files.

### Serena (LSP semantic analysis) — use FIRST

Serena provides IDE-level semantic analysis via LSP (gopls). Always use it for **precise symbol-level queries** before falling back to the knowledge graph or grep.

**MCP server:** `mcp_serena`
**Always start with:** `run_mcp` → `server_name: "mcp_serena"`, `tool_name: "activate_project"`, `args: {"project": "mysql_to_asyns"}`

#### Core tools

All Serena tools are called via `run_mcp` with `server_name: "mcp_serena"` and the tool_name below:

| Question | tool_name | Key args |
|----------|-----------|----------|
| What's in this file? (symbols) | `get_symbols_overview` | `relative_path` |
| Where is symbol X defined? | `find_symbol` | `name_path_pattern`, `include_info: true` |
| Who references symbol X? | `find_referencing_symbols` | `name_path`, `relative_path` |
| Are there compilation errors? | `get_diagnostics_for_file` | `relative_path` |
| Regex search across project | `search_for_pattern` | `substring_pattern`, `restrict_search_to_code_files: true` |
| Read a file (LSP-aware) | `read_file` | `relative_path` |

#### Tool selection priority (MUST follow this order)

1. **Serena** (`mcp_serena`) — precise symbol lookup, references, diagnostics, file overview
2. **Codebase-memory graph** (`mcp_codebase-memory-mcp`) — architecture-level call chains, cross-module impact analysis, `trace_path`
3. **Grep** — raw text search (last resort, only when Serena and graph can't answer)

#### Examples

All examples use `run_mcp(server_name: "mcp_serena", tool_name: "...", args: {...})`:

```
// Activate project (always first)
run_mcp(server_name: "mcp_serena", tool_name: "activate_project", args: {"project": "mysql_to_asyns"})

// Get file structure
run_mcp(server_name: "mcp_serena", tool_name: "get_symbols_overview", args: {"relative_path": "internal/sync/fullload/engine.go"})

// Find symbol with signature
run_mcp(server_name: "mcp_serena", tool_name: "find_symbol", args: {"name_path_pattern": "executeFullSync", "include_info": true, "max_matches": 1})

// Find all references to a symbol
run_mcp(server_name: "mcp_serena", tool_name: "find_referencing_symbols", args: {"name_path": "executeFullSync", "relative_path": "internal/task/application/service/task_service.go"})

// Check for errors
run_mcp(server_name: "mcp_serena", tool_name: "get_diagnostics_for_file", args: {"relative_path": "internal/sync/fullload/engine.go", "min_severity": 2})

// Search for patterns
run_mcp(server_name: "mcp_serena", tool_name: "search_for_pattern", args: {"substring_pattern": "UNIQUE_CHECKS", "restrict_search_to_code_files": true})
```

#### Serena vs Graph: when to use which

| Scenario | Tool |
|----------|------|
| "Who calls X and what does X call?" (full chain) | `trace_path` (graph) |
| "Where exactly is X called? Show me the line" | `find_referencing_symbols` (Serena) |
| "What's the architecture of module X?" | `get_architecture` (graph) |
| "What's the signature of function X?" | `find_symbol` (Serena) |
| "What files are in this package?" | `get_symbols_overview` (Serena) |
| "Which modules depend on this package?" | `trace_path` (graph) |

### Codebase-memory knowledge graph (Windows)

Workspace roots:

- `D:\Epan\BaiduNetdiskDownload\go\` — mysql_to_asyns, lianghua, aiops, bindip
- `D:\E\BaiduNetdiskDownload\go\` — goInception, archery

MCP server id in Cursor: `user-codebase-memory-mcp` (global) or `project-0-<workspace>-codebase-memory-mcp` (project-level). Add `.cursor/mcp.json` with `CBM_ALLOWED_ROOT` = `C:/temp/<folder>` (ASCII junction) when indexing from this machine.

#### Indexed projects (use `"project"` in every MCP call)

| Folder | MCP `project` | Local path | Index status | Junction |
|--------|---------------|------------|--------------|----------|
| `mysql_to_asyns` | **`mysql_to_asyns`** | `D:\Epan\BaiduNetdiskDownload\go\mysql_to_asyns` | ✅ indexed (3489 nodes) | `C:\temp\mysql_to_asyns` |
| `lianghua` | **`lianghua`** | `D:\Epan\BaiduNetdiskDownload\go\lianghua` | ✅ indexed (2410 nodes) | `C:\temp\lianghua` |
| `aiops` | **`aiops`** | `D:\Epan\BaiduNetdiskDownload\go\aiops` | ✅ indexed (7282 nodes) | `C:\temp\aiops` |
| `bindip` | **`bindip`** | `D:\Epan\BaiduNetdiskDownload\go\bindip` | ✅ indexed (407 nodes) | `C:\temp\bindip` |
| `goInception` | **`goInception`** | `D:\E\BaiduNetdiskDownload\go\goInception` | ✅ indexed (16908 nodes) | `C:\temp\goInception` |
| `archery` | **`archery`** | `D:\E\BaiduNetdiskDownload\go\archery` | ✅ indexed (8012 nodes) | `C:\temp\archery` |

**This repo:** always pass `"project": "mysql_to_asyns"`.

**Sibling repo in Cursor:** pass the matching `project` from the table (same as folder name).

#### Query tools (use via run_mcp)

All graph tools are called via `run_mcp` with `server_name: "mcp_codebase-memory-mcp"` and the tool_name below:

| Question | tool_name | Key args |
|----------|-----------|----------|
| Find functions/classes by keyword | `search_graph` | `query` or `name_pattern` |
| Text search enriched with graph context | `search_code` | `query` |
| Who calls X / what does X call | `trace_path` | `function_name`, `direction`: inbound/outbound/both |
| Complex graph patterns | `query_graph` | `cypher` (Cypher query) |
| Read source for a symbol | `get_code_snippet` | `qualified_name` (get from `search_graph` first) |
| Package/module overview | `get_architecture` | (no required args beyond `project`) |
| Node/edge types | `get_graph_schema` | (no required args beyond `project`) |

All calls require `"project": "mysql_to_asyns"` in args.

Examples:

```
// Search by keyword
run_mcp(server_name: "mcp_codebase-memory-mcp", tool_name: "search_graph", args: {"project": "mysql_to_asyns", "query": "full load engine", "limit": 20})

// Trace call chain
run_mcp(server_name: "mcp_codebase-memory-mcp", tool_name: "trace_path", args: {"project": "mysql_to_asyns", "function_name": "executeFullSync", "direction": "both", "depth": 3})

// Architecture overview
run_mcp(server_name: "mcp_codebase-memory-mcp", tool_name: "get_architecture", args: {"project": "mysql_to_asyns"})

// Get code snippet by qualified name
run_mcp(server_name: "mcp_codebase-memory-mcp", tool_name: "get_code_snippet", args: {"project": "mysql_to_asyns", "qualified_name": "mysql_to_asyns.internal.sync.fullload.Run"})

// Sibling project examples
run_mcp(server_name: "mcp_codebase-memory-mcp", tool_name: "search_graph", args: {"project": "aiops", "query": "runbook execution", "limit": 20})
run_mcp(server_name: "mcp_codebase-memory-mcp", tool_name: "search_graph", args: {"project": "lianghua", "name_pattern": ".*Order.*", "label": "Function"})
```

Prefer these graph tools over `rg`/Grep when exploring structure, callers, or architecture.

#### Tools not exposed in Cursor (MCP pagination — use CLI instead)

Cursor only loads the first page of MCP tools. These require the standalone CLI:

```powershell
& "$env:USERPROFILE\.local\bin\codebase-memory-mcp.exe" cli list_projects
& "$env:USERPROFILE\.local\bin\codebase-memory-mcp.exe" cli detect_changes --project mysql_to_asyns
& "$env:USERPROFILE\.local\bin\codebase-memory-mcp.exe" cli index_status --project mysql_to_asyns
```

Replace `mysql_to_asyns` with any project name from the table as needed.

#### Indexing (do not use MCP `index_repository` while Cursor is connected)

The MCP server holds the SQLite graph open and the worker exits immediately. Paths containing non-ASCII characters (e.g. `E盘`) also crash the worker. Always index through an ASCII junction: `C:\temp\<folder>`.

**This repo (`mysql_to_asyns`):**

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/index-codebase.ps1
# or: make index-codebase
```

**Sibling repos — generic recipe:**

```powershell
$cbm = "$env:USERPROFILE\.local\bin\codebase-memory-mcp.exe"
$name = "goInception"   # or aiops / lianghua / bindip / archery
$repo = "D:\E\BaiduNetdiskDownload\go\$name"   # use D:\Epan\... for Epan-path repos
$junction = "C:\temp\$name"

if (-not (Test-Path $junction)) {
  New-Item -ItemType Junction -Path $junction -Target $repo | Out-Null
}

& $cbm cli index_repository --repo-path $junction --name $name --mode full --persistence true
```

Then reload MCP servers (or restart Cursor). Artifact: `<repo>/.codebase-memory/graph.db.zst` (gitignored).

Useful context files:

- `README.md`: product behavior, public API, operations, and examples.
- `docs/design/shejiwendang.md`: DDD boundaries and no-primary-key table design.
- `docs/guides/FULL_SYNC_RESUME_GUIDE.md`: full-sync resume semantics.
- `docs/guides/INCREMENTAL_SYNC_GUIDE.md`: binlog checkpoint behavior.
- `docs/CONFIGURATION.md`: TOML/env config, storage, security, and tuning.
- `docs/testing/UNIT_TEST.md`: test strategy and package-specific coverage.

## Project Shape

This is a Go 1.24 MySQL-to-MySQL sync service with a Vue management UI.

- `main.go`: config, logger, task service, metadata analyzer, Gin router, and graceful shutdown wiring.
- `internal/api`: HTTP handlers and routing. Keep request/response translation here.
- `internal/task`: task aggregate, lifecycle, scheduling, storage, runtime isolation, and full-sync resume state.
- `internal/metadata`: schema inspection and identity selection. This owns PK/UK/no-PK strategy discovery.
- `internal/sync`: sync orchestration plus reader, writer, readonly, and match strategy implementation.
- `internal/checkpoint`: incremental binlog checkpoint persistence through Redis or memory.
- `internal/config`: TOML loading, env overrides, validation, and DB pool tuning.
- `internal/audit`, `internal/metrics`: cross-cutting audit and Prometheus behavior.
- `pkg/binlog`, `pkg/crypto`, `pkg/logger`, `pkg/storage`: shared technical packages.
- `web`: Vue 3 UI. Follow existing Vue and Arco Design Vue patterns.
- `docs`, `plans`, `k8s`, `docker`, `etc`: keep docs, deploy assets, and config examples aligned with behavior changes.

## Domain Rules

- Keep `TaskStatus` and `SyncPhase` separate. `TaskStatus` is the external lifecycle state; `SyncPhase` decides whether full sync must run or hand off to incremental sync.
- Keep historical full-sync progress fields in task archives at `context.full_sync_resume` for compatibility only. Do not move them to Redis. Redis/memory checkpoints are for incremental binlog positions.
- Do not resume full sync after interruption. If `sync_phase` is `FULL_STARTED` or `FULL_FAILED` and `enable_drop_table_before_ddl=false`, starting `FULL` or `ALL` must be rejected. Exception: `full_load_engine=v2` with persisted `full_load_v2_states` allows resume — already-PUBLISHED tables are skipped and incomplete tables restart with a fresh snapshot.
- Clear historical full-sync progress before any new full sync. With `enable_drop_table_before_ddl=true`, the target table is rebuilt and a fresh full sync can run.
- Keep full-sync read paths distinct: `keyset`, `range`, `sample`, and `nopk` still choose different read strategies, but none of them performs current full-sync resume.
- Preserve the short `FLUSH TABLES WITH READ LOCK` binlog-position capture before full sync. Do not reintroduce long global snapshot behavior or `enable_consistent_snapshot`.
- Incremental sync requires MySQL binlog ROW format. Treat `binlog_row_image=FULL` as essential for safe no-primary-key incremental handling.
- For no-primary-key tables, use `FullColumnsStrategy` and before-image data for UPDATE/DELETE matching. Honor `enable_limit_one` behavior in SQL generation and tests.
- Full-sync bulk writes use plain `INSERT`; users must guarantee the target table is empty or enable `enable_drop_table_before_ddl`. Incremental PK/UK inserts need upsert semantics; no-PK inserts cannot rely on duplicate-key upsert.
- Preserve task-level runtime isolation. Each running task owns its source DB, target DB, analyzer, read-only manager, and cancel function.
- Do not leak task database passwords. Storage encryption uses AES-GCM via `pkg/crypto`; serialize encrypted values without permanently mutating the in-memory plaintext task object.

## Change Guidelines

- Put entity and state-machine behavior under `domain/entity`.
- Put strategy decisions under `domain/service` or `domain/strategy`.
- Put DB, file, SQL, network, and external system details under `infrastructure`.
- Put cross-domain orchestration under `application`.
- Keep API JSON field names stable unless the user asks for a breaking change.
- When API fields or defaults change, update handlers, tests, docs, config examples, and the web UI if applicable.
- Keep SQL generation deterministic and covered by tests. Preserve schema-qualified table references where existing code uses them.
- For concurrency or lifecycle changes, inspect `TaskService`, runtime maps, cancellation, scheduler behavior, and shutdown cleanup together.
- For storage changes, check both file and MySQL task storage paths, including pagination/sorting and encryption compatibility.
- For config changes, update TOML examples, env override handling, validation, docs, and deployment templates if relevant.

## Validation

Use focused tests while iterating, then run broader checks based on the blast radius.

Common backend checks:

```bash
go test ./...
go vet ./...
```

Targeted examples:

```bash
go test ./internal/task/...
go test ./internal/sync/...
go test ./internal/metadata/...
go test -run TestSQLBuilder ./internal/sync/...
```

Frontend check when `web/` changes:

```bash
cd web
npm run build
```

Test expectations:

- Use table-driven tests for strategy, SQL builder, config validation, and state-machine behavior.
- Use `go-sqlmock` for database behavior.
- Use `miniredis` when Redis persistence matters.
- Add or update resume tests when touching `SyncPhase`, `FullSyncResume`, cursor serialization, or range sharding.
- Add concurrency tests when touching task runtime isolation, scheduler behavior, or cancellation.

Before finishing, verify docs match API behavior, full-sync and incremental checkpoints have not been conflated, no-PK correctness is preserved, and destructive behavior such as DROP TABLE is explicit.
