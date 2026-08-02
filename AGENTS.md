# Repository Instructions

These instructions apply to the entire repository. Read this file before making code, test, documentation, deployment, or UI changes.

## First Steps

- Check the working tree first with `git status --short`.
- Search with `rg` / `rg --files`; exclude `node_modules/` unless the task explicitly targets vendored frontend dependencies.
- Read the smallest relevant source files before editing. Do not rely only on documentation.
- Preserve user changes. Do not revert unrelated modified files.

## Semantic Code Discovery and MCP Safety

These rules are client-independent. They apply to Codex, Cursor, Trae, VS Code
agents, CLI agents, and any other host that can read `AGENTS.md`.

Do not depend on a client-specific MCP server id, wrapper function, or UI label.
Discover the available server/tool names in the current client, then map them by
capability:

- **Serena**: LSP-backed symbol lookup, references, diagnostics, and file
  structure.
- **Codebase Memory**: knowledge-graph search, architecture, call chains, and
  cross-module impact analysis.

If an MCP server is not exposed as a native tool, its standalone CLI may be
used only after the workspace safety gate below succeeds. Do not report a
server as unavailable merely because one client uses a different tool prefix.

### Mandatory Workspace Safety Gate

Run this check before activating Serena, starting either MCP server, refreshing
an index, or calling any operation that accepts a repository path:

```powershell
$expectedRepo = [System.IO.Path]::GetFullPath(
  "D:\Epan\BaiduNetdiskDownload\go\mysql_to_asyns"
).TrimEnd("\")
$actualRepo = [System.IO.Path]::GetFullPath(
  (git rev-parse --show-toplevel).Trim()
).TrimEnd("\")

if ($actualRepo -ine $expectedRepo) {
  throw "Wrong repository root. Expected '$expectedRepo', got '$actualRepo'."
}
if (-not (Test-Path -LiteralPath (Join-Path $actualRepo ".git"))) {
  throw "The validated path is not the mysql_to_asyns Git repository."
}
```

Safety requirements:

- The only valid project root for these instructions is
  `D:\Epan\BaiduNetdiskDownload\go\mysql_to_asyns`.
- Both MCP processes must use that exact directory as their working directory.
- `CBM_ALLOWED_ROOT` must resolve to that exact directory. Never set it to
  `C:\`, `C:\Users`, a user profile, a drive root, the parent `go` directory,
  or another broad workspace.
- Treat unresolved values such as `${workspaceFolder}`, `${cwd}`, an empty
  string, or `.` as unsafe. Resolve and compare the final absolute path first.
- Never infer an index path from the MCP process's inherited current directory.
- Never run `index_repository` without an explicit validated `repo_path` and
  the explicit project name `mysql_to_asyns`.
- Never index a sibling repository under these instructions.
- If path validation fails or the active project cannot be verified, stop MCP
  work. Do not try a broader parent directory and do not fall back to `C:\`.

Recommended Codebase Memory process limits for this repository:

```text
CBM_ALLOWED_ROOT=D:/Epan/BaiduNetdiskDownload/go/mysql_to_asyns
CBM_MEM_BUDGET_MB=2048
CBM_WORKERS=2
CBM_LOG_LEVEL=warn
```

These limits are a secondary guard only. Correct root validation is mandatory.

### Startup and Project Verification

For Serena:

1. Start it with the validated repository root as its working directory.
2. Use `--project-from-cwd`, or activate the absolute validated repository path
   when the client requires an explicit activation call.
3. Confirm that the active project is `mysql_to_asyns` and that its root equals
   the validated repository root before any broad search.
4. Use project-relative paths in subsequent Serena calls.

For Codebase Memory:

1. Start it with the validated repository root and the exact
   `CBM_ALLOWED_ROOT` above.
2. Call `list_projects` or `index_status` before assuming an index is missing.
3. Every graph query must include `project: "mysql_to_asyns"`.
4. Confirm that the selected project's canonical/worktree root resolves to the
   validated repository root.
5. Do not automatically create or rebuild an index. Prefer `index_status` and
   `detect_changes`; indexing is an explicit maintenance operation.

When indexing is explicitly required, use the standalone Codebase Memory CLI
after stopping or reloading any client process that holds the graph database:

```powershell
$repo = "D:\Epan\BaiduNetdiskDownload\go\mysql_to_asyns"
$cbm = "$env:USERPROFILE\.local\bin\codebase-memory-mcp.exe"
$validatedRepo = [System.IO.Path]::GetFullPath($repo).TrimEnd("\")
$gitRepo = [System.IO.Path]::GetFullPath(
  (git -C $repo rev-parse --show-toplevel).Trim()
).TrimEnd("\")

if ($gitRepo -ine $validatedRepo) {
  throw "Refusing to index an unexpected path."
}

& $cbm cli index_repository `
  --repo-path $validatedRepo `
  --name mysql_to_asyns `
  --mode full `
  --persistence true
```

Do not use the old Chinese path, a `C:\temp` fallback, or a generic
`workspaceFolder` placeholder. The repository path is ASCII now, so a junction
is not required.

### Tool Selection Priority

1. **Serena** for exact symbols, signatures, references, diagnostics, and file
   structure.
2. **Codebase Memory** for architecture, callers/callees, dependency paths, and
   cross-module impact.
3. **`rg` / `rg --files`** for string literals, error messages, configuration,
   documentation, and cases not answered by semantic tools.

Use capabilities rather than client-specific invocation syntax:

| Need | Preferred capability |
|------|----------------------|
| File symbol overview | Serena `get_symbols_overview` |
| Find a symbol or signature | Serena `find_symbol` |
| Find exact references | Serena `find_referencing_symbols` |
| File diagnostics | Serena diagnostics tool exposed by the client |
| Semantic source pattern search | Serena `search_for_pattern` |
| Find graph entities | Codebase Memory `search_graph` |
| Trace inbound/outbound calls | Codebase Memory `trace_path` |
| Read graph-located source | Codebase Memory `get_code_snippet` |
| Architecture overview | Codebase Memory `get_architecture` |
| Complex dependency query | Codebase Memory `query_graph` |

Example argument payloads are portable even when the outer invocation differs:

```json
{"project":"mysql_to_asyns","query":"full load engine","limit":20}
{"project":"mysql_to_asyns","function_name":"executeFullSync","direction":"both","depth":3}
{"project":"mysql_to_asyns","qualified_name":"mysql_to_asyns.internal.sync.fullload.Run"}
```

For Serena, pass project-relative paths such as
`internal/sync/fullload/engine.go`; do not pass paths outside the validated
repository.

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
- Preserve the unlocked `SHOW MASTER STATUS` binlog-position capture (P0) before full sync. After the baseline scan, capture P1 and run a bounded catch-up from P0 to P1, then restore non-identity indexes. Do not reintroduce `FLUSH TABLES WITH READ LOCK`, long global snapshots, table-level long-lived RR snapshots, or `enable_consistent_snapshot`.
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
