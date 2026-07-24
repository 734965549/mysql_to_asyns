# Repository Instructions

These instructions apply to the entire repository. Read this file before making code, test, documentation, deployment, or UI changes.

## First Steps

- Check the working tree first with `git status --short`.
- Search with `rg` / `rg --files`; exclude `node_modules/` unless the task explicitly targets vendored frontend dependencies.
- Read the smallest relevant source files before editing. Do not rely only on documentation.
- Preserve user changes. Do not revert unrelated modified files.

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

#### Query tools (use via MCP)

| Question | Tool |
|----------|------|
| Find functions/classes by keyword | `search_graph` with `query` or `name_pattern` |
| Text search enriched with graph context | `search_code` |
| Who calls X / what does X call | `trace_path` (`direction`: inbound/outbound/both) |
| Complex graph patterns | `query_graph` (Cypher) |
| Read source for a symbol | `get_code_snippet` (get `qualified_name` from `search_graph` first) |
| Package/module overview | `get_architecture` |
| Node/edge types | `get_graph_schema` |

Examples:

```json
{"project": "mysql_to_asyns", "query": "full load engine", "limit": 20}
{"project": "aiops", "query": "runbook execution", "limit": 20}
{"project": "lianghua", "name_pattern": ".*Order.*", "label": "Function"}
{"project": "bindip", "query": "binding state", "limit": 10}
{"project": "goInception", "query": "sql audit execute", "limit": 20}
{"project": "archery", "query": "sql workflow review", "limit": 20}
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
- Do not resume full sync after interruption. If `sync_phase` is `FULL_STARTED` or `FULL_FAILED` and `enable_drop_table_before_ddl=false`, starting `FULL` or `ALL` must be rejected.
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
