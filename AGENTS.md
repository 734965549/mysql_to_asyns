# Repository Instructions

These instructions apply to the entire repository. Read this file before making code, test, documentation, deployment, or UI changes.

## First Steps

- Check the working tree first with `git status --short`.
- Search with `rg` / `rg --files`; exclude `node_modules/` unless the task explicitly targets vendored frontend dependencies.
- Read the smallest relevant source files before editing. Do not rely only on documentation.
- Preserve user changes. Do not revert unrelated modified files.

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

- Keep `TaskStatus` and `SyncPhase` separate. `TaskStatus` is the external lifecycle state; `SyncPhase` decides whether full sync must run, resume, or hand off to incremental sync.
- Store full-sync resume state in task archives at `context.full_sync_resume`. Do not move it to Redis. Redis/memory checkpoints are for incremental binlog positions.
- Resume full sync only when `sync_phase` is `FULL_STARTED` or `FULL_FAILED`, `enable_drop_table_before_ddl=false`, and `full_sync_resume` still exists.
- Disable or clear full-sync resume when `enable_drop_table_before_ddl=true`, because the target table may be rebuilt.
- Advance full-sync row cursors only after the write transaction commits.
- Keep full-sync read paths distinct: `keyset` and `range` support row-level resume; `sample` and `nopk` support table-level resume only.
- Preserve the short `FLUSH TABLES WITH READ LOCK` binlog-position capture before full sync. Do not reintroduce long global snapshot behavior or `enable_consistent_snapshot`.
- Incremental sync requires MySQL binlog ROW format. Treat `binlog_row_image=FULL` as essential for safe no-primary-key incremental handling.
- For no-primary-key tables, use `FullColumnsStrategy` and before-image data for UPDATE/DELETE matching. Honor `enable_limit_one` behavior in SQL generation and tests.
- Keep full-sync writes idempotent. Full-sync bulk writes use `INSERT IGNORE`; incremental PK/UK inserts need upsert semantics; no-PK inserts cannot rely on duplicate-key upsert.
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
