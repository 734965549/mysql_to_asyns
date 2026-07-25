# Project core
- Go MySQL-to-MySQL synchronization service with Vue management UI.
- Entry: `main.go` wires config, logging, task service, metadata analyzer, Gin routes, graceful shutdown.
- Preserve separation: external `TaskStatus` vs internal `SyncPhase`.
- Full-sync resume is only supported by FullLoad V2 persisted `full_load_v2_states`; historical `context.full_sync_resume` is compatibility-only. Incremental checkpoints belong to Redis/memory binlog position storage.
- Task runtime resources are isolated per running task (source/target DB, analyzer, read-only manager, cancel).
- No-PK incremental UPDATE/DELETE must match before images via `FullColumnsStrategy`; `binlog_row_image=FULL` is required for safety.
- Read backend details in `mem:backend/core`; frontend details in `mem:frontend/core`; toolchain in `mem:tech_stack`; conventions in `mem:conventions`.