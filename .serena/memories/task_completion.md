# Completion checklist
- Run `gofmt` on changed Go files; use `git diff --check`.
- Run focused package tests while iterating, then `go test ./...` and `go vet ./...` when blast radius warrants.
- If `web/` changed: from `web`, run `npm test` when relevant and `npm run build`.
- Add table-driven tests for strategy/SQL/config/state-machine logic; go-sqlmock for DB behavior; concurrency tests for runtime/scheduler/cancellation changes.
- Touching SyncPhase, FullSyncResume, cursors, or range sharding requires resume tests.
- Confirm docs/config/UI match changed API/defaults; full vs incremental checkpoints remain separate; no-PK before-image correctness remains intact; destructive behavior is explicit.
- Final `git status --short --branch`; ensure unrelated dirty/untracked files are not staged. Optional memory reference audit: `serena memories check`.