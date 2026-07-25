# Suggested commands
- Working tree: `git status --short --branch`
- File/string discovery: `rg --files`; `rg <pattern> <path>` (exclude `node_modules/`). Prefer Serena/Codebase Memory for code symbols/call paths.
- Backend run: `go run .`
- Focused tests: `go test ./internal/task/... -count=1`; `go test ./internal/sync/... -count=1`; `go test ./internal/metadata/... -count=1`
- All backend tests: `go test ./...`
- Static checks: `go vet ./...`
- Format changed Go files: `gofmt -w <files>`
- Frontend: `Set-Location web; npm run dev`; tests `npm test`; production build `npm run build`
- PowerShell equivalents: list hidden `Get-ChildItem -Force`; recursive files `Get-ChildItem -Recurse`; inspect env `Test-Path Env:NAME`.