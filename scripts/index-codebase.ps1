#Requires -Version 5.1
<#
.SYNOPSIS
  Build or refresh the codebase-memory knowledge graph for this repository.

.NOTES
  Do NOT use the MCP index_repository tool while Cursor is connected — the MCP
  server holds the SQLite DB open and the worker exits immediately. This script
  runs the standalone CLI instead.

  On Windows, repo paths with non-ASCII characters must go through the ASCII
  junction created by ensure-cbm-junction.ps1.
#>
param(
    [ValidateSet("fast", "moderate", "full")]
    [string]$Mode = "full",
    [switch]$NoPersistence,
    [string]$ProjectName = "mysql_to_asyns"
)

$ErrorActionPreference = "Stop"

function Find-CbmExecutable {
    $candidates = @(
        "$env:USERPROFILE\.local\bin\codebase-memory-mcp.exe",
        "$env:USERPROFILE\.local\bin\codebase-memory-mcp",
        "codebase-memory-mcp.exe",
        "codebase-memory-mcp"
    )
    foreach ($path in $candidates) {
        if ($path -match '[\\/]' -and (Test-Path -LiteralPath $path)) {
            return (Resolve-Path -LiteralPath $path).Path
        }
        $cmd = Get-Command $path -ErrorAction SilentlyContinue
        if ($cmd) {
            return $cmd.Source
        }
    }
    throw "codebase-memory-mcp not found. Install from https://github.com/DeusData/codebase-memory-mcp"
}

$cbm = Find-CbmExecutable
$junctionScript = Join-Path $PSScriptRoot "ensure-cbm-junction.ps1"
$indexPath = & $junctionScript
$persistence = if ($NoPersistence) { "false" } else { "true" }

$env:CBM_LOG_LEVEL = "info"

Write-Host "Indexing via: $indexPath"
Write-Host "Mode: $Mode  Project: $ProjectName  Persistence: $persistence"

& $cbm cli index_repository `
    --repo-path $indexPath `
    --mode $Mode `
    --name $ProjectName `
    --persistence $persistence

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "Indexing failed. Common causes on Windows:" -ForegroundColor Yellow
    Write-Host "  1. Cursor MCP server holds the graph DB open — reload MCP servers or restart Cursor, then rerun."
    Write-Host "  2. repo_path must use the ASCII junction ($indexPath), not the Unicode workspace path."
    Write-Host "  3. Ensure codebase-memory-mcp is up to date."
    throw "index_repository failed with exit code $LASTEXITCODE"
}

Write-Host ""
Write-Host "Index complete."
Write-Host "Restart Cursor (or reload MCP servers) so the running codebase-memory MCP picks up the new graph."
Write-Host "Artifact: .codebase-memory/graph.db.zst (local cache, gitignored)"
