#Requires -Version 5.1
<#
.SYNOPSIS
  Create or refresh an ASCII-only junction for codebase-memory indexing on Windows.

.DESCRIPTION
  codebase-memory-mcp worker crashes when repo_path contains non-ASCII characters
  (e.g. paths under "E盘"). Indexing must use an ASCII junction:
  C:\temp\mysql_to_asyns -> <actual repo root>.
#>
param(
    [string]$JunctionPath = "C:\temp\mysql_to_asyns"
)

$ErrorActionPreference = "Stop"

function Get-RepoRoot {
    $here = Split-Path -Parent $PSScriptRoot
    if (Test-Path (Join-Path $here ".git")) {
        return (Resolve-Path $here).Path
    }
    throw "Cannot locate repository root from $PSScriptRoot"
}

function Test-ReparsePointTarget([string]$LinkPath, [string]$ExpectedTarget) {
    if (-not (Test-Path -LiteralPath $LinkPath)) {
        return $false
    }
    $item = Get-Item -LiteralPath $LinkPath -Force
    if (-not ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        return $false
    }
    $actual = (Resolve-Path -LiteralPath $LinkPath).Path
    $expected = (Resolve-Path -LiteralPath $ExpectedTarget).Path
    return ($actual -eq $expected)
}

$repoRoot = Get-RepoRoot
$parent = Split-Path -Parent $JunctionPath
if (-not (Test-Path -LiteralPath $parent)) {
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
}

if (Test-ReparsePointTarget -LinkPath $JunctionPath -ExpectedTarget $repoRoot) {
    Write-Host "Junction OK: $JunctionPath -> $repoRoot"
    return $JunctionPath
}

if (Test-Path -LiteralPath $JunctionPath) {
    $item = Get-Item -LiteralPath $JunctionPath -Force
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
        cmd /c rmdir "$JunctionPath" 2>$null
    } else {
        throw "Path exists and is not a junction: $JunctionPath"
    }
}

New-Item -ItemType Junction -Path $JunctionPath -Target $repoRoot | Out-Null
Write-Host "Created junction: $JunctionPath -> $repoRoot"
return $JunctionPath
