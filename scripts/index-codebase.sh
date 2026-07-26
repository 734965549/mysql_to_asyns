#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
PROJECT_NAME="${CBM_PROJECT_NAME:-mysql_to_asyns}"
PERSISTENCE="${CBM_PERSISTENCE:-true}"

case "$MODE" in
  fast|moderate|full) ;;
  *)
    echo "usage: $0 [fast|moderate|full]" >&2
    exit 2
    ;;
esac

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

find_cbm() {
  if command -v codebase-memory-mcp >/dev/null 2>&1; then
    command -v codebase-memory-mcp
    return
  fi
  if [[ -x "${HOME}/.local/bin/codebase-memory-mcp" ]]; then
    echo "${HOME}/.local/bin/codebase-memory-mcp"
    return
  fi
  echo "codebase-memory-mcp not found" >&2
  exit 1
}

CBM="$(find_cbm)"
export CBM_LOG_LEVEL="${CBM_LOG_LEVEL:-info}"

echo "Indexing: ${ROOT}"
echo "Mode: ${MODE}  Project: ${PROJECT_NAME}  Persistence: ${PERSISTENCE}"

"${CBM}" cli index_repository \
  --repo-path "${ROOT}" \
  --mode "${MODE}" \
  --name "${PROJECT_NAME}" \
  --persistence "${PERSISTENCE}"

echo
echo "Index complete."
echo "Restart your editor MCP session if graph queries look stale."
