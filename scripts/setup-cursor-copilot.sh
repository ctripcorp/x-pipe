#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COPILOT="$ROOT/xpipe-copilot"
CURSOR="$ROOT/.cursor"

if [[ ! -d "$COPILOT/rules" ]]; then
  echo "error: xpipe-copilot submodule not initialized. Run: git submodule update --init --recursive" >&2
  exit 1
fi

mkdir -p "$CURSOR"

link_dir() {
  local name="$1"
  local target="../xpipe-copilot/$name"
  local link="$CURSOR/$name"

  if [[ -e "$link" && ! -L "$link" ]]; then
    echo "error: $link exists and is not a symlink; move it aside and retry" >&2
    exit 1
  fi

  ln -sfn "$target" "$link"
  echo "linked $link -> $target"
}

for dir in agents changes knowledge rules; do
  link_dir "$dir"
done

echo "done: Cursor will load rules from .cursor/rules/ (symlinked to xpipe-copilot/rules/)"
