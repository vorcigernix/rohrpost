#!/bin/sh
set -eu

WORKSPACE_DIR="${WORKSPACE_DIR:-/workspace}"

if [ ! -d "$WORKSPACE_DIR/node_modules" ]; then
  cd "$WORKSPACE_DIR"
  bun install
fi

exec "$@"
