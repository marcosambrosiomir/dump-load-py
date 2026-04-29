#!/usr/bin/env sh

set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PROJECT_DIR"

if ! command -v podman >/dev/null 2>&1; then
  echo "podman não encontrado" >&2
  exit 1
fi

CONTAINER_NAME="dump-viewer"

if podman ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  podman rm -f "$CONTAINER_NAME" >/dev/null
fi

if command -v sudo >/dev/null 2>&1; then
  if sudo -n env -i HOME=/root PATH=/usr/bin:/bin:/usr/sbin:/sbin podman ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
    sudo -n env -i HOME=/root PATH=/usr/bin:/bin:/usr/sbin:/sbin podman rm -f "$CONTAINER_NAME" >/dev/null
  fi
fi

echo "Container parado."