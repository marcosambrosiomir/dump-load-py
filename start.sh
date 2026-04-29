#!/usr/bin/env sh

set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PROJECT_DIR"

APP_HOST_IP=${APP_HOST_IP:-10.0.211.102}
APP_HOST_PORT=${APP_HOST_PORT:-8001}
CONFIG_FILE=${CONFIG_FILE:-$PROJECT_DIR/config/config.yaml}
CONFIG_DIR=$(dirname "$CONFIG_FILE")
HOST_ROOT_MOUNT=${HOST_ROOT_MOUNT:-/hostfs}
DB_ROOT=${DB_ROOT:-/totvs/database/prod}
DLC_PATH=${DLC_PATH:-/totvs/dba/progress/dlc12}

if [ "$(id -u)" -ne 0 ]; then
  exec sudo -n env -i \
    HOME=/root \
    PATH=/usr/bin:/bin:/usr/sbin:/sbin \
    APP_HOST_IP="$APP_HOST_IP" \
    APP_HOST_PORT="$APP_HOST_PORT" \
    CONFIG_FILE="$CONFIG_FILE" \
    HOST_ROOT_MOUNT="$HOST_ROOT_MOUNT" \
    DB_ROOT="$DB_ROOT" \
    DLC_PATH="$DLC_PATH" \
    "$0" "$@"
fi

export APP_HOST_IP

if ! command -v podman >/dev/null 2>&1; then
  echo "podman não encontrado" >&2
  exit 1
fi

IMAGE_NAME="dump-viewer"
CONTAINER_NAME="dump-viewer"

podman build -t "$IMAGE_NAME" .

if podman ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  podman rm -f "$CONTAINER_NAME" >/dev/null
fi

if [ -d "$DLC_PATH" ]; then
  DLC_MOUNT="-v $DLC_PATH:$DLC_PATH:ro"
else
  echo "Aviso: DLC_PATH inexistente em $DLC_PATH; iniciando sem este mount." >&2
  DLC_MOUNT=""
fi

DB_ROOT_MOUNT=""
case "$DB_ROOT" in
  /totvs/database|/totvs/database/*)
    # O diretório /totvs/database já é montado por completo abaixo.
    # Evita mount redundante e warning desnecessário para subpaths opcionais.
    ;;
  *)
    if [ -d "$DB_ROOT" ]; then
      DB_ROOT_MOUNT="-v $DB_ROOT:/totvs/database/prod:ro"
    else
      echo "Aviso: DB_ROOT inexistente em $DB_ROOT; iniciando sem este mount." >&2
    fi
    ;;
esac

TEMP_DIR="/totvs/temp"
if [ -d "$TEMP_DIR" ]; then
  TEMP_DIR_MOUNT="-v $TEMP_DIR:$TEMP_DIR:rw"
else
  echo "Aviso: TEMP_DIR inexistente em $TEMP_DIR; iniciando sem este mount." >&2
  TEMP_DIR_MOUNT=""
fi

podman run -d --ipc=host --pid=host --uidmap 0:0:4294967295 --gidmap 0:0:4294967295 --name "$CONTAINER_NAME" -p "${APP_HOST_IP}:${APP_HOST_PORT}:8000" -v "${CONFIG_DIR}:/app/config:rw" -v "/:${HOST_ROOT_MOUNT}:rw" -v /totvs/database:/totvs/database:rw ${DLC_MOUNT} ${DB_ROOT_MOUNT} ${TEMP_DIR_MOUNT} "$IMAGE_NAME"

echo "Container iniciado. Acesse: http://${APP_HOST_IP}:${APP_HOST_PORT}"