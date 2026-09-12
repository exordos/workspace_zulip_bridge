#!/usr/bin/env bash

# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

set -euo pipefail

# shellcheck disable=SC1091
source /usr/local/lib/exordos/lib_bootstrap.sh

PG_VERSION="18"
SERVICE_NAME="workspace-zulip-bridge"
DATABASE_NAME="workspace_zulip_bridge"
DATABASE_ROLE="workspace_zulip_bridge"

PERSISTENT_DISK=$(find_persistent_disk)
if [[ -z "$PERSISTENT_DISK" ]]; then
    echo "workspace-zulip-bridge requires a persistent data disk" >&2
    exit 1
fi

prepare_persistent_disk "$PERSISTENT_DISK" "$PERSISTENT_MOUNT"
migrate_to_persistent_stop_start \
    "/var/lib/postgresql" \
    "${PERSISTENT_MOUNT}/var/lib/postgresql" \
    "postgresql@${PG_VERSION}-main"
persist_migrate_complete

sudo systemctl enable --now postgresql

if ! sudo -u postgres psql -tAc \
    "SELECT 1 FROM pg_roles WHERE rolname = '${DATABASE_ROLE}'" | grep -qx 1; then
    sudo -u postgres createuser --no-createdb --no-createrole --no-superuser \
        "$DATABASE_ROLE"
fi

if ! sudo -u postgres psql -tAc \
    "SELECT 1 FROM pg_database WHERE datname = '${DATABASE_NAME}'" | grep -qx 1; then
    sudo -u postgres createdb --owner "$DATABASE_ROLE" "$DATABASE_NAME"
fi

sudo systemctl enable --now "$SERVICE_NAME"
