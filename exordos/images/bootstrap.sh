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
PERSISTENT_POSTGRESQL_DIR="${PERSISTENT_MOUNT}/var/lib/postgresql"
PERSISTENT_RUNTIME_DIR="${PERSISTENT_MOUNT}/var/lib/workspace_zulip_bridge"

PERSISTENT_DISK=""
for _ in {1..300}; do
    if PERSISTENT_DISK=$(find_persistent_disk); then
        break
    fi
    sleep 1
done
if [[ -z "$PERSISTENT_DISK" ]]; then
    echo "workspace-zulip-bridge requires a persistent data disk" >&2
    exit 1
fi

prepare_persistent_disk "$PERSISTENT_DISK" "$PERSISTENT_MOUNT"

postgres_cluster_valid() {
    local data_root="$1"
    local cluster_dir="${data_root}/${PG_VERSION}/main"

    [[ "$(cat "${cluster_dir}/PG_VERSION" 2>/dev/null || true)" == "$PG_VERSION" ]] \
        && [[ -s "${cluster_dir}/global/pg_control" ]] \
        && [[ -d "${cluster_dir}/base" ]]
}

rebuild_image_postgres_cluster() {
    systemctl stop "postgresql@${PG_VERSION}-main" 2>/dev/null || true
    pg_dropcluster --stop "$PG_VERSION" main 2>/dev/null || true
    rm -rf -- "/var/lib/postgresql/${PG_VERSION}/main"
    pg_createcluster "$PG_VERSION" main --start-conf=auto
}

# The marker is written after both migrations complete.  A reset in that small
# window has two valid recovery states: the persistent cluster may already be
# complete, or it may be partial.  Never delete the only valid cluster merely
# because the marker is missing.  Rebuild from the image only after validating
# the destination and, if necessary, detaching an interrupted bind mount.
if [[ ! -f "$PERSIST_MIGRATE_MARKER" ]] \
    && ! postgres_cluster_valid "$PERSISTENT_POSTGRESQL_DIR"; then
    systemctl stop "postgresql@${PG_VERSION}-main" 2>/dev/null || true
    if mountpoint -q "/var/lib/postgresql"; then
        umount "/var/lib/postgresql"
    fi
    rm -rf -- "$PERSISTENT_POSTGRESQL_DIR"
    if ! postgres_cluster_valid "/var/lib/postgresql"; then
        rebuild_image_postgres_cluster
    fi
fi

migrate_to_persistent_stop_start \
    "/var/lib/postgresql" \
    "$PERSISTENT_POSTGRESQL_DIR" \
    "postgresql@${PG_VERSION}-main"
install -d -o "$DATABASE_ROLE" -g "$DATABASE_ROLE" -m 0700 \
    "/var/lib/workspace_zulip_bridge"
migrate_to_persistent \
    "/var/lib/workspace_zulip_bridge" \
    "$PERSISTENT_RUNTIME_DIR" \
    "$DATABASE_ROLE" \
    "$DATABASE_ROLE"
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
