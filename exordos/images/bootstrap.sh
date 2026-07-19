#!/usr/bin/env bash

set -eu
set -o pipefail

source /usr/local/lib/exordos/lib_bootstrap.sh
source /usr/local/lib/workspace-zulip-bridge/bootstrap-persistence.sh

CONFIG=/etc/workspace-zulip-bridge/bridge.conf
RUN_DIR=/run/workspace-zulip-bridge
SOURCE=/opt/workspace-zulip-bridge
VENV=/opt/workspace-zulip-bridge-venv
DATABASE_ROLE=workspace-zulip
DATABASE_NAME=workspace_zulip_bridge

install -d -m 0755 -o workspace-zulip -g workspace-zulip "$RUN_DIR"
exec 9>"$RUN_DIR/bootstrap.lock"
flock -x 9

if [ ! -s "$CONFIG" ]; then
    echo "Workspace Zulip bridge configuration is not available; deferring."
    exit 0
fi

bridge_prepare_persistent_mount "$PERSISTENT_MOUNT"
bridge_make_persistent_mount_private "$PERSISTENT_MOUNT"

# Exordos runs this entrypoint both as the enabled bootstrap service and as the
# worker's before hook.  A later, serialized invocation must not stop the
# database underneath a worker that has already started.
if bridge_persistence_migration_is_required \
    /var/lib/postgresql \
    "$PERSISTENT_MOUNT/var/lib/postgresql" \
    /var/lib/workspace-zulip-bridge \
    "$PERSISTENT_MOUNT/var/lib/workspace-zulip-bridge"; then
    systemctl stop postgresql.service || true
    bridge_migrate_to_persistent \
        /var/lib/postgresql \
        "$PERSISTENT_MOUNT/var/lib/postgresql"
    bridge_migrate_to_persistent \
        /var/lib/workspace-zulip-bridge \
        "$PERSISTENT_MOUNT/var/lib/workspace-zulip-bridge"
    persist_migrate_complete
fi
chown -R postgres:postgres /var/lib/postgresql
chown -R workspace-zulip:workspace-zulip /var/lib/workspace-zulip-bridge
systemctl start postgresql.service
bridge_wait_for_postgresql

runuser -u workspace-zulip -- "$VENV/bin/workspace-zulip-bridge-enroll" \
    --config "$CONFIG"
if ! runuser -u postgres -- psql -tAc \
    "SELECT 1 FROM pg_roles WHERE rolname='$DATABASE_ROLE'" | grep -qx 1; then
    runuser -u postgres -- createuser "$DATABASE_ROLE"
fi
if ! runuser -u postgres -- psql -tAc \
    "SELECT 1 FROM pg_database WHERE datname='$DATABASE_NAME'" | grep -qx 1; then
    runuser -u postgres -- createdb -O "$DATABASE_ROLE" "$DATABASE_NAME"
fi
runuser -u workspace-zulip -- "$VENV/bin/ra-apply-migration" \
    --config-file "$CONFIG" \
    --path "$SOURCE/migrations"

echo "Workspace Zulip bridge bootstrap completed."
