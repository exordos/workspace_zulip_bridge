#!/usr/bin/env bash

# Bridge-local idempotency guards for the persistent data bootstrap.  The base
# image helper currently mounts a bind target unconditionally, so callers must
# avoid invoking it after the target is already bound.

bridge_persistent_mount_is_ready() {
    local mount_path="$1"
    local root_device
    local persistent_device

    [[ -d "$mount_path" ]] || return 1
    root_device=$(stat -L -c '%d' -- /) || return 1
    persistent_device=$(stat -L -c '%d' -- "$mount_path") || return 1
    [[ "$root_device" != "$persistent_device" ]]
}

bridge_prepare_persistent_mount() {
    local mount_path="$1"
    local persistent_disk

    if bridge_persistent_mount_is_ready "$mount_path"; then
        echo "Workspace Zulip bridge persistent filesystem is already mounted."
        return 0
    fi

    if ! persistent_disk=$(find_persistent_disk); then
        echo "Workspace Zulip bridge persistent disk is required" >&2
        return 1
    fi
    if [[ -z "$persistent_disk" ]]; then
        echo "Workspace Zulip bridge persistent disk is required" >&2
        return 1
    fi
    prepare_persistent_disk "$persistent_disk" "$mount_path"
}

bridge_make_persistent_mount_private() {
    local mount_path="$1"

    # The base image mounts filesystems as shared.  A bind from a shared
    # persistent subtree can otherwise propagate back into the source path;
    # repeated bootstrap calls then create a growing mount graph even when the
    # data directories themselves are unchanged.
    mount --make-rprivate "$mount_path"
}

bridge_paths_share_identity() {
    local old_data_dir="$1"
    local persistent_dir="$2"
    local old_identity
    local persistent_identity

    [[ -e "$old_data_dir" && -e "$persistent_dir" ]] || return 1
    old_identity=$(stat -L -c '%d:%i' -- "$old_data_dir") || return 1
    persistent_identity=$(stat -L -c '%d:%i' -- "$persistent_dir") || return 1
    [[ "$old_identity" == "$persistent_identity" ]]
}

bridge_migrate_to_persistent() {
    local old_data_dir="$1"
    local persistent_dir="$2"

    if bridge_paths_share_identity "$old_data_dir" "$persistent_dir"; then
        echo "Persistent bind mount for $old_data_dir is already active; skipping."
        return 0
    fi

    migrate_to_persistent "$old_data_dir" "$persistent_dir"
}

bridge_persistence_migration_is_required() {
    local postgres_data_dir="$1"
    local persistent_postgres_dir="$2"
    local bridge_data_dir="$3"
    local persistent_bridge_dir="$4"

    ! bridge_paths_share_identity \
        "$postgres_data_dir" "$persistent_postgres_dir" || \
        ! bridge_paths_share_identity \
            "$bridge_data_dir" "$persistent_bridge_dir"
}

bridge_wait_for_postgresql() {
    local max_attempts="${1:-60}"
    local delay_seconds="${2:-1}"
    local attempt

    for ((attempt = 1; attempt <= max_attempts; attempt++)); do
        if runuser -u postgres -- \
            pg_isready --quiet --dbname postgres; then
            return 0
        fi
        if ((attempt < max_attempts)); then
            sleep "$delay_seconds"
        fi
    done

    echo "PostgreSQL did not become ready after $max_attempts attempts" >&2
    return 1
}
