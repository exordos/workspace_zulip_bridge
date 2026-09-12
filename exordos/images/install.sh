#!/usr/bin/env bash

# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

set -euo pipefail

APP_DIR="/opt/workspace_zulip_bridge"
CONFIG_DIR="/etc/workspace_zulip_bridge"
BOOTSTRAP_DIR="/var/lib/exordos/bootstrap/scripts"
SYSTEMD_DIR="/etc/systemd/system"
SERVICE_USER="workspace_zulip_bridge"
PG_VERSION="18"

sudo apt-get update
sudo apt-get install -y postgresql-common python3
sudo YES=1 /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
sudo apt-get update
sudo apt-get install -y "postgresql-${PG_VERSION}"
sudo systemctl disable --now postgresql

if ! getent passwd "$SERVICE_USER" >/dev/null; then
    sudo useradd \
        --system \
        --home-dir "/var/lib/${SERVICE_USER}" \
        --create-home \
        --shell /usr/sbin/nologin \
        "$SERVICE_USER"
fi

cd "$APP_DIR"
uv sync --locked --no-dev

sudo install -d -o root -g "$SERVICE_USER" -m 0750 "$CONFIG_DIR"
if [[ ! -f "$CONFIG_DIR/bridge.env" ]]; then
    sudo install \
        -o root \
        -g "$SERVICE_USER" \
        -m 0640 \
        "$APP_DIR/etc/workspace-zulip-bridge.env.example" \
        "$CONFIG_DIR/bridge.env"
fi

sudo install \
    -o root \
    -g root \
    -m 0644 \
    "$APP_DIR/etc/systemd/workspace-zulip-bridge.service" \
    "$SYSTEMD_DIR/workspace-zulip-bridge.service"
sudo install \
    -o root \
    -g root \
    -m 0755 \
    "$APP_DIR/exordos/images/bootstrap.sh" \
    "$BOOTSTRAP_DIR/0100-workspace-zulip-bridge.sh"
sudo systemctl daemon-reload
