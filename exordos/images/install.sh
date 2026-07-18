#!/usr/bin/env bash

set -eu
set -o pipefail
set -x

SOURCE=/opt/workspace-zulip-bridge
VENV=/opt/workspace-zulip-bridge-venv

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
    openssh-server \
    postgresql \
    python3 \
    python3-pip \
    python3-venv
sudo systemctl enable ssh.service

if ! getent group workspace-zulip >/dev/null; then
    sudo groupadd --system workspace-zulip
fi
if ! getent passwd workspace-zulip >/dev/null; then
    sudo useradd --system --gid workspace-zulip \
        --home-dir /var/lib/workspace-zulip-bridge \
        --shell /usr/sbin/nologin workspace-zulip
fi

sudo python3 -m venv "$VENV"
sudo "$VENV/bin/python" -m pip install "$SOURCE"
sudo install -d -m 0750 -o workspace-zulip -g workspace-zulip \
    /var/lib/workspace-zulip-bridge
sudo install -d -m 0755 -o workspace-zulip -g workspace-zulip \
    /run/workspace-zulip-bridge
sudo install -d -m 0750 -o root -g workspace-zulip \
    /etc/workspace-zulip-bridge /etc/workspace-zulip-bridge/secrets
sudo install -d -m 0755 /usr/local/bin /usr/local/lib/exordos
sudo install -d -m 0755 /usr/local/lib/workspace-zulip-bridge
sudo install -m 0755 "$SOURCE/exordos/images/bootstrap.sh" \
    /usr/local/bin/workspace-zulip-bridge-bootstrap
sudo install -m 0644 "$SOURCE/exordos/images/bootstrap-persistence.sh" \
    /usr/local/lib/workspace-zulip-bridge/bootstrap-persistence.sh
sudo install -m 0755 "$SOURCE/exordos/images/restart.sh" \
    /usr/local/bin/workspace-zulip-bridge-restart
sudo systemctl disable --now postgresql.service || true
