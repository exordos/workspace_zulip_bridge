#!/usr/bin/env bash

set -eu

if [ ! -s /etc/workspace-zulip-bridge/bridge.conf ]; then
    exit 0
fi
/usr/local/bin/workspace-zulip-bridge-bootstrap
systemctl try-restart workspace-zulip-bridge.service || true
