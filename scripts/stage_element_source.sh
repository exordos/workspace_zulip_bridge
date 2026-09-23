#!/usr/bin/env bash

# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
stage_root="${repo_root}/.build-source"
stage_dir="${stage_root}/workspace_zulip_bridge"

case "${stage_dir}" in
    "${repo_root}/.build-source/workspace_zulip_bridge") ;;
    *)
        echo "refusing to replace unexpected staging path: ${stage_dir}" >&2
        exit 1
        ;;
esac

rm -rf -- "${stage_dir}"
mkdir -p "${stage_dir}"
git -C "${repo_root}" archive --format=tar HEAD | tar -xf - -C "${stage_dir}"
git -C "${repo_root}" rev-parse HEAD > "${stage_dir}/.source-commit"
