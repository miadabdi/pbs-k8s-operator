#!/usr/bin/env bash
# Idempotent: generate the shared testenv SSH keypair (used by both the PBS
# playbook and kubespray) only if absent. Everything lands under
# testenv/secrets/ which is gitignored.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
secrets_dir="$repo_root/testenv/secrets"
key="$secrets_dir/testenv_ed25519"

mkdir -p "$secrets_dir"
chmod 0700 "$secrets_dir"

if [ ! -f "$key" ]; then
  ssh-keygen -t ed25519 -N '' -f "$key" -C testenv
  chmod 0600 "$key"
fi

echo "OK: $key.pub"
