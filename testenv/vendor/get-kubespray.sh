#!/usr/bin/env bash
# Idempotent: shallow-clone kubespray v2.31.0 into testenv/vendor/kubespray
# and build its venv. Exits early when both already exist.
#
# Interpreter / runtime fallback ladder (if python3.11 disappears or the venv
# refuses to build):
#   1. ~/.local/bin/python3.11 (present on this host)
#   2. python3.11 on PATH
#   3. dnf install python3.12 and swap the interpreter below (kubespray
#      v2.31 needs python >= 3.11 for its requirements.txt / ansible-core 2.20)
#   4. Last resort: skip the venv entirely and run kubespray from its
#      container image:
#        podman run --rm -it -v "$PWD:/testenv" -w /testenv \
#          quay.io/kubespray/kubespray:v2.31.0 \
#          ansible-playbook -i inventory/hosts.yml vendor/kubespray/cluster.yml
set -euo pipefail

testenv_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ks_dir="$testenv_dir/vendor/kubespray"

py="${HOME}/.local/bin/python3.11"
[ -x "$py" ] || py="$(command -v python3.11 || true)"
[ -n "${py:-}" ] || { echo "ERROR: no python3.11 found; see the fallback ladder in this script." >&2; exit 1; }

if [ -d "$ks_dir/.git" ] && [ -x "$ks_dir/venv/bin/ansible-playbook" ]; then
  echo "kubespray + venv already provisioned at $ks_dir"
  exit 0
fi

if [ -d "$ks_dir/.git" ]; then
  echo "kubespray already cloned at $ks_dir"
else
  git clone --branch v2.31.0 --depth 1 https://github.com/kubernetes-sigs/kubespray.git "$ks_dir"
fi

if [ ! -x "$ks_dir/venv/bin/ansible-playbook" ]; then
  "$py" -m venv "$ks_dir/venv"
fi

"$ks_dir/venv/bin/pip" install -r "$ks_dir/requirements.txt"

echo "kubespray venv ready: $ks_dir/venv"
