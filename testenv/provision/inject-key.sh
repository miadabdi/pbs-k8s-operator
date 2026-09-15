#!/bin/sh
# Idempotent: append the shared testenv pubkey to the vagrant user's
# authorized_keys. Run by the Vagrant shell provisioner on every VM
# (as root); the key must already exist on the host (scripts/gen-keys.sh).
set -eu

pubkey_file=/vagrant/secrets/testenv_ed25519.pub
ssh_dir=/home/vagrant/.ssh
auth_keys=$ssh_dir/authorized_keys

if [ ! -f "$pubkey_file" ]; then
  echo "ERROR: $pubkey_file missing; run testenv/scripts/gen-keys.sh on the host first" >&2
  exit 1
fi

pubkey="$(cat "$pubkey_file")"

mkdir -p "$ssh_dir"
chmod 0700 "$ssh_dir"
touch "$auth_keys"
chmod 0600 "$auth_keys"
grep -qxF "$pubkey" "$auth_keys" || echo "$pubkey" >>"$auth_keys"
chown -R vagrant:vagrant "$ssh_dir"

echo "inject-key: OK"
