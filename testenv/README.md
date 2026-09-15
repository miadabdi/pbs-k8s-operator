# testenv — disposable PBS + k8s environment for pbs-operator

Three VirtualBox VMs on the pre-existing host-only net `vboxnet0`
(192.168.56.1): `pbs` (192.168.56.10, PBS 4.2), `k8s-ctl1` (.11, control
plane), `k8s-node1` (.12, worker). Kubespray v2.31.0 deploys Kubernetes
v1.35.4 with local-path storage. Everything generated (keys, tokens,
kubeconfig, kubespray checkout) is gitignored under `secrets/`, `artifacts/`,
`vendor/`.

## Layout

- `Vagrantfile` — 3 VMs, shared SSH key injected by `provision/inject-key.sh`
- `scripts/gen-keys.sh` — the one shared SSH key (idempotent)
- `provision/pbs.yml` — PBS install + datastore/user/token/ACL/namespace/
  fingerprint/keyfile; renders the K8s Secrets the controller will read
- `vendor/get-kubespray.sh` — shallow kubespray clone + venv (idempotent)
- `inventory/` — kubespray inventory (the `pbs` host sits outside all
  kubespray groups) + group_vars
- `fixtures/` — pg (restore probe on k8s-ctl1), crdapp (PVC workload on
  k8s-node1), sample CRD + CR
- `scripts/up.sh` — the full runbook with gates

## Bring-up order (or just `scripts/up.sh`)

1. `scripts/gen-keys.sh`
2. `vagrant up pbs` → `ansible-playbook -i inventory/hosts.yml provision/pbs.yml`
3. `vagrant up k8s-ctl1 k8s-node1`
4. `vendor/get-kubespray.sh` (skips itself when already provisioned)
5. `vendor/kubespray/venv/bin/ansible-playbook -i inventory/hosts.yml vendor/kubespray/cluster.yml`
   (run from `testenv/`; admin.conf is fetched to `inventory/artifacts/` and
   copied to `artifacts/admin.conf`)
6. `export KUBECONFIG=$PWD/artifacts/admin.conf`, untaint `k8s-ctl1`
   (`kubectl taint node k8s-ctl1 node-role.kubernetes.io/control-plane:NoSchedule- || true`),
   wait for both nodes Ready
7. apply fixtures (CRD first, wait for Established, then the directory) +
   `secrets/pbsrepo-testenv.yaml` + `secrets/pbsrepo-testenv-bootstrap.yaml`
8. Spread check: fixture pods must run on BOTH nodes and all PVCs must be
   Bound — `up.sh` fails loudly otherwise.

Re-runs are cheap: `SKIP=kubespray ./up.sh`, or comma-separated step names
(`keys,pbs,nodes,vendor,kubespray,apply`).

## RAM / disk cautions

The full stack needs ~13 GB host RAM and lots of disk; the host has ~44 G
free. If pressure hits between heavy phases: `vagrant suspend pbs` (the
k8s-only phases don't need PBS). If free disk drops below ~10 G, stop and
`vagrant destroy` + rebuild rather than letting VM disks fill the host.

## Remint procedure

PBS token secrets are generate-only (printed once). They are stored on the VM
at `/root/pbs-secrets/<token>.secret` (0600) and reused on re-runs. To mint
fresh tokens:

    ansible-playbook -i inventory/hosts.yml provision/pbs.yml -e remint=true

This deletes + re-creates `operator@pbs!producer` and `operator@pbs!pilot`,
refreshes `secrets/*.token`, `secrets/pbsrepo-testenv*.yaml`, and re-renders
them. The client encryption key (`secrets/pbs-keyfile.json`,
`/root/.config/proxmox-backup/encryption-key.json` on the VM — the client key
is per-user XDG and the playbook runs as root) is NEVER regenerated —
it must stay stable across restores.

## Notes

- The rendered `pbsrepo-testenv`/`pbsrepo-testenv-bootstrap` Secrets carry no
  namespace; they apply wherever `kubectl`'s context points (default).
- `kubectl` must be installed on the host (`kubectl_localhost: false`).
- Disposable VMs reuse static IPs, so their host keys churn on every rebuild.
  `up.sh` refreshes `~/.ssh/known_hosts` for .10 (before the PBS playbook) and
  .11/.12 (before kubespray): `ssh-keygen -R` + `ssh-keyscan -H`. A host that
  is not up yet only warns — the following step fails loudly if it matters.
- Namespace creation on PBS 4.2.5 has no `proxmox-backup-manager` subcommand;
  `provision/pbs.yml` tries the CLI, then the local debug API, then fails with
  the documented REST call (`POST /api2/json/admin/datastore/k8s-test/namespace`).
