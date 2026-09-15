#!/usr/bin/env bash
# Bring up the whole testenv in order, with gate checks at the end.
#   usage: testenv/scripts/up.sh
# Skip steps for cheap re-runs (comma-separated):
#   SKIP=kubespray ./up.sh
#   SKIP=keys,pbs,nodes,vendor,kubespray,apply ./up.sh   # gates only
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."   # testenv/
SKIP="${SKIP:-}"

step() { printf '\n==> %s\n' "$*"; }
skip() { case ",$SKIP," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }

# Disposable VMs reusing static IPs churn host keys: drop the stale entry and
# re-scan from the fresh VM, so ansible (default host_key_checking) stays
# non-interactive. Tolerates "no such key" AND a host that is not up yet.
refresh_host_key() {
  local ip="$1"
  mkdir -p -m 700 "$HOME/.ssh"
  ssh-keygen -R "$ip" >/dev/null 2>&1 || true
  if ssh-keyscan -H -T 5 "$ip" >>"$HOME/.ssh/known_hosts" 2>/dev/null; then
    echo "host key refreshed: $ip"
  else
    echo "WARN: $ip not reachable for keyscan (yet?)" >&2
  fi
}

command -v vagrant >/dev/null 2>&1 || { echo "ERROR: vagrant not on PATH" >&2; exit 1; }
command -v ansible-playbook >/dev/null 2>&1 || { echo "ERROR: ansible-playbook not on PATH" >&2; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "ERROR: kubectl not on PATH (kubectl_localhost is false; install kubectl on the host)" >&2; exit 1; }

if ! skip keys; then
  step "1/8 shared SSH key"
  scripts/gen-keys.sh
fi

if ! skip pbs; then
  step "2/8 PBS VM + provisioning"
  vagrant up pbs
  refresh_host_key 192.168.56.10
  ansible-playbook -i inventory/hosts.yml provision/pbs.yml
fi

if ! skip nodes; then
  step "3/8 k8s VMs"
  vagrant up k8s-ctl1 k8s-node1
fi

if ! skip vendor; then
  step "4/8 kubespray vendor"
  vendor/get-kubespray.sh
fi

if ! skip kubespray; then
  step "5/8 kubespray cluster.yml (run from inside the clone so its ansible.cfg loads)"
  refresh_host_key 192.168.56.11
  refresh_host_key 192.168.56.12
  # cwd must be vendor/kubespray: its repo-root ansible.cfg (roles_path=roles:...)
  # is only picked up from there; from testenv/ the play dies at boilerplate.yml
  # with "role 'dynamic_groups' was not found".
  (cd vendor/kubespray && venv/bin/ansible-playbook -i ../../inventory/hosts.yml cluster.yml)
fi

step "6/8 kubeconfig + untaint guard + wait for nodes"
# kubespray (kubeconfig_localhost: true) fetches admin.conf to
# {{ inventory_dir }}/artifacts/ — copy it to the canonical testenv location.
mkdir -p artifacts
if [ -f inventory/artifacts/admin.conf ]; then
  cp inventory/artifacts/admin.conf artifacts/admin.conf
  chmod 0600 artifacts/admin.conf
fi
export KUBECONFIG="$PWD/artifacts/admin.conf"
[ -f "$KUBECONFIG" ] || { echo "ERROR: $KUBECONFIG missing — run without SKIP=kubespray first" >&2; exit 1; }
kubectl taint node k8s-ctl1 node-role.kubernetes.io/control-plane:NoSchedule- || true
kubectl wait --for=condition=Ready node/k8s-ctl1 --timeout=900s
kubectl wait --for=condition=Ready node/k8s-node1 --timeout=900s

if ! skip apply; then
  step "7/8 fixtures + PBS secrets"
  # CRD first (a directory apply would sort sample-cr.yaml before
  # sample-crd.yaml and fail with "no matches for kind Probe"), and wait for
  # the CRD to be Established before the CR lands.
  kubectl apply -f fixtures/sample-crd.yaml
  kubectl wait --for=condition=Established crd/probes.examples.sharifmind.ir --timeout=120s
  kubectl apply -f fixtures/
  kubectl apply -f secrets/pbsrepo-testenv.yaml
  kubectl apply -f secrets/pbsrepo-testenv-bootstrap.yaml
fi

step "8/8 spread check (fixture pods on BOTH nodes, all PVCs Bound)"
# rollout status (unlike `kubectl wait` on a label selector) tolerates the
# window before the pod objects exist.
kubectl rollout status statefulset/pg -n pg --timeout=600s
kubectl rollout status deployment/crdapp -n crdapp --timeout=600s
pg_node="$(kubectl get pod -n pg -l app=pg -o jsonpath='{.items[0].spec.nodeName}')"
app_node="$(kubectl get pod -n crdapp -l app=crdapp -o jsonpath='{.items[0].spec.nodeName}')"
[ "$pg_node" = "k8s-ctl1" ] || { echo "FAIL: pg pod runs on '$pg_node', expected k8s-ctl1" >&2; exit 1; }
[ "$app_node" = "k8s-node1" ] || { echo "FAIL: crdapp pod runs on '$app_node', expected k8s-node1" >&2; exit 1; }
unbound="$(kubectl get pvc -A -o jsonpath='{range .items[?(@.status.phase!="Bound")]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}')"
[ -z "$unbound" ] || { echo "FAIL: PVCs not Bound:" >&2; echo "$unbound" >&2; exit 1; }
echo "SPREAD OK: pg on k8s-ctl1, crdapp on k8s-node1, all PVCs Bound."
