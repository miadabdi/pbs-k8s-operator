#!/usr/bin/env bash
# Sideload the registry.k8s.io images kubespray needs into both nodes'
# containerd, via host docker. registry.k8s.io is geo-blocked from the VMs
# (403) while quay.io / docker.io are reachable from them — calico, etcd and
# the fixtures keep pulling online; only the registry.k8s.io set below travels
# host docker (which has the proxy) -> docker save -> /vagrant -> ctr images
# import. Kubespray's Check_pull_required skips images already present, so a
# subsequent kubespray run proceeds.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."   # testenv/

cache_dir=.image-cache
tar_file="$cache_dir/k8s-system-images.tar"

# Exact set for this config: kube 1.35.4, calico (quay.io, not listed),
# metrics-server + the default DNS stack (nodelocaldns + its autoscaler)
# enabled. local-path-provisioner is docker.io/rancher — VMs reach docker.io,
# it stays online. NOTE: published tags are inconsistent — kube-* and
# coredns/metrics-server/cluster-proportional-autoscaler carry a v prefix,
# pause and k8s-dns-node-cache do not (verified via docker manifest inspect;
# kubespray's map stores unprefixed versions and prepends v at use-site).
images=(
  "registry.k8s.io/kube-apiserver:v1.35.4"
  "registry.k8s.io/kube-controller-manager:v1.35.4"
  "registry.k8s.io/kube-scheduler:v1.35.4"
  "registry.k8s.io/kube-proxy:v1.35.4"
  "registry.k8s.io/pause:3.10.1"
  "registry.k8s.io/coredns/coredns:v1.12.4"
  "registry.k8s.io/metrics-server/metrics-server:v0.8.1"
  "registry.k8s.io/dns/k8s-dns-node-cache:1.25.0"
  "registry.k8s.io/cpa/cluster-proportional-autoscaler:v1.8.8"
)
nodes=(k8s-ctl1 k8s-node1)

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not on PATH (host docker carries the proxy)" >&2; exit 1; }

node_refs() {  # all image refs present in <node>'s containerd (k8s.io ns)
  vagrant ssh "$1" -c 'sudo ctr -n k8s.io images ls -q' 2>/dev/null | tr -d '\r'
}

node_has_all() {
  local node img refs
  for node in "${nodes[@]}"; do
    refs="$(node_refs "$node")" || return 1
    for img in "${images[@]}"; do
      grep -qFx "$img" <<<"$refs" || return 1
    done
  done
  return 0
}

if node_has_all; then
  echo "sideload: both nodes already have all ${#images[@]} images, nothing to do"
  exit 0
fi

local_images=()
for img in "${images[@]}"; do
  if docker pull "$img"; then
    local_images+=("$img")
    continue
  fi
  if docker image inspect "$img" >/dev/null 2>&1; then
    echo "sideload: pull failed, using local copy of $img" >&2
    local_images+=("$img")
    continue
  fi
  on_nodes=1
  for node in "${nodes[@]}"; do
    node_refs "$node" | grep -qFx "$img" || on_nodes=0
  done
  if [ "$on_nodes" -eq 1 ]; then
    echo "sideload: pull failed but $img already on both nodes, skipping it" >&2
    continue
  fi
  echo "ERROR: cannot get $img (pull failed, not local, not on the nodes)" >&2
  exit 1
done

if [ "${#local_images[@]}" -gt 0 ]; then
  mkdir -p "$cache_dir"
  echo "sideload: docker save -> $tar_file"
  docker save -o "$tar_file" "${local_images[@]}"
  for node in "${nodes[@]}"; do
    echo "sideload: importing ${#local_images[@]} images into $node"
    vagrant ssh "$node" -c "sudo ctr -n k8s.io images import /vagrant/$tar_file" >/dev/null
  done
fi

node_has_all || { echo "ERROR: images still missing after import" >&2; exit 1; }
echo "sideload: OK (${#images[@]} images present on both nodes)"
