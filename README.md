# pbs-operator

Kubernetes backup/restore operator backed by [Proxmox Backup Server](https://pbs.proxmox.com/)
(PBS). The Velero model with PBS as the storage backend: a controller watches
namespaces' workloads and volumes, snapshots volume data plus the namespace's
API objects into a PBS datastore as content-addressed, deduplicated, encrypted
pxar archives, and replays them later — same cluster or another one — as Jobs.

Volume data is copied by a node-pinned Job per node (local-path volumes are
only readable on the node that holds them), client-side encrypted with a PBS
keyfile before it ever leaves the cluster, and the whole namespace's API state
(Deployments, Secrets, CRs, ...) rides along as a serialized `api.pxar`
archive, so a restore rebuilds the namespace, not just its disks.

## Architecture

API group `pbs.sharifmind.ir/v1`:

| CRD          | Scope    | Purpose                                                                     |
|--------------|----------|-----------------------------------------------------------------------------|
| `PBSRepo`    | Cluster  | One PBS server + credentials secret; health-probed (Ready/Reachable)        |
| `PBSBackup`  | Namespaced| One backup of the CR's namespace into a repo                                |
| `PBSSchedule`| Namespaced| Cron-driven PBSBackup factory (template + repoRef)                          |
| `PBSRestore` | Namespaced| One restore of a snapshotRef into a target namespace                        |

Controllers (`internal/controller/`):

- **PBSRepo** — validates the credentials secret (`tokenID`, `tokenSecret`,
  `fingerprint` from spec-first/secret-fallback), pings PBS via the REST API
  with TLS fingerprint pinning, optionally bootstraps the PBS namespace via
  `bootstrapTokenRef`, re-probes every 5m. CR spec fields win over secret keys.
  The controller only REQUIRES those three — but the backup/restore Jobs
  consume the secret's keys verbatim (all 8: `host`, `port`, `datastore`,
  `namespace`, `tokenID`, `tokenSecret`, `fingerprint`, `keyfile`) with no
  spec merge: the referenced secret MUST carry the full contract consistent
  with the spec, or a spec-complete PBSRepo shows Ready while its Jobs fail
  on the missing secret key / agent "missing required env".
- **PBSBackup** — the backup pipeline below.
- **PBSSchedule** — standard 5-field cron; fires at most one backup per missed
  slot (catch-up to the latest, never backfill); deterministic per-slot backup
  names make duplicate fires an ignored AlreadyExists.
- **PBSRestore** — the 4-Job restore pipeline below.

The backup/restore work itself runs in `pbs-agent` Jobs
(`internal/agent`, image `pbs-agent`): a thin Go wrapper around
`proxmox-backup-client` (static) and `kubectl`, reporting
`{"snapshotRef","bytes"}` JSON through the container termination log.

### Backup flow

```
PBSBackup (ns: pg)
 │ 1  repo gate: PBSRepo Ready?                       (else hold, 1m requeue)
 │ 2  secret projection: copy the repo secret's 8 contract keys
 │      into <ns>/pbsrepo-<repo> (Jobs dereference it there)
 │ 3  PVC select: spec.pvcs > spec.selector > all PVCs in the namespace
 │ 4  placement: PV nodeAffinity kubernetes.io/hostname, else a mounting
 │      pod's nodeName; API-only backups take the first node alphabetically
 │    serialize the namespace's API objects → ConfigMap <backup>-api
 │      (deterministic YAML, volatile fields stripped, managed objects skipped)
 │ 4.5 pre-hooks: pods mounting a selected PVC with annotations
 │      pbs.backup/pre-hook-container / pbs.backup/pre-hook-command (JSON argv)
 │      are exec'd sequentially (120s cap) BEFORE any Job is created
 ▼
one Job per node: <backup>-<node>   (pod hostname = PBS backup-id)
      pbs-agent backup --pvc <pvc>… [--api /staging/api] [--notes <notes>]
        → pvc-<name>.pxar per volume + api.pxar   (client-side encrypted)
        → `snapshot notes update` attaches spec.notes (best-effort)
 ▼
termination log {"snapshotRef":"host/<id>/<ISO8601Z>","bytes":N}
      → status.SnapshotRef (single-node) / status.Jobs[].State per node
```

Terminal phases (`Completed`/`Failed`) are final: no requeue, no Job
resurrection. A Job deleted mid-run holds the backup (`JobDeleted`); hooks run
at most once per backup (`status.Jobs` non-empty means they already ran).

### Restore flow (4 Jobs)

```
PBSRestore (spec: repoRef, snapshotRef, targetNamespace[, dropKinds/keepKinds])
 │ repo gate → create target ns if missing → copy repo secret into it
 │ provision Job RBAC: SA pbs-restore + per-ns RoleBinding + one CRB
 ▼
1 <name>-api-fetch     restore api.pxar.didx → emptyDir → upload as
                      ConfigMap <name>-api (kubectl, in-pod SA)
2 <name>-api-pre       apply namespaces → CRDs (wait Established, 60s each)
                      → non-workload docs: PVCs (unbound!), Secrets, CRs, …
3 <name>-vol-<node>    restore pvc-*.pxar.didx per node. THIS Job is the
                      WaitForFirstConsumer consumer: its nodeAffinity picks
                      the node, the claims bind as it schedules
4 <name>-api-workload  apply workload docs (pods consume the restored data)
 ▼
phase Completed
```

## Quickstart against testenv

The repo ships a Vagrant environment (`testenv/`): a PBS VM plus a 2-node
kubespray cluster (k8s-ctl1, k8s-node1), fixtures spread across both nodes
(`pg` on ctl1, `crdapp` on node1, local-path WFFC storage), and a
pre-provisioned cluster-scoped `PBSRepo` named `testenv` (credentials secret
`default/pbsrepo-testenv`).

```sh
make testenv-up                                  # 3 VMs + cluster + fixtures (~30-60 min once)
export KUBECONFIG=$PWD/testenv/artifacts/admin.conf

# deploy loop: build both images, import into both nodes, apply config/default
make docker-build load-image deploy

# first backup: all PVCs + all API objects of namespace pg
cat <<EOF | kubectl apply -f -
apiVersion: pbs.sharifmind.ir/v1
kind: PBSBackup
metadata: {name: pg-1, namespace: pg}
spec: {repoRef: testenv}
EOF
kubectl -n pg wait --for=jsonpath='{.status.phase}'=Completed pbsbackup/pg-1 --timeout=10m
kubectl -n pg get pbsbackup pg-1 -o jsonpath='{.status.snapshotRef}{"\n"}'
# → host/pg-1-k8s-ctl1/2026-01-02T15:04:05Z

# restore that snapshot into a fresh namespace (same cluster here).
# The PBSRestore lives IN the target namespace (Jobs, ConfigMap, ownerRefs
# are namespace-local), so create the namespace first — the controller
# creates it too when the CR lives elsewhere.
kubectl create namespace pg-restored
cat <<EOF | kubectl apply -f -
apiVersion: pbs.sharifmind.ir/v1
kind: PBSRestore
metadata: {name: pg-1-r, namespace: pg-restored}
spec:
  repoRef: testenv
  snapshotRef: host/pg-1-k8s-ctl1/2026-01-02T15:04:05Z
  targetNamespace: pg-restored
EOF
kubectl -n pg-restored wait --for=jsonpath='{.status.phase}'=Completed \
  pbsrestore/pg-1-r --timeout=10m
kubectl -n pg-restored get pods,pvc
```

Full acceptance matrix (13 scenarios, live cluster + PBS):

```sh
make e2e-live
```

Pre-backup hooks: annotate the WORKLOAD's pods (the pg fixture carries one):

```yaml
metadata:
  annotations:
    pbs.backup/pre-hook-container: postgres
    pbs.backup/pre-hook-command: '["pg_dump","-U","postgres","-Fc","-f","/var/lib/postgresql/data/dump.pgc"]'
```

Retention hints: `spec.notes` on PBSBackup/PBSSchedule.template is attached to
the PBS snapshot post-upload via `snapshot notes update` (PBS has no snapshot
labels; notes are the hint channel — e.g. a keep-policy JSON). Best-effort: it
needs `Datastore.Modify` on the token, and the snapshot is already safe if it
fails. Pruning itself stays server-side.

## Restore semantics

- **Ordering.** CRDs apply (and reach `Established`) before custom resources;
  PVCs apply before volume data; workloads apply last so their pods find bound
  volumes with data. Services apply with the source's `clusterIP`/nodePorts
  stripped (fresh allocations); PVCs apply with `volumeName` and the
  bind-completed annotations stripped — replaying them desyncs WFFC into
  `ClaimLost` instead of a clean bind.
- **WFFC Job-as-consumer.** The pre phase applies claims unbound
  (WaitForFirstConsumer storage classes never bind on apply). The
  `vol-<node>` Job IS the consumer: its nodeAffinity decides where each PVC
  binds. Node choice = the consuming workload's own `kubernetes.io/hostname`
  nodeSelector pin (local-path RWO data must land where the workload will),
  else the alphabetically first node.
- **Empty-first (EEXIST).** pxar extraction refuses existing directory
  entries even with `--overwrite`, so the agent empties every target directory
  (never the mount point itself) before restoring. That plus upsert-style
  `kubectl apply` makes a second restore into the SAME namespace complete
  cleanly over live state.
- **drop/keep filters.** `spec.dropKinds` removes exact Kind names from BOTH
  apply phases; `spec.keepKinds` exempts from drop (Keep wins; Keep alone is
  an exception list, not a whitelist). Always dropped regardless: bare `Pod`,
  `Endpoints`/`EndpointSlice`, `ControllerRevision`, `PodMetrics` — derived
  state that breaks or collides on re-apply.
- **RBAC shape.** The restore Jobs run as SA `pbs-restore` (created in the
  target ns). Broad rights (every namespaced Kind, incl. Secrets) live in
  ClusterRole `pbs-operator-pbs-restore`, bound ONLY via a per-target-ns
  RoleBinding — the blast radius IS the target namespace. The cluster-scoped
  ClusterRole `pbs-operator-pbs-restore-cluster` (namespaces + CRDs, create/
  get/list/update/patch, no delete) is the sole ClusterRoleBinding surface.
  The manager's own `bind` permission is resourceNames-scoped to exactly
  those two roles.
- **Sweep contract.** SA/RoleBinding/CRB cannot carry ownerRefs (cluster-
  scoped dependents of a namespaced owner; shared target ns), so they outlive
  their PBSRestore, labeled `pbs.sharifmind.ir/managed=true` +
  `pbsrestore=<name>`. Cleanup = delete by that label pair (a stale binding
  would otherwise reactivate if the ns+SA name is recreated); a finalizer-
  based sweep is deferred.
- **Cross-namespace.** Jobs always run in the TARGET namespace; ownerRefs and
  the ConfigMap adoption are skipped when the PBSRestore lives elsewhere
  (cross-namespace owner references are invalid).

## Metrics

On the manager's metrics endpoint (`:8443`, HTTPS + authn/authz filter —
scrape with a bearer token bound to the metrics-reader ClusterRole), label
`namespace`:

| Metric | Type | Set when |
|---|---|---|
| `pbs_backup_last_success_timestamp_seconds` | gauge | Completed transition (unix ts of CompletedAt) |
| `pbs_backup_duration_seconds` | gauge | Completed transition (CompletedAt − StartedAt) |
| `pbs_backup_errors_total` | counter | Failed transition |

Compatibility note: `last_success` and `duration` are only ever SET on success
transitions — a failure never touches them. So a prod `BackupStale` alert
(`time() - pbs_backup_last_success_timestamp_seconds > N`) means exactly "no
success in N" and does not flap on failures, and a `BackupJobFailed`-class
alert maps to `increase(pbs_backup_errors_total[N]) > 0`; an `absent()`-style
alert fires correctly on namespaces that only ever failed.

## Security posture

- **TLS fingerprint pinning.** Every PBS connection (repo controller REST
  client; agent's `PBS_FINGERPRINT` env) verifies the server certificate's
  sha256 against the configured fingerprint (spec wins, secret key fallback).
  Self-signed certs are the norm; the pin IS the trust boundary — mismatch is
  a hard error (`Unreachable`).
- **Client-side encryption.** The repo secret's `keyfile` (PBS encryption key
  JSON) is materialized into each Job (0600 temp file) and every pxar archive
  — volume data AND the serialized API objects (Secrets included) — is
  encrypted client-side. The PBS server never sees plaintext.
- **Documented accepted risk — read-everything wildcard.** Full-namespace API
  serialization needs `get;list` on every namespaced resource in every
  namespace (`groups=*,resources=*,verbs=get;list` in the manager ClusterRole):
  secrets included, auto-expanding with every installed CRD. A namespaced Role
  cannot express it and the API-only node pick needs cluster-scoped nodes —
  same posture as Velero-class backup operators; accepted deliberately (also
  stated in `config/rbac/role.yaml` and the controller source).
- **Plaintext staging hop.** The serialized `api.yaml` — which INCLUDES the
  namespace's Secrets — sits as plaintext in the `<backup>-api` ConfigMap in
  the backed-up namespace until the PBSBackup CR (its owner) is garbage
  collected. ConfigMaps are unencrypted etcd objects; anyone who can read
  ConfigMaps in that namespace can read that snapshot of its Secrets. Mostly
  co-privileged (same-namespace readers can usually read those Secrets
  directly), but the copy also outlives deleted Secrets for the CR's
  lifetime. Encryption-at-rest for the hop would move serialization into the
  agent (same upgrade path as the 1 MiB ceiling above).
- **Repo-token duplication.** Every backed-up and restore-target namespace
  gets a copy of the repo credentials (`pbsrepo-<repo>`, all 8 contract keys
  incl. `tokenSecret` and `keyfile`) — inherent to Jobs-run-in-the-namespace
  dereferencing a namespace-local secret. Mitigate with a backup-scoped,
  minimal-privilege PBS token (Backup-only, per-datastore/namespace) so the
  copy is worth little on its own.
- **Restore RBAC scope.** See *Restore semantics*: the broad grant is fenced
  to the target namespace by a RoleBinding; the only cluster-wide surface is
  namespace/CRD create-update (no delete, no reads beyond those kinds).
- **Events** are emitted through the events.k8s.io/v1 recorder
  (`mgr.GetEventRecorder`); RBAC carries both the core and events.k8s.io
  events create/patch rules.

## Known ceilings — deliberate simplifications

Every shortcut below is marked `ponytail:` at its source location; the table
is the complete inventory (count matches `grep -rn "ponytail:" --include="*.go"
--include="*.yaml" | wc -l`).

| Location | Ceiling | Upgrade path |
|---|---|---|
| `internal/controller/pbsschedule_controller.go:117` | Catch-up iterates one `Next()` per missed slot (a `*/1` cron stale for a year = ~525k cheap calls, once) | Chunked time advancement if it ever matters |
| `internal/controller/pbsbackup_controller.go:268` | API-only backups run on the alphabetically first node; a NotReady/cordoned first node stalls the backup | Filter on Ready |
| `internal/controller/pbsbackup_controller.go:393` | A crash between hook exec and the status write re-execs hooks once on the next reconcile | Per-backup hook-run bookkeeping |
| `internal/controller/pbsbackup_controller.go:445` | Several backups to one repo in one ns share the copied secret `pbsrepo-<repo>`; the first backup owns it and it GCs with THAT CR | Per-backup copies |
| `internal/controller/pbsbackup_controller.go:510` | The staged `api.yaml` rides a ConfigMap (1 MiB cap) | Agent-side serialization with a read-only Job SA |
| `internal/controller/pbsbackup_controller.go:607` | Multi-node backups keep per-Job refs in `status.Jobs[].State` (`"ok:<ref>"`); `status.SnapshotRef` is set only for single-node | Aggregate after live multi-node runs |
| `internal/controller/pbsrestore_controller.go:231` | An api.yaml with no PVCs skips the volume phase entirely — an empty/mis-serialized plan can Complete with nothing restored | Cross-check the snapshot's `pvc-*` archives |
| `internal/backup/serialize.go:73` | One discovery round-trip per backup (no cache) | Cached discovery client if reconcile frequency makes it hot |
| `internal/backup/restore_plan.go:209` | Restore-side node pinning reads only pod-template `nodeSelector`s; affinity/topology-spread workloads fall back to the first node | Extend `NodeForPVC` if those workloads matter |

## Helm chart

`charts/pbs-operator` — minimal chart (Deployment + SA + ClusterRoles/
bindings + leader-election Role + metrics Service + auth roles).

- Values: `image.repository`/`image.tag`, `resources`, `agentImage`.
- The chart's ClusterRoles `pbs-operator-pbs-restore` and
  `pbs-operator-pbs-restore-cluster` are name-locked: the restore controller
  provisions bindings to those exact names.
- `crds/` carries a copy of `config/crd/bases`. Helm installs CRDs from
  `crds/` ONCE (skips any already present), never upgrades or deletes them —
  CRD changes go through `make manifests` + `kubectl apply -f charts/pbs-operator/crds/`.
- Gate used in development: `helm lint charts/pbs-operator` and
  `helm template charts/pbs-operator | kubectl apply --dry-run=server -f -`
  against the testenv cluster.

```sh
helm install pbs-operator charts/pbs-operator -n pbs-operator-system --create-namespace \
  --set image.repository=pbs-operator,image.tag=dev,agentImage=pbs-agent:dev
```

## CI

`.gitlab-ci.yml` builds and pushes both images (operator + agent) to
`nexus.sharifmind.ir` on the production GitLab only — it is not part of the
testenv dev loop.

## License

Apache 2.0 (see the source headers).
