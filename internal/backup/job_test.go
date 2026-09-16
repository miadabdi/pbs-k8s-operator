package backup

import (
	"fmt"
	"reflect"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

var fullSpec = BackupJobSpec{
	Name:       "pg-backup",
	Namespace:  "app",
	Node:       "k8s-node1",
	RepoSecret: "pbsrepo-testenv",
	PVCs:       []string{"pg-data", "pg-wal"},
	Image:      "pbs-agent:dev",
}

// wantEnv is the EXACT testenv secret contract: env var -> secret key, in order.
var wantEnv = []struct{ env, key string }{
	{"PBS_HOST", "host"},
	{"PBS_PORT", "port"},
	{"PBS_DATASTORE", "datastore"},
	{"PBS_NS", "namespace"},
	{"PBS_TOKEN_ID", "tokenID"},
	{"PBS_TOKEN_SECRET", "tokenSecret"},
	{"PBS_FINGERPRINT", "fingerprint"},
	{"PBS_KEYFILE", "keyfile"},
}

func TestBuildBackupJob(t *testing.T) {
	job := BuildBackupJob(fullSpec)

	// Metadata: name, namespace, labels.
	if job.Name != fullSpec.Name || job.Namespace != fullSpec.Namespace {
		t.Errorf("job meta = %s/%s, want %s/%s", job.Namespace, job.Name, fullSpec.Namespace, fullSpec.Name)
	}
	if got := job.Labels["app.kubernetes.io/managed-by"]; got != "pbs-operator" {
		t.Errorf("label managed-by = %q, want pbs-operator", got)
	}
	if got := job.Labels["pbsbackup"]; got != fullSpec.Name {
		t.Errorf("label pbsbackup = %q, want %q", got, fullSpec.Name)
	}
	if len(job.Labels) != 3 {
		t.Errorf("job has %d labels, want exactly 3: %v", len(job.Labels), job.Labels)
	}

	// Template carries the same labels: the controller locates the Job's pod
	// via pbsbackup to read its termination log; the managed label keeps the
	// pod out of api.yaml.
	tl := job.Spec.Template.Labels
	if tl["app.kubernetes.io/managed-by"] != "pbs-operator" || tl["pbsbackup"] != fullSpec.Name {
		t.Errorf("template labels = %v, want managed-by=pbs-operator and pbsbackup=%q", tl, fullSpec.Name)
	}
	if tl[ManagedLabel] != "true" {
		t.Errorf("template labels = %v, want %s=true (serializer exclusion)", tl, ManagedLabel)
	}
	if len(tl) != 3 {
		t.Errorf("template has %d labels, want exactly 3: %v", len(tl), tl)
	}

	spec := job.Spec.Template.Spec

	// Affinity: required hostname In [Node].
	na := spec.Affinity
	if na == nil || na.NodeAffinity == nil || na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatalf("missing required nodeAffinity: %+v", na)
	}
	terms := na.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("node selector terms = %+v, want one term with one matchExpression", terms)
	}
	req := terms[0].MatchExpressions[0]
	if req.Key != "kubernetes.io/hostname" || req.Operator != corev1.NodeSelectorOpIn || len(req.Values) != 1 || req.Values[0] != fullSpec.Node {
		t.Errorf("nodeAffinity requirement = %+v, want kubernetes.io/hostname In [%s]", req, fullSpec.Node)
	}

	// Hostname: CR name (becomes the PBS backup-id).
	if spec.Hostname != fullSpec.Name {
		t.Errorf("pod hostname = %q, want %q", spec.Hostname, fullSpec.Name)
	}

	// Restart policy, backoff, ttl.
	if spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", spec.RestartPolicy)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Errorf("backoffLimit = %v, want 0", job.Spec.BackoffLimit)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 3600 {
		t.Errorf("ttlSecondsAfterFinished = %v, want 3600", job.Spec.TTLSecondsAfterFinished)
	}

	// ServiceAccount empty -> default (unset).
	if spec.ServiceAccountName != "" {
		t.Errorf("serviceAccountName = %q, want empty for default", spec.ServiceAccountName)
	}

	// One container.
	if len(spec.Containers) != 1 {
		t.Fatalf("got %d containers, want 1", len(spec.Containers))
	}
	c := spec.Containers[0]
	if c.Image != fullSpec.Image {
		t.Errorf("image = %q, want %q", c.Image, fullSpec.Image)
	}
	if c.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("imagePullPolicy = %q, want IfNotPresent", c.ImagePullPolicy)
	}

	// Command: agent binary, backup subcommand, one --pvc pair per PVC, in order.
	wantCmd := []string{"pbs-agent", "backup"}
	for _, pvc := range fullSpec.PVCs {
		wantCmd = append(wantCmd, "--pvc", pvc)
	}
	if fmt.Sprint(c.Command) != fmt.Sprint(wantCmd) {
		t.Errorf("command = %v, want %v", c.Command, wantCmd)
	}

	// Volumes: one per PVC, named pvc-<i>, ReadOnly, in order.
	for i, pvc := range fullSpec.PVCs {
		v := spec.Volumes[i]
		if v.Name != fmt.Sprintf("pvc-%d", i) {
			t.Errorf("volumes[%d].Name = %q, want pvc-%d", i, v.Name, i)
		}
		if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != pvc || !v.PersistentVolumeClaim.ReadOnly {
			t.Errorf("volumes[%d] = %+v, want ReadOnly claim %q", i, v.PersistentVolumeClaim, pvc)
		}
	}

	// Mounts: ReadOnly at /backup/<pvc>, in order, matching volume names.
	for i, pvc := range fullSpec.PVCs {
		m := c.VolumeMounts[i]
		if m.Name != fmt.Sprintf("pvc-%d", i) {
			t.Errorf("mounts[%d].Name = %q, want pvc-%d", i, m.Name, i)
		}
		if m.MountPath != "/backup/"+pvc {
			t.Errorf("mounts[%d].MountPath = %q, want /backup/%s", i, m.MountPath, pvc)
		}
		if !m.ReadOnly {
			t.Errorf("mounts[%d] not ReadOnly", i)
		}
	}

	// Env: exactly the 8 contract vars, all via secretKeyRef to RepoSecret.
	if len(c.Env) != len(wantEnv) {
		t.Fatalf("got %d env vars, want %d: %v", len(c.Env), len(wantEnv), c.Env)
	}
	for i, w := range wantEnv {
		e := c.Env[i]
		if e.Name != w.env {
			t.Errorf("env[%d].Name = %q, want %q", i, e.Name, w.env)
		}
		if e.Value != "" {
			t.Errorf("env[%d] (%s) inlines a value; must come from secretKeyRef", i, e.Name)
		}
		skr := e.ValueFrom.SecretKeyRef
		if skr == nil {
			t.Errorf("env[%d] (%s) missing secretKeyRef", i, e.Name)
			continue
		}
		if skr.Name != fullSpec.RepoSecret {
			t.Errorf("env %s secretKeyRef.Name = %q, want %q", e.Name, skr.Name, fullSpec.RepoSecret)
		}
		if skr.Key != w.key {
			t.Errorf("env %s secretKeyRef.Key = %q, want %q", e.Name, skr.Key, w.key)
		}
	}
}

func TestBuildBackupJobServiceAccount(t *testing.T) {
	s := fullSpec
	s.ServiceAccount = "pbs-backup"
	job := BuildBackupJob(s)
	if got := job.Spec.Template.Spec.ServiceAccountName; got != "pbs-backup" {
		t.Errorf("serviceAccountName = %q, want pbs-backup", got)
	}
}

// StagingConfigMap adds the api-staging volume/mount and the --api pair; the
// pvc volumes/mounts and --pvc pairs stay untouched.
func TestBuildBackupJobStaging(t *testing.T) {
	s := fullSpec
	s.StagingConfigMap = "pg-backup-api"
	job := BuildBackupJob(s)

	c := job.Spec.Template.Spec.Containers[0]
	// Command: pvc pairs first, then --api /staging/api.
	wantCmd := []string{"pbs-agent", "backup", "--pvc", "pg-data", "--pvc", "pg-wal", "--api", "/staging/api"}
	if fmt.Sprint(c.Command) != fmt.Sprint(wantCmd) {
		t.Errorf("command = %v, want %v", c.Command, wantCmd)
	}

	// Volumes: pvcs then the staging ConfigMap volume.
	vols := job.Spec.Template.Spec.Volumes
	if len(vols) != 3 {
		t.Fatalf("got %d volumes, want 3 (2 pvc + api-staging)", len(vols))
	}
	cmVol := vols[2]
	if cmVol.Name != "api-staging" || cmVol.ConfigMap == nil || cmVol.ConfigMap.Name != "pg-backup-api" {
		t.Errorf("staging volume = %+v, want api-staging ConfigMap pg-backup-api", cmVol)
	}

	// Mounts: pvc mounts then /staging/api, ReadOnly.
	if len(c.VolumeMounts) != 3 {
		t.Fatalf("got %d mounts, want 3", len(c.VolumeMounts))
	}
	m := c.VolumeMounts[2]
	if m.Name != "api-staging" || m.MountPath != "/staging/api" || !m.ReadOnly {
		t.Errorf("staging mount = %+v, want api-staging at /staging/api ReadOnly", m)
	}
}

// API-only backup: no PVCs, staging only — one volume, one mount, --api alone.
func TestBuildBackupJobStagingOnly(t *testing.T) {
	job := BuildBackupJob(BackupJobSpec{
		Name: "bk-api", Namespace: "app", Node: "k8s-node1",
		RepoSecret: "pbsrepo-testenv", StagingConfigMap: "bk-api-staging",
		Image: "pbs-agent:dev",
	})

	c := job.Spec.Template.Spec.Containers[0]
	wantCmd := []string{"pbs-agent", "backup", "--api", "/staging/api"}
	if fmt.Sprint(c.Command) != fmt.Sprint(wantCmd) {
		t.Errorf("command = %v, want %v", c.Command, wantCmd)
	}
	if len(job.Spec.Template.Spec.Volumes) != 1 || job.Spec.Template.Spec.Volumes[0].Name != "api-staging" {
		t.Errorf("volumes = %+v, want only api-staging", job.Spec.Template.Spec.Volumes)
	}
	if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != "/staging/api" {
		t.Errorf("mounts = %+v, want only /staging/api", c.VolumeMounts)
	}
}

// M3: spec.Notes is passed to the agent as "--notes <value>"; empty → absent.
func TestBuildBackupJobNotes(t *testing.T) {
	spec := fullSpec
	spec.Notes = `{"keep-daily":7}`
	job := BuildBackupJob(spec)
	cmd := job.Spec.Template.Spec.Containers[0].Command
	want := []string{"pbs-agent", "backup", "--pvc", "pg-data", "--pvc", "pg-wal", "--notes", `{"keep-daily":7}`}
	if !reflect.DeepEqual(cmd, want) {
		t.Errorf("command = %q\nwant %q", cmd, want)
	}

	spec.Notes = ""
	job = BuildBackupJob(spec)
	for i, a := range job.Spec.Template.Spec.Containers[0].Command {
		if a == "--notes" && i > 0 {
			t.Errorf("empty notes still passed: %q", job.Spec.Template.Spec.Containers[0].Command)
		}
	}
}

// ---- M5 restore Jobs -------------------------------------------------------

var restoreSpec = RestoreJobSpec{
	Name:           "r1-api-fetch",
	Restore:        "r1",
	Namespace:      "restore-test",
	RepoSecret:     "pbsrepo-testenv",
	Ref:            "host/pg-bk-1-k8s-ctl1/2026-09-16T10:00:00Z",
	Image:          "pbs-agent:dev",
	ServiceAccount: "pbs-restore",
}

// Shared skeleton: labels (incl managed), env contract, SA, backoff/TTL.
func checkRestoreJobBase(t *testing.T, job *batchv1.Job) {
	t.Helper()
	if job.Namespace != restoreSpec.Namespace || job.Labels["pbsrestore"] != restoreSpec.Name &&
		job.Labels["pbsrestore"] != restoreSpec.Restore {
		t.Errorf("job meta = %s/%s labels %v", job.Namespace, job.Name, job.Labels)
	}
	if job.Labels[ManagedLabel] != "true" || job.Spec.Template.Labels[ManagedLabel] != "true" {
		t.Errorf("managed label missing: %v / %v", job.Labels, job.Spec.Template.Labels)
	}
	if got := job.Spec.Template.Spec.ServiceAccountName; got != "pbs-restore" {
		t.Errorf("serviceAccountName = %q, want pbs-restore", got)
	}
	if *job.Spec.BackoffLimit != 0 || *job.Spec.TTLSecondsAfterFinished != 3600 {
		t.Errorf("backoffLimit/TTL = %d/%d, want 0/3600", *job.Spec.BackoffLimit, *job.Spec.TTLSecondsAfterFinished)
	}
	c := job.Spec.Template.Spec.Containers[0]
	if c.Image != "pbs-agent:dev" || c.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("container image = %s/%s", c.Image, c.ImagePullPolicy)
	}
	// The 8 contract vars must be present (by name — restore jobs add the
	// cache env on top).
	byName := map[string]corev1.EnvVar{}
	for _, e := range c.Env {
		byName[e.Name] = e
	}
	for _, w := range wantEnv {
		e, ok := byName[w.env]
		if !ok || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil ||
			e.ValueFrom.SecretKeyRef.Key != w.key || e.ValueFrom.SecretKeyRef.Name != "pbsrepo-testenv" {
			t.Errorf("env %s = %+v, want secretKeyRef %s from pbsrepo-testenv", w.env, e, w.key)
		}
	}
}

// checkCache asserts the tmpfs client-cache volume + env (the O_TMPFILE fix).
func checkCache(t *testing.T, job *batchv1.Job) {
	t.Helper()
	pod := job.Spec.Template.Spec
	var vol *corev1.Volume
	for i := range pod.Volumes {
		if pod.Volumes[i].Name == "client-cache" {
			vol = &pod.Volumes[i]
		}
	}
	if vol == nil || vol.EmptyDir == nil || vol.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Errorf("client-cache volume = %+v, want memory emptyDir", vol)
	}
	byName := map[string]corev1.EnvVar{}
	for _, e := range pod.Containers[0].Env {
		byName[e.Name] = e
	}
	if byName["XDG_CACHE_HOME"].Value != "/cache" || byName["TMPDIR"].Value != "/cache" {
		t.Errorf("cache env missing: %v", byName)
	}
}

// Fetch Job: emptyDir staging, api.pxar.didx + --configmap upload, no pin.
func TestBuildAPIFetchJob(t *testing.T) {
	job := BuildAPIFetchJob(restoreSpec, "r1-api")
	checkRestoreJobBase(t, job)
	c := job.Spec.Template.Spec.Containers[0]
	wantCmd := []string{
		"pbs-agent", "restore-volume",
		"--ref", restoreSpec.Ref,
		"--archive", "api.pxar.didx",
		"--target", "/staging/api",
		"--configmap", "r1-api",
	}
	if !reflect.DeepEqual(c.Command, wantCmd) {
		t.Errorf("command = %q\nwant %q", c.Command, wantCmd)
	}
	vol := job.Spec.Template.Spec.Volumes[0]
	if vol.EmptyDir == nil {
		t.Errorf("fetch volume = %+v, want emptyDir", vol)
	}
	if c.VolumeMounts[0].MountPath != "/staging/api" || c.VolumeMounts[0].ReadOnly {
		t.Errorf("mounts = %+v, want rw /staging/api", c.VolumeMounts)
	}
	if job.Spec.Template.Spec.Affinity != nil {
		t.Error("fetch job must not be node-pinned")
	}
	checkCache(t, job)
}

// Apply Jobs: ConfigMap mount, --file under it, drop/keep pass-through; the
// pre and workload phases differ only in --phase.
func TestBuildApplyJob(t *testing.T) {
	spec := restoreSpec
	spec.Name = "r1-api-pre"
	job := BuildApplyJob(spec, "pre", "r1-api", []string{"Secret"}, []string{"Probe"})
	checkRestoreJobBase(t, job)
	c := job.Spec.Template.Spec.Containers[0]
	wantCmd := []string{
		"pbs-agent", "apply-manifests",
		"--phase", "pre",
		"--file", "/staging/api/api.yaml",
		"--drop", "Secret",
		"--keep", "Probe",
	}
	if !reflect.DeepEqual(c.Command, wantCmd) {
		t.Errorf("command = %q\nwant %q", c.Command, wantCmd)
	}
	vol := job.Spec.Template.Spec.Volumes[0]
	if vol.ConfigMap == nil || vol.ConfigMap.Name != "r1-api" {
		t.Errorf("apply volume = %+v, want configmap r1-api", vol)
	}
	if !c.VolumeMounts[0].ReadOnly {
		t.Errorf("apply mount must be read-only: %+v", c.VolumeMounts)
	}

	// No filters → no flags.
	bare := BuildApplyJob(spec, "workload", "r1-api", nil, nil)
	cmd := bare.Spec.Template.Spec.Containers[0].Command
	for _, f := range []string{"--drop", "--keep"} {
		for _, a := range cmd {
			if a == f {
				t.Errorf("empty filters still emitted %s: %q", f, cmd)
			}
		}
	}
}

// Volume Job: node-pinned (WFFC binding follows the Job's node), PVCs
// mounted rw at /backup/<name>, one --archive/--target pair per PVC.
func TestBuildRestoreVolumeJob(t *testing.T) {
	spec := restoreSpec
	spec.Name = "r1-vol-k8s-ctl1"
	job := BuildRestoreVolumeJob(spec, "k8s-ctl1", []string{"data-pg-0"})
	checkRestoreJobBase(t, job)

	sel := job.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	req := sel.NodeSelectorTerms[0].MatchExpressions[0]
	if req.Key != "kubernetes.io/hostname" || req.Operator != corev1.NodeSelectorOpIn || req.Values[0] != "k8s-ctl1" {
		t.Errorf("nodeAffinity = %+v, want hostname In [k8s-ctl1]", req)
	}
	vol := job.Spec.Template.Spec.Volumes[0]
	if vol.PersistentVolumeClaim == nil || vol.PersistentVolumeClaim.ClaimName != "data-pg-0" {
		t.Errorf("volume = %+v, want data-pg-0", vol)
	}
	m := job.Spec.Template.Spec.Containers[0].VolumeMounts[0]
	if m.MountPath != "/backup/data-pg-0" || m.ReadOnly {
		t.Errorf("mount = %+v, want rw /backup/data-pg-0", m)
	}
	wantCmd := []string{
		"pbs-agent", "restore-volume",
		"--ref", restoreSpec.Ref,
		"--archive", "pvc-data-pg-0.pxar.didx",
		"--target", "/backup/data-pg-0",
	}
	if got := job.Spec.Template.Spec.Containers[0].Command; !reflect.DeepEqual(got, wantCmd) {
		t.Errorf("command = %q\nwant %q", got, wantCmd)
	}
	checkCache(t, job)
	// The apply Jobs run only kubectl — no PBS client, no cache volume.
	if apply := BuildApplyJob(restoreSpec, "pre", "r1-api", nil, nil); func() bool {
		for _, v := range apply.Spec.Template.Spec.Volumes {
			if v.Name == "client-cache" {
				return true
			}
		}
		return false
	}() {
		t.Error("apply job carries the client-cache volume, want kubectl-only")
	}
}
