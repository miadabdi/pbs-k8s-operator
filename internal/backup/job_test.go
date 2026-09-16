package backup

import (
	"fmt"
	"reflect"
	"testing"

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
