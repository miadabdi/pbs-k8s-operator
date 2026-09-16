package backup

import (
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// BackupJobSpec carries everything needed to build a backup Job.
type BackupJobSpec struct {
	Name             string // PBSBackup CR name; also pod hostname → PBS backup-id
	Namespace        string // PBSBackup CR namespace
	Node             string // from ResolveNode
	RepoSecret       string // name of the PBSRepo secret (testenv contract keys)
	PVCs             []string
	StagingConfigMap string // optional: ConfigMap with api.yaml → api.pxar (M2)
	Notes            string // optional: PBSBackup spec.notes → agent --notes (M3)
	Image            string
	ServiceAccount   string // optional; empty → default
}

// envContract is the exact testenv PBSRepo secret key set: env var -> secret key.
var envContract = []struct{ env, key string }{
	{"PBS_HOST", "host"},
	{"PBS_PORT", "port"},
	{"PBS_DATASTORE", "datastore"},
	{"PBS_NS", "namespace"},
	{"PBS_TOKEN_ID", "tokenID"},
	{"PBS_TOKEN_SECRET", "tokenSecret"},
	{"PBS_FINGERPRINT", "fingerprint"},
	{"PBS_KEYFILE", "keyfile"},
}

// BuildBackupJob returns a complete batchv1.Job:
//   - labels on the Job AND its pod template (the controller finds the Job's
//     pod via pbsbackup to read its termination log): app.kubernetes.io/
//     managed-by=pbs-operator, pbsbackup=<Name>
//   - nodeAffinity required, hostname In [Node]
//   - hostname: <Name> (RFC1123-safe: CR names already are) → proxmox-backup-client
//     derives backup-id from hostname
//   - restartPolicy: Never; backoffLimit: 0; ttlSecondsAfterFinished: 3600
//   - one volume per PVC (name pvc-<i>), mounted ReadOnly at /backup/<pvc-name>
//   - when StagingConfigMap is set: volume api-staging (ConfigMap source)
//     mounted ReadOnly at /staging/api, and "--api /staging/api" appended to
//     the command (the agent adds the api.pxar pair)
//   - env (from RepoSecret keys — EXACT testenv contract, via secretKeyRef;
//     the Job does NOT inline secret data)
//   - container command: ["pbs-agent","backup","--pvc","<pvc1>","--pvc","<pvc2>",...]
//     (agent resolves /backup/<pvc> mounts; arg contract for the agent task)
//   - imagePullPolicy: IfNotPresent
func BuildBackupJob(spec BackupJobSpec) *batchv1.Job {
	volumes := make([]corev1.Volume, 0, len(spec.PVCs)+1)
	mounts := make([]corev1.VolumeMount, 0, len(spec.PVCs)+1)
	command := []string{"pbs-agent", "backup"}
	for i, pvc := range spec.PVCs {
		name := fmt.Sprintf("pvc-%d", i)
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc, ReadOnly: true},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: name, MountPath: "/backup/" + pvc, ReadOnly: true})
		command = append(command, "--pvc", pvc)
	}
	if spec.StagingConfigMap != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "api-staging",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: spec.StagingConfigMap},
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "api-staging", MountPath: "/staging/api", ReadOnly: true})
		command = append(command, "--api", "/staging/api")
	}
	if spec.Notes != "" {
		command = append(command, "--notes", spec.Notes)
	}

	env := contractEnv(spec.RepoSecret)

	labels := map[string]string{
		"app.kubernetes.io/managed-by": "pbs-operator",
		"pbsbackup":                    spec.Name,
		// The serializer skips managed-labeled objects, so re-reconciles never
		// fold the backup's own pod into api.yaml.
		ManagedLabel: "true",
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: spec.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](0),
			TTLSecondsAfterFinished: ptr.To[int32](3600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Hostname:      spec.Name,
					RestartPolicy: corev1.RestartPolicyNever,
					Affinity: &corev1.Affinity{
						NodeAffinity: &corev1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
								NodeSelectorTerms: []corev1.NodeSelectorTerm{{
									MatchExpressions: []corev1.NodeSelectorRequirement{{
										Key:      hostnameKey,
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{spec.Node},
									}},
								}},
							},
						},
					},
					Containers: []corev1.Container{{
						Name:            "backup",
						Image:           spec.Image,
						Command:         command,
						Env:             env,
						VolumeMounts:    mounts,
						ImagePullPolicy: corev1.PullIfNotPresent,
					}},
					Volumes: volumes,
				},
			},
		},
	}
	if spec.ServiceAccount != "" {
		job.Spec.Template.Spec.ServiceAccountName = spec.ServiceAccount
	}
	return job
}

// ---- restore Jobs (M5) -----------------------------------------------------

// RestoreJobSpec carries the shared inputs of the three restore Job shapes.
// All restore Jobs run in the TARGET namespace: PVC mounts and the api
// ConfigMap are namespace-local, and the in-pod SA namespace is what
// apply-manifests rewrites docs to.
type RestoreJobSpec struct {
	Name           string // full Job name (controller-computed, DNS-1123 ≤63)
	Restore        string // PBSRestore CR name (labels, pod grouping)
	Namespace      string // TARGET namespace
	RepoSecret     string // copied repo secret (lives in the target ns)
	Ref            string // snapshot ref "host/<id>/<ISO8601Z>"
	Image          string
	ServiceAccount string // the pbs-restore SA (broad apply rights)
}

// restoreJobBase builds the shared skeleton: managed labels (serializer
// exclusion), the 8-key env contract from the copied repo secret,
// Never/backoffLimit 0/TTL 1h, IfNotPresent, and the Job SA.
func restoreJobBase(spec RestoreJobSpec, command []string) *batchv1.Job {
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "pbs-operator",
		"pbsrestore":                   spec.Restore,
		ManagedLabel:                   "true",
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spec.Name,
			Namespace: spec.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To[int32](0),
			TTLSecondsAfterFinished: ptr.To[int32](3600),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyNever,
					ServiceAccountName: spec.ServiceAccount,
					Containers: []corev1.Container{{
						Name:            "restore",
						Image:           spec.Image,
						Command:         command,
						Env:             contractEnv(spec.RepoSecret),
						ImagePullPolicy: corev1.PullIfNotPresent,
					}},
				},
			},
		},
	}
}

// contractEnv renders the 8-key testenv secret contract (shared with backup).
func contractEnv(secret string) []corev1.EnvVar {
	env := make([]corev1.EnvVar, len(envContract))
	for i, c := range envContract {
		env[i] = corev1.EnvVar{
			Name: c.env,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: secret},
					Key:                  c.key,
				},
			},
		}
	}
	return env
}

// The PBS client stages its download temp file with O_TMPFILE in the XDG
// cache dir (falling back to /tmp). On overlayfs — the container writable
// layer on many nodes — O_TMPFILE fails with EOPNOTSUPP (live-verified on
// kernel 6.8; restore died "Operation not supported" before extracting a
// byte). A Memory-backed emptyDir (tmpfs, O_TMPFILE-capable since 3.11) plus
// XDG_CACHE_HOME/TMPDIR pointing at it sidesteps the whole class. Only the
// restore path hits this: backups stream without a cache temp file.
var (
	cacheVolume = corev1.Volume{
		Name: "client-cache",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory},
		},
	}
	cacheMount = corev1.VolumeMount{Name: "client-cache", MountPath: "/cache"}
)

// addCacheEnv routes the client's temp files to the tmpfs cache volume.
func addCacheEnv(pod *corev1.PodSpec) {
	pod.Containers[0].Env = append(pod.Containers[0].Env,
		corev1.EnvVar{Name: "XDG_CACHE_HOME", Value: "/cache"},
		corev1.EnvVar{Name: "TMPDIR", Value: "/cache"},
	)
}

// BuildAPIFetchJob builds the phase-0 Job: restore api.pxar.didx from the
// snapshot into an emptyDir, then upload it as ConfigMap configMap (in the
// target ns — the pod's own) for the apply Jobs to mount.
func BuildAPIFetchJob(spec RestoreJobSpec, configMap string) *batchv1.Job {
	command := []string{
		"pbs-agent", "restore-volume",
		"--ref", spec.Ref,
		"--archive", "api.pxar.didx",
		"--target", "/staging/api",
		"--configmap", configMap,
	}
	job := restoreJobBase(spec, command)
	pod := &job.Spec.Template.Spec // points into the job; mutations land
	pod.Volumes = []corev1.Volume{
		{
			Name: "api-staging",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		cacheVolume,
	}
	pod.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{Name: "api-staging", MountPath: "/staging/api"},
		cacheMount,
	}
	addCacheEnv(pod)
	return job
}

// BuildApplyJob builds one of the two apply Jobs (phase "pre": namespaces →
// CRDs → pre-volume docs incl PVCs; phase "workload": workload docs). The
// api ConfigMap is the mount; drop/keep filter both phases.
func BuildApplyJob(spec RestoreJobSpec, phase, configMap string, drop, keep []string) *batchv1.Job {
	command := []string{
		"pbs-agent", "apply-manifests",
		"--phase", phase,
		"--file", "/staging/api/api.yaml",
	}
	if len(drop) > 0 {
		command = append(command, "--drop", strings.Join(drop, ","))
	}
	if len(keep) > 0 {
		command = append(command, "--keep", strings.Join(keep, ","))
	}
	job := restoreJobBase(spec, command)
	pod := &job.Spec.Template.Spec // points into the job; mutations land
	pod.Volumes = []corev1.Volume{{
		Name: "api-staging",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: configMap},
			},
		},
	}}
	pod.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{Name: "api-staging", MountPath: "/staging/api", ReadOnly: true},
	}
	return job
}

// BuildRestoreVolumeJob builds one node-pinned volume restore Job: the PVCs
// were APPLIED unbound in the pre phase (WaitForFirstConsumer) — THIS Job is
// the consumer, so its nodeAffinity picks the node and the claims bind to
// that node as the Job schedules. Mounts are rw: extraction writes data.
func BuildRestoreVolumeJob(spec RestoreJobSpec, node string, pvcs []string) *batchv1.Job {
	command := []string{"pbs-agent", "restore-volume", "--ref", spec.Ref}
	volumes := make([]corev1.Volume, 0, len(pvcs)+1)
	mounts := make([]corev1.VolumeMount, 0, len(pvcs)+1)
	for i, pvc := range pvcs {
		name := fmt.Sprintf("pvc-%d", i)
		volumes = append(volumes, corev1.Volume{
			Name: name,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: name, MountPath: "/backup/" + pvc})
		command = append(command,
			"--archive", "pvc-"+pvc+".pxar.didx",
			"--target", "/backup/"+pvc,
		)
	}
	volumes = append(volumes, cacheVolume)
	mounts = append(mounts, cacheMount)
	job := restoreJobBase(spec, command)
	pod := &job.Spec.Template.Spec // points into the job; mutations land
	pod.Volumes = volumes
	pod.Containers[0].VolumeMounts = mounts
	addCacheEnv(pod)
	pod.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      hostnameKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{node},
					}},
				}},
			},
		},
	}
	return job
}
