package backup

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// BackupJobSpec carries everything needed to build a backup Job.
type BackupJobSpec struct {
	Name           string // PBSBackup CR name; also pod hostname → PBS backup-id
	Namespace      string // PBSBackup CR namespace
	Node           string // from ResolveNode
	RepoSecret     string // name of the PBSRepo secret (testenv contract keys)
	PVCs           []string
	Image          string
	ServiceAccount string // optional; empty → default
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
//   - env (from RepoSecret keys — EXACT testenv contract, via secretKeyRef;
//     the Job does NOT inline secret data)
//   - container command: ["pbs-agent","backup","--pvc","<pvc1>","--pvc","<pvc2>",...]
//     (agent resolves /backup/<pvc> mounts; arg contract for the agent task)
//   - imagePullPolicy: IfNotPresent
func BuildBackupJob(spec BackupJobSpec) *batchv1.Job {
	volumes := make([]corev1.Volume, len(spec.PVCs))
	mounts := make([]corev1.VolumeMount, len(spec.PVCs))
	command := []string{"pbs-agent", "backup"}
	for i, pvc := range spec.PVCs {
		volumes[i] = corev1.Volume{
			Name: fmt.Sprintf("pvc-%d", i),
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc, ReadOnly: true},
			},
		}
		mounts[i] = corev1.VolumeMount{Name: volumes[i].Name, MountPath: "/backup/" + pvc, ReadOnly: true}
		command = append(command, "--pvc", pvc)
	}

	env := make([]corev1.EnvVar, len(envContract))
	for i, c := range envContract {
		env[i] = corev1.EnvVar{
			Name: c.env,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: spec.RepoSecret},
					Key:                  c.key,
				},
			},
		}
	}

	labels := map[string]string{
		"app.kubernetes.io/managed-by": "pbs-operator",
		"pbsbackup":                    spec.Name,
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
