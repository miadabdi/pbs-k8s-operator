// Package hooks implements Velero-style pre-backup exec hooks: a command run
// inside a target container (declared via pod annotations) before the backup
// Jobs are created, so the container can quiesce/flush itself to disk first.
package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

// Pod annotations carrying the hook contract (api/v1 stays annotation-only,
// Velero-familiar). Both live on PODS in the backup namespace.
const (
	// AnnotationContainer names the container to exec into; empty/absent means
	// the pod's first container.
	AnnotationContainer = "pbs.backup/pre-hook-container"
	// AnnotationCommand is a JSON array of argv strings, e.g.
	// ["pg_dump","-U","postgres","-Fc","-f","/var/lib/postgresql/data/dump.pgc"].
	AnnotationCommand = "pbs.backup/pre-hook-command"
)

// HookTimeout bounds ONE exec. 120s: generous for an in-container dump of a
// small database, short enough that a wedged hook cannot stall a backup
// forever (a timeout fails the backup like any other exec error).
const HookTimeout = 120 * time.Second

// Executor runs one command in one container of one pod. The seam exists so
// tests can fake exec without a kubelet; err must carry pod/container/exit
// details for the user-facing event.
type Executor interface {
	Exec(ctx context.Context, ns, pod, container string, command []string, timeout time.Duration) error
}

// NewExecutor builds the real SPDY-based executor on the manager's rest.Config.
func NewExecutor(cfg *rest.Config) Executor {
	return &spdyExecutor{cfg: cfg, pods: kubernetes.NewForConfigOrDie(cfg).CoreV1().RESTClient()}
}

type spdyExecutor struct {
	cfg  *rest.Config
	pods rest.Interface
}

// Exec streams the command's stdio over a SPDY exec session. A non-zero exit
// code surfaces as an apiserver error ("command terminated with exit code N"),
// which is wrapped with pod/container context plus a stderr tail.
func (e *spdyExecutor) Exec(ctx context.Context, ns, pod, container string, command []string, timeout time.Duration) error {
	url := e.pods.Post().
		Resource("pods").Namespace(ns).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec).URL()

	exec, err := remotecommand.NewSPDYExecutor(e.cfg, "POST", url)
	if err != nil {
		return fmt.Errorf("pod %s/%s container %s: set up exec: %w", ns, pod, container, err)
	}
	var stdout, stderr bytes.Buffer
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := exec.StreamWithContext(tctx, remotecommand.StreamOptions{
		Stdout: &stdout, Stderr: &stderr,
	}); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			if len(msg) > 512 {
				msg = msg[:512] + "..."
			}
			return fmt.Errorf("pod %s/%s container %s: %v (stderr: %s)", ns, pod, container, err, msg)
		}
		return fmt.Errorf("pod %s/%s container %s: %v", ns, pod, container, err)
	}
	return nil
}

// ParseHook extracts the pre-hook from a pod's annotations. hasHook reports
// whether a hook is requested at all (absent/empty command annotation → no
// hook, no error). err carries a user-fixable annotation problem (malformed
// JSON, non-string argv element, or empty array) — the caller fails the backup
// on it. An absent container annotation resolves to the pod's first container.
func ParseHook(pod *corev1.Pod) (container string, command []string, hasHook bool, err error) {
	raw := pod.Annotations[AnnotationCommand]
	if raw == "" {
		return "", nil, false, nil
	}
	if err := json.Unmarshal([]byte(raw), &command); err != nil {
		return "", nil, true, fmt.Errorf("annotation %s is not a JSON array of strings: %w", AnnotationCommand, err)
	}
	if len(command) == 0 {
		return "", nil, true, fmt.Errorf("annotation %s is an empty array", AnnotationCommand)
	}
	container = pod.Annotations[AnnotationContainer]
	if container == "" {
		if len(pod.Spec.Containers) == 0 {
			return "", nil, true, fmt.Errorf("no container annotation and the pod has no containers")
		}
		container = pod.Spec.Containers[0].Name
	}
	return container, command, true, nil
}
