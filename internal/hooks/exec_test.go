package hooks

import (
	"encoding/json"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func hookedPod(annotations map[string]string, containers ...string) *corev1.Pod {
	if len(containers) == 0 {
		containers = []string{"app", "sidecar"}
	}
	cs := make([]corev1.Container, len(containers))
	for i, name := range containers {
		cs[i] = corev1.Container{Name: name}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", Annotations: annotations},
		Spec:       corev1.PodSpec{Containers: cs},
	}
}

func TestParseHook(t *testing.T) {
	valid := `["pg_dump","-U","postgres","-Fc","-f","/var/lib/postgresql/data/dump.pgc"]`
	for _, tc := range []struct {
		name        string
		annotations map[string]string
		containers  []string
		wantHas     bool
		wantCont    string
		wantCmd     []string
		wantErr     bool
	}{
		{
			name:    "absent annotations -> no hook",
			wantHas: false,
		},
		{
			name:        "empty command annotation -> no hook",
			annotations: map[string]string{AnnotationCommand: ""},
			wantHas:     false,
		},
		{
			name:        "container annotation honored",
			annotations: map[string]string{AnnotationCommand: valid, AnnotationContainer: "sidecar"},
			wantHas:     true,
			wantCont:    "sidecar",
			wantCmd:     []string{"pg_dump", "-U", "postgres", "-Fc", "-f", "/var/lib/postgresql/data/dump.pgc"},
		},
		{
			name:        "no container annotation -> first container",
			annotations: map[string]string{AnnotationCommand: valid},
			wantHas:     true,
			wantCont:    "app",
			wantCmd:     []string{"pg_dump", "-U", "postgres", "-Fc", "-f", "/var/lib/postgresql/data/dump.pgc"},
		},
		{
			name:        "container annotation without command -> no hook",
			annotations: map[string]string{AnnotationContainer: "app"},
			wantHas:     false,
		},
		{
			name:        "malformed JSON -> error",
			annotations: map[string]string{AnnotationCommand: `["pg_dump",`},
			wantHas:     true,
			wantErr:     true,
		},
		{
			name:        "non-string argv element -> error",
			annotations: map[string]string{AnnotationCommand: `["pg_dump",42]`},
			wantHas:     true,
			wantErr:     true,
		},
		{
			name:        "empty array -> error",
			annotations: map[string]string{AnnotationCommand: `[]`},
			wantHas:     true,
			wantErr:     true,
		},
		{
			name:        "JSON object instead of array -> error",
			annotations: map[string]string{AnnotationCommand: `{"cmd":"x"}`},
			wantHas:     true,
			wantErr:     true,
		},
		{
			name:        "default container with no containers at all -> error",
			annotations: map[string]string{AnnotationCommand: valid},
			containers:  []string{}, // and then none
			wantHas:     true,
			wantErr:     true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := hookedPod(tc.annotations, tc.containers...)
			if tc.name == "default container with no containers at all -> error" {
				pod.Spec.Containers = nil
			}
			cont, cmd, has, err := ParseHook(pod)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got container=%q command=%v has=%v", cont, cmd, has)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if has != tc.wantHas {
				t.Fatalf("hasHook = %v, want %v", has, tc.wantHas)
			}
			if tc.wantHas {
				if cont != tc.wantCont {
					t.Fatalf("container = %q, want %q", cont, tc.wantCont)
				}
				if !reflect.DeepEqual(cmd, tc.wantCmd) {
					t.Fatalf("command = %v, want %v", cmd, tc.wantCmd)
				}
			}
		})
	}
}

// The annotation contract is public API (fixture YAML references the keys);
// pin the exact strings so a rename cannot slip in silently.
func TestAnnotationKeys(t *testing.T) {
	want := map[string]string{
		AnnotationContainer: "pbs.backup/pre-hook-container",
		AnnotationCommand:   "pbs.backup/pre-hook-command",
	}
	for k, v := range want {
		if k != v {
			t.Fatalf("annotation key %q drifted from the contract value %q", k, v)
		}
	}
	// The documented example in the brief must parse.
	cont, cmd, has, err := ParseHook(hookedPod(map[string]string{
		AnnotationCommand: `["pg_dump","-U","postgres","-Fc","-f","/var/lib/postgresql/data/dump.pgc"]`,
	}))
	if err != nil || !has || len(cmd) != 6 || cont != "app" {
		b, _ := json.Marshal(cmd)
		t.Fatalf("brief example parse: cont=%q cmd=%s has=%v err=%v", cont, b, has, err)
	}
}
