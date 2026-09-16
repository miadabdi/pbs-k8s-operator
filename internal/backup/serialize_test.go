package backup

import (
	"context"
	"reflect"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	fakedynamic "k8s.io/client-go/dynamic/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"

	pbsv1 "gitlab.sharifmind.ir/miad/pbs-operator/api/v1"
)

// fakeDisco scripts the discovery seam: one APIResourceList per group-version,
// exactly what ServerPreferredNamespacedResources returns.
type fakeDisco struct{ lists []*metav1.APIResourceList }

func (f fakeDisco) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return f.lists, nil
}

func listVerbs() []string { return []string{"get", "list", "watch"} }

func namespaced(name, kind string) metav1.APIResource {
	return metav1.APIResource{Name: name, Kind: kind, Namespaced: true, Verbs: listVerbs()}
}

// testDisco advertises every namespaced kind the fixtures use, including the
// excluded ones (serializer must skip them by GVK, not by absence).
var testDisco = fakeDisco{lists: []*metav1.APIResourceList{
	{GroupVersion: "v1", APIResources: []metav1.APIResource{
		namespaced("configmaps", "ConfigMap"),
		namespaced("events", "Event"),
		namespaced("pods", "Pod"),
		namespaced("secrets", "Secret"),
	}},
	{GroupVersion: "batch/v1", APIResources: []metav1.APIResource{
		namespaced("jobs", "Job"),
	}},
	{GroupVersion: "events.k8s.io/v1", APIResources: []metav1.APIResource{
		namespaced("events", "Event"),
	}},
	{GroupVersion: "pbs.sharifmind.ir/v1", APIResources: []metav1.APIResource{
		namespaced("pbsbackups", "PBSBackup"),
	}},
}}

// volatileMeta stamps every field stripVolatile must remove.
func volatileMeta(name, ns string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:              name,
		Namespace:         ns,
		UID:               types.UID("1111-2222"),
		ResourceVersion:   "42",
		Generation:        3,
		CreationTimestamp: metav1.Now(),
		ManagedFields:     []metav1.ManagedFieldsEntry{{Manager: "kubectl", Operation: "Update"}},
		Annotations: map[string]string{
			"kubectl.kubernetes.io/last-applied-configuration": `{"skip":"me"}`,
			"keep": "me",
		},
	}
}

// fixtures seeds ns "tenant": two keepers (ConfigMap alpha, Pod beta, Secret
// zeta), and four skippables (core Event, events.k8s.io Event, batch Job,
// PBSBackup, managed-labeled Pod). One decoy Secret lives in another ns.
func fixtures() []runtime.Object {
	return []runtime.Object{
		&corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: volatileMeta("zeta", "tenant"),
			Data:       map[string][]byte{"password": []byte("c2VjcmV0")}},
		&corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: volatileMeta("alpha", "tenant"),
			Data:       map[string]string{"k": "v"}},
		&corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: volatileMeta("beta", "tenant"),
			Spec:       corev1.PodSpec{Hostname: "beta"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning}},
		// Managed-labeled pod: operator scaffolding, skipped.
		&corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{Name: "managed-pod", Namespace: "tenant",
				Labels: map[string]string{ManagedLabel: "true"}}},
		&corev1.Event{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Event"},
			ObjectMeta: metav1.ObjectMeta{Name: "e1", Namespace: "tenant"}},
		&eventsv1.Event{TypeMeta: metav1.TypeMeta{APIVersion: "events.k8s.io/v1", Kind: "Event"},
			ObjectMeta: metav1.ObjectMeta{Name: "e2", Namespace: "tenant"}},
		&batchv1.Job{TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
			ObjectMeta: metav1.ObjectMeta{Name: "j1", Namespace: "tenant"}},
		&pbsv1.PBSBackup{TypeMeta: metav1.TypeMeta{APIVersion: "pbs.sharifmind.ir/v1", Kind: "PBSBackup"},
			ObjectMeta: metav1.ObjectMeta{Name: "bk-1", Namespace: "tenant"}},
		// Decoy: same kind, other namespace.
		&corev1.Secret{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{Name: "elsewhere", Namespace: "other"}},
	}
}

func testSerializer(objs ...runtime.Object) *Serializer {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		panic(err)
	}
	if err := pbsv1.AddToScheme(scheme); err != nil {
		panic(err)
	}
	return NewSerializer(fakedynamic.NewSimpleDynamicClient(scheme, objs...), testDisco)
}

// docs splits a multi-document YAML stream on the --- separators.
func docs(t *testing.T, multi string) []map[string]any {
	t.Helper()
	parts := strings.Split(strings.TrimSuffix(multi, "\n"), "\n---\n")
	out := make([]map[string]any, 0, len(parts))
	for _, p := range parts {
		var m map[string]any
		if err := yaml.Unmarshal([]byte(p), &m); err != nil {
			t.Fatalf("doc %q does not parse: %v", p, err)
		}
		out = append(out, m)
	}
	return out
}

func TestSerializeNamespace(t *testing.T) {
	out, err := testSerializer(fixtures()...).SerializeNamespace(context.Background(), "tenant")
	if err != nil {
		t.Fatalf("SerializeNamespace: %v", err)
	}

	got := docs(t, out)
	if len(got) != 3 {
		t.Fatalf("got %d documents, want 3 (ConfigMap, Pod, Secret):\n%s", len(got), out)
	}

	// Deterministic order: GroupKind alphabetical, then name.
	wantOrder := []struct{ kind, name string }{
		{"ConfigMap", "alpha"}, {"Pod", "beta"}, {"Secret", "zeta"},
	}
	for i, w := range wantOrder {
		if got[i]["kind"] != w.kind {
			t.Errorf("doc %d kind = %v, want %s", i, got[i]["kind"], w.kind)
		}
		meta := got[i]["metadata"].(map[string]any)
		if meta["name"] != w.name {
			t.Errorf("doc %d name = %v, want %s", i, meta["name"], w.name)
		}
	}

	// Volatile fields stripped, kept annotations survive.
	for i, m := range got {
		if _, ok := m["status"]; ok {
			t.Errorf("doc %d still has status", i)
		}
		meta := m["metadata"].(map[string]any)
		for _, f := range []string{"uid", "resourceVersion", "creationTimestamp", "generation", "managedFields"} {
			if _, ok := meta[f]; ok {
				t.Errorf("doc %d metadata still has %s", i, f)
			}
		}
		anns, _ := meta["annotations"].(map[string]any)
		if _, ok := anns["kubectl.kubernetes.io/last-applied-configuration"]; ok {
			t.Errorf("doc %d still has last-applied annotation", i)
		}
		if anns["keep"] != "me" {
			t.Errorf("doc %d lost the kept annotation: %v", i, anns)
		}
		if _, ok := meta["namespace"]; !ok {
			t.Errorf("doc %d lost metadata.namespace", i)
		}
	}

	// Secrets and ConfigMaps are included (encrypted server-side by the keyfile;
	// YAML renders []byte data base64-encoded).
	if !strings.Contains(out, "password: YzJWamNtVjA=") {
		t.Errorf("secret data missing from output:\n%s", out)
	}

	// Exclusions: events (both APIs), Jobs, own CRs, managed-labeled objects,
	// and other namespaces.
	for _, absent := range []string{"kind: Event", "kind: Job", "kind: PBSBackup", "managed-pod", "elsewhere"} {
		if strings.Contains(out, absent) {
			t.Errorf("output contains %q:\n%s", absent, out)
		}
	}
}

func TestSerializeNamespaceEmpty(t *testing.T) {
	out, err := testSerializer().SerializeNamespace(context.Background(), "tenant")
	if err != nil {
		t.Fatalf("SerializeNamespace on empty ns: %v", err)
	}
	if out != "" {
		t.Errorf("empty namespace = %q, want empty string", out)
	}
}

func TestStripVolatile(t *testing.T) {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"name":              "p",
			"uid":               "x",
			"resourceVersion":   "1",
			"creationTimestamp": "now",
			"generation":        int64(2),
			"managedFields":     []any{"junk"},
			"annotations":       map[string]any{"kubectl.kubernetes.io/last-applied-configuration": "{}"},
		},
		"status": map[string]any{"phase": "Running"},
	}}
	stripVolatile(u.Object)
	want := map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{"name": "p"},
	}
	if !reflect.DeepEqual(u.Object, want) {
		t.Errorf("stripVolatile = %#v\nwant %#v", u.Object, want)
	}
}
