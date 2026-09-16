package backup

import (
	"strings"
	"testing"
)

// apiYAML mirrors what serialize.go writes: multi-doc, "---"-separated, docs
// carrying their source namespace. Includes one doc of every bucket class.
const apiYAML = `apiVersion: v1
kind: Namespace
metadata:
  name: pg
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: probes.examples.sharifmind.ir
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: pg-config
  namespace: pg
data:
  app.conf: mode=test
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-pg-0
  namespace: pg
  annotations:
    pv.kubernetes.io/bind-completed: "yes"
    pv.kubernetes.io/bound-by-controller: "yes"
    volume.kubernetes.io/selected-node: k8s-ctl1
    volume.beta.kubernetes.io/storage-provisioner: rancher.io/local-path
spec:
  accessModes: [ReadWriteOnce]
  volumeName: pvc-fc86d1d7-3368-4e1a-8556-fea26f679428
---
apiVersion: v1
kind: Secret
metadata:
  name: pg-secret
  namespace: pg
stringData:
  api-key: testenv
---
apiVersion: v1
kind: Service
metadata:
  name: pg
  namespace: pg
spec:
  type: NodePort
  clusterIP: 10.233.16.82
  clusterIPs: [10.233.16.82]
  loadBalancerIP: 192.0.2.50
  healthCheckNodePort: 31000
  ports:
    - port: 5432
      nodePort: 30432
---
apiVersion: v1
kind: Endpoints
metadata:
  name: pg
  namespace: pg
---
apiVersion: apps/v1
kind: ControllerRevision
metadata:
  name: pg-69d56bbd5f
  namespace: pg
---
apiVersion: v1
kind: Pod
metadata:
  name: pg-0
  namespace: pg
---
apiVersion: metrics.k8s.io/v1beta1
kind: PodMetrics
metadata:
  name: pg-0
  namespace: pg
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: default
  namespace: pg
---
apiVersion: examples.sharifmind.ir/v1
kind: Probe
metadata:
  name: sample-probe
  namespace: pg
spec:
  message: hello
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: crdapp
  namespace: pg
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: k8s-node1
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: crdapp-data
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: pg
  namespace: pg
spec:
  template:
    spec:
      nodeSelector:
        kubernetes.io/hostname: k8s-ctl1
  volumeClaimTemplates:
    - metadata:
        name: data
`

// ParseAPIManifests must split the multi-doc payload, read kind/name/namespace
// from every doc, and rewrite namespaced docs' namespace to the target while
// leaving the cluster-scoped Namespace and CRD docs untouched.
func TestParseAPIManifests(t *testing.T) {
	docs, err := ParseAPIManifests([]byte(apiYAML), "restore-test")
	if err != nil {
		t.Fatalf("ParseAPIManifests: %v", err)
	}
	if len(docs) != 14 {
		t.Fatalf("parsed %d docs, want 14", len(docs))
	}
	type k struct{ kind, ns, name string }
	want := []k{
		{"Namespace", "", "pg"},
		{"CustomResourceDefinition", "", "probes.examples.sharifmind.ir"},
		{"ConfigMap", "restore-test", "pg-config"},
		{"PersistentVolumeClaim", "restore-test", "data-pg-0"},
		{"Secret", "restore-test", "pg-secret"},
		{"Service", "restore-test", "pg"},
		{"Endpoints", "restore-test", "pg"},
		{"ControllerRevision", "restore-test", "pg-69d56bbd5f"},
		{"Pod", "restore-test", "pg-0"},
		{"PodMetrics", "restore-test", "pg-0"},
		{"ServiceAccount", "restore-test", "default"},
		{"Probe", "restore-test", "sample-probe"},
		{"Deployment", "restore-test", "crdapp"},
		{"StatefulSet", "restore-test", "pg"},
	}
	for i, w := range want {
		d := docs[i]
		if d.Kind != w.kind || d.Namespace != w.ns || d.Name != w.name {
			t.Errorf("doc %d = %s/%s/%s, want %s", i, d.Kind, d.Namespace, d.Name, w)
		}
	}
	// The rewritten namespace must be IN the object (the rendered YAML carries it).
	if got := docs[2].Obj["metadata"].(map[string]any)["namespace"]; got != "restore-test" {
		t.Errorf("rewritten namespace = %v, want restore-test", got)
	}
	if _, has := docs[0].Obj["metadata"].(map[string]any)["namespace"]; has {
		t.Error("Namespace doc must keep no namespace rewrite")
	}
	// The Service's allocated clusterIP/clusterIPs are stripped: the source
	// namespace owns them, applying them into the target fails allocation.
	svc := docs[5].Obj["spec"].(map[string]any)
	if _, has := svc["clusterIP"]; has {
		t.Error("Service clusterIP not stripped")
	}
	if _, has := svc["clusterIPs"]; has {
		t.Error("Service clusterIPs not stripped")
	}
	// NodePort/LoadBalancer allocations are source-owned too: node ports
	// pin host ports cluster-wide, the LB IP and health-check node port
	// belong to the source's allocator. All stripped; the port itself stays.
	if _, has := svc["loadBalancerIP"]; has {
		t.Error("Service loadBalancerIP not stripped")
	}
	if _, has := svc["healthCheckNodePort"]; has {
		t.Error("Service healthCheckNodePort not stripped")
	}
	port := svc["ports"].([]any)[0].(map[string]any)
	if _, has := port["nodePort"]; has {
		t.Error("Service port nodePort not stripped")
	}
	if port["port"] != float64(5432) { // JSON round-trip numbers are float64
		t.Errorf("Service port mangled: %v", port)
	}
	// The PVC's source volumeName is stripped: the PV belongs to the source
	// claim; naming it in the target leaves the restored claim Lost. The
	// bind annotations go with it (they desync WFFC); the provisioner
	// annotation stays — the provisioner needs it.
	pvcSpec := docs[3].Obj["spec"].(map[string]any)
	if _, has := pvcSpec["volumeName"]; has {
		t.Error("PVC volumeName not stripped")
	}
	pvcAnns := docs[3].Obj["metadata"].(map[string]any)["annotations"].(map[string]any)
	for _, k := range []string{
		"pv.kubernetes.io/bind-completed",
		"pv.kubernetes.io/bound-by-controller",
		"volume.kubernetes.io/selected-node",
	} {
		if _, has := pvcAnns[k]; has {
			t.Errorf("PVC bind annotation %s not stripped", k)
		}
	}
	if _, has := pvcAnns["volume.beta.kubernetes.io/storage-provisioner"]; !has {
		t.Error("PVC provisioner annotation stripped, want kept")
	}
}

// An unparseable doc is an error naming it (doc index + snippet), never a
// silent skip: restoring half a namespace must fail loudly.
func TestParseAPIManifestsBadDoc(t *testing.T) {
	bad := "kind: ConfigMap\nmetadata:\n  name: ok\n  namespace: pg\n---\n::: not yaml [\n"
	_, err := ParseAPIManifests([]byte(bad), "t")
	if err == nil || !strings.Contains(err.Error(), "doc 1") {
		t.Fatalf("err = %v, want an error naming doc 1", err)
	}

	noKind := "metadata:\n  name: x\n"
	_, err = ParseAPIManifests([]byte(noKind), "t")
	if err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("err = %v, want an error about the missing kind", err)
	}
}

// Empty separators (trailing "---", blank docs) are skipped, not errors.
func TestParseAPIManifestsEmptyDocs(t *testing.T) {
	docs, err := ParseAPIManifests([]byte("---\nkind: ConfigMap\nmetadata:\n  name: a\n---\n---\n"), "t")
	if err != nil {
		t.Fatalf("ParseAPIManifests: %v", err)
	}
	if len(docs) != 1 || docs[0].Name != "a" {
		t.Fatalf("docs = %v, want exactly the one ConfigMap", docs)
	}
}

func kinds(docs []PlannedDoc) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = d.Kind
	}
	return out
}

// BucketDocs splits in source order: Namespace docs, CRDs, PreVolume
// (non-workload: PVC, Secret, ConfigMap, Service, ServiceAccount, CRs),
// Workload (Deployment/StatefulSet/...).
func TestBucketDocs(t *testing.T) {
	docs, err := ParseAPIManifests([]byte(apiYAML), "t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ns, crds, pre, work := BucketDocs(docs, nil, nil)
	if got := kinds(ns); len(got) != 1 || got[0] != "Namespace" {
		t.Errorf("ns bucket = %v", got)
	}
	if got := kinds(crds); len(got) != 1 || got[0] != "CustomResourceDefinition" {
		t.Errorf("crd bucket = %v", got)
	}
	if got := kinds(pre); strings.Join(got, ",") != "ConfigMap,PersistentVolumeClaim,Secret,Service,ServiceAccount,Probe" {
		t.Errorf("pre bucket = %v", got)
	}
	// Bare Pods, Endpoints/EndpointSlice and PodMetrics never restore
	// (derived / read-only state).
	all2 := append(append(ns, crds...), append(pre, work...)...)
	for _, d := range all2 {
		if droppedKinds[d.Kind] {
			t.Errorf("derived kind %s restored", d.Kind)
		}
	}
	if got := kinds(work); strings.Join(got, ",") != "Deployment,StatefulSet" {
		t.Errorf("work bucket = %v", got)
	}
	// PlannedDoc carries renderable YAML with the rewritten namespace.
	all := append(append(append(append([]PlannedDoc{}, ns...), crds...), pre...), work...)
	for _, d := range all {
		if len(d.YAML) == 0 {
			t.Fatalf("doc %s/%s has no YAML", d.Kind, d.Name)
		}
	}
	if !strings.Contains(string(pre[0].YAML), "namespace: t") {
		t.Errorf("rendered pre doc lacks rewritten namespace:\n%s", pre[0].YAML)
	}
}

// DropKinds removes kinds from BOTH api buckets; KeepKinds rescues a kind
// named by both lists (Keep wins over Drop).
func TestBucketDocsDropKeep(t *testing.T) {
	docs, _ := ParseAPIManifests([]byte(apiYAML), "t")
	_, _, pre, _ := BucketDocs(docs, []string{"Secret", "Probe"}, nil)
	if got := strings.Join(kinds(pre), ","); strings.Contains(got, "Secret") || strings.Contains(got, "Probe") {
		t.Errorf("dropped kinds leaked: %v", got)
	}
	// Keep wins: Probe survives, Secret still dropped.
	_, _, pre, _ = BucketDocs(docs, []string{"Secret", "Probe"}, []string{"Probe"})
	if got := strings.Join(kinds(pre), ","); strings.Contains(got, "Secret") || !strings.Contains(got, "Probe") {
		t.Errorf("keep-wins rule broken: %v", got)
	}
	// Drop applies to the workload bucket too.
	_, _, _, work := BucketDocs(docs, []string{"StatefulSet"}, nil)
	if got := strings.Join(kinds(work), ","); got != "Deployment" {
		t.Errorf("workload drop broken: %v", got)
	}
}

// PVCsFromDocs lists the PVC names from the PreVolume bucket, order kept.
func TestPVCsFromDocs(t *testing.T) {
	docs, _ := ParseAPIManifests([]byte(apiYAML), "t")
	_, _, pre, _ := BucketDocs(docs, nil, nil)
	if got := PVCsFromDocs(pre); strings.Join(got, ",") != "data-pg-0" {
		t.Errorf("PVCsFromDocs = %v, want [data-pg-0]", got)
	}
}

// NodeForPVC maps a PVC to the kubernetes.io/hostname its consuming workload
// pins via pod-template nodeSelector: direct volume refs (Deployment) and
// StatefulSet claim-template names (<tmpl>-<sts>-<ordinal>). Unknown → "".
func TestNodeForPVC(t *testing.T) {
	docs, _ := ParseAPIManifests([]byte(apiYAML), "t")
	if got := NodeForPVC(docs, "crdapp-data"); got != "k8s-node1" {
		t.Errorf("Deployment direct ref: NodeForPVC = %q, want k8s-node1", got)
	}
	if got := NodeForPVC(docs, "data-pg-0"); got != "k8s-ctl1" {
		t.Errorf("STS claim template: NodeForPVC = %q, want k8s-ctl1", got)
	}
	if got := NodeForPVC(docs, "no-such"); got != "" {
		t.Errorf("unknown PVC: NodeForPVC = %q, want \"\"", got)
	}
}
