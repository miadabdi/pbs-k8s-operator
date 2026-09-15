package pbs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

// Coverage 5a: creating a new namespace issues exactly one POST with the
// right path and body.
func TestEnsureNamespaceCreates(t *testing.T) {
	var posts atomic.Int32
	c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
			http.Error(w, "wrong method", http.StatusBadRequest)
			return
		}
		if want := "/api2/json/admin/datastore/k8s-test/namespace"; r.URL.Path != want {
			t.Errorf("path = %q, want %q", r.URL.Path, want)
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != `{"name":"test-ns"}` {
			t.Errorf("body = %q, want %q", body, `{"name":"test-ns"}`)
		}
		posts.Add(1)
		fmt.Fprint(w, `{"data":null}`)
	})

	if err := c.EnsureNamespace(context.Background(), "k8s-test", "test-ns"); err != nil {
		t.Fatalf("EnsureNamespace: %v", err)
	}
	if n := posts.Load(); n != 1 {
		t.Errorf("POST count = %d, want 1", n)
	}
}

// Coverage 5b: a 400 "already exists" response is tolerated with no error
// and no retry.
func TestEnsureNamespaceAlreadyExists(t *testing.T) {
	var posts atomic.Int32
	c := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		posts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"errors":{"name":"namespace 'test-ns' already exists"}}`)
	})

	if err := c.EnsureNamespace(context.Background(), "k8s-test", "test-ns"); err != nil {
		t.Errorf("EnsureNamespace on already-exists: %v", err)
	}
	if n := posts.Load(); n != 1 {
		t.Errorf("POST count = %d, want 1 (no second POST)", n)
	}
}

// Non-already-exists errors must surface, not be swallowed.
func TestEnsureNamespaceOtherError(t *testing.T) {
	c := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	err := c.EnsureNamespace(context.Background(), "k8s-test", "test-ns")
	if err == nil {
		t.Fatal("EnsureNamespace on 500: want error, got nil")
	}
	if !strings.Contains(err.Error(), "test-ns") {
		t.Errorf("error %q does not name the namespace", err)
	}
}
