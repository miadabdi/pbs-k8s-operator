package pbs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testUPID = "UPID:localhost:0000AB12:00000123:0000000000000ABC:backup:ct/100:root@pam:"

func fastPoll(t *testing.T) {
	t.Helper()
	old := pollInterval
	pollInterval = time.Millisecond
	t.Cleanup(func() { pollInterval = old })
}

func taskReply(w http.ResponseWriter, status, exitstatus string) {
	if exitstatus == "" {
		fmt.Fprintf(w, `{"data":{"status":%q}}`, status)
		return
	}
	fmt.Fprintf(w, `{"data":{"status":%q,"exitstatus":%q}}`, status, exitstatus)
}

// Coverage 3a: running -> running -> stopped/OK is success.
func TestWaitTaskSuccess(t *testing.T) {
	fastPoll(t)
	var polls atomic.Int32
	c := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		switch polls.Add(1) {
		case 1, 2:
			taskReply(w, "running", "")
		default:
			taskReply(w, "stopped", "OK")
		}
	})

	exitstatus, err := c.WaitTask(context.Background(), testUPID)
	if err != nil {
		t.Fatalf("WaitTask: %v", err)
	}
	if exitstatus != "OK" {
		t.Errorf("exitstatus = %q, want %q", exitstatus, "OK")
	}
	if n := polls.Load(); n < 3 {
		t.Errorf("polls = %d, want >= 3", n)
	}
}

// Coverage 3b: non-OK exitstatus is an error carrying the exitstatus.
func TestWaitTaskFailure(t *testing.T) {
	fastPoll(t)
	c := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		taskReply(w, "stopped", "backup failed: vm 101 not found")
	})

	exitstatus, err := c.WaitTask(context.Background(), testUPID)
	if err == nil {
		t.Fatal("WaitTask on failed task: want error, got nil")
	}
	if exitstatus != "backup failed: vm 101 not found" {
		t.Errorf("exitstatus = %q, want failure message", exitstatus)
	}
	if !strings.Contains(err.Error(), "backup failed: vm 101 not found") {
		t.Errorf("error %q does not carry exitstatus", err)
	}
}

// Coverage 3c: ctx cancelled mid-poll errors out.
func TestWaitTaskContextCanceled(t *testing.T) {
	fastPoll(t)
	c := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		taskReply(w, "running", "")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := c.WaitTask(ctx, testUPID)
	if err == nil {
		t.Fatal("WaitTask with cancelled ctx: want error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

// Coverage 4: the UPID is percent-encoded in the task-status path. The fake
// 404s unless the raw path is exactly the url.PathEscape form (UPIDs contain
// ':' and '/'; '/' must appear as %2F).
func TestWaitTaskUPIDEncoded(t *testing.T) {
	fastPoll(t)
	c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.EscapedPath()
		want := "/api2/json/nodes/localhost/tasks/" + url.PathEscape(testUPID) + "/status"
		if raw != want {
			http.Error(w, fmt.Sprintf("raw path %q, want %q", raw, want), http.StatusNotFound)
			return
		}
		if !strings.Contains(raw, "%2F") {
			http.Error(w, "raw path lacks %2F escape for '/' in UPID", http.StatusNotFound)
			return
		}
		taskReply(w, "stopped", "OK")
	})

	exitstatus, err := c.WaitTask(context.Background(), testUPID)
	if err != nil {
		t.Fatalf("WaitTask: %v", err)
	}
	if exitstatus != "OK" {
		t.Errorf("exitstatus = %q, want %q", exitstatus, "OK")
	}
}
