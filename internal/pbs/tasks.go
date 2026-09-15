package pbs

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// pollInterval is the gap between task-status polls. A var so tests can
// shorten it.
var pollInterval = time.Second

type taskStatus struct {
	Status     string `json:"status"`
	ExitStatus string `json:"exitstatus"`
}

// WaitTask polls the status of an async task (PBS POSTs return
// {"data":"UPID:..."}) until it leaves the running state. It returns
// ("OK", nil) iff the task's exitstatus is "OK"; any other exitstatus is
// returned alongside an error. The caller's ctx bounds the total wait.
func (c *Client) WaitTask(ctx context.Context, upid string) (exitstatus string, err error) {
	path := "/api2/json/nodes/localhost/tasks/" + url.PathEscape(upid) + "/status"
	for {
		var ts taskStatus
		if err := c.do(ctx, http.MethodGet, path, nil, &ts); err != nil {
			return "", err
		}
		if ts.Status != "running" {
			if ts.ExitStatus == "OK" {
				return "OK", nil
			}
			return ts.ExitStatus, fmt.Errorf("pbs: task %s failed: %s", upid, ts.ExitStatus)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}
