/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// pbs-agent is the entrypoint of the backup/restore Jobs created by the
// pbs-operator. The backup and restore-volume subcommands wrap
// proxmox-backup-client; apply-manifests shells out to kubectl (logic in
// internal/agent).
package main

import (
	"fmt"
	"os"

	"gitlab.sharifmind.ir/miad/pbs-operator/internal/agent"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: pbs-agent <backup|restore-volume|apply-manifests> [args...]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "backup":
		pvcs, apiDir, notes, termlog, err := agent.ParseBackupArgs(os.Args[2:], os.Getenv)
		if err != nil {
			fmt.Fprintln(os.Stderr, "pbs-agent:", err)
			os.Exit(2)
		}
		os.Exit(agent.RunBackup(agent.BackupDeps{
			Getenv:   os.Getenv,
			Hostname: os.Hostname,
			Environ:  os.Environ,
			Stdout:   os.Stdout,
			Stderr:   os.Stderr,
			Run:      agent.ExecClient,
		}, pvcs, apiDir, notes, termlog))
	case "restore-volume":
		ref, pairs, cm, termlog, err := agent.ParseRestoreVolumeArgs(os.Args[2:], os.Getenv)
		if err != nil {
			fmt.Fprintln(os.Stderr, "pbs-agent:", err)
			os.Exit(2)
		}
		os.Exit(agent.RunRestoreVolume(agent.RestoreDeps{
			Getenv:  os.Getenv,
			Environ: os.Environ,
			Stdout:  os.Stdout,
			Stderr:  os.Stderr,
			Run:     agent.ExecClient,
		}, ref, pairs, cm, termlog))
	case "apply-manifests":
		phase, file, drop, keep, termlog, err := agent.ParseApplyArgs(os.Args[2:], os.Getenv)
		if err != nil {
			fmt.Fprintln(os.Stderr, "pbs-agent:", err)
			os.Exit(2)
		}
		os.Exit(agent.RunApplyManifests(agent.ApplyDeps{
			PodNS:  agent.DefaultPodNS,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
			Run:    agent.ExecClient,
		}, phase, file, drop, keep, termlog))
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q (expected backup, restore-volume or apply-manifests)\n", os.Args[1])
		os.Exit(2)
	}
}
