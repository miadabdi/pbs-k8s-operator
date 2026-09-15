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
// pbs-operator. This scaffold only parses the subcommand; the real backup and
// restore logic (proxmox-backup-client invocation, staging, exit reporting)
// arrives in a later milestone.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: pbs-agent <backup|restore> [args...]")
		os.Exit(2)
	}
	switch cmd := os.Args[1]; cmd {
	case "backup", "restore":
		fmt.Fprintf(os.Stderr, "pbs-agent %s: not implemented\n", cmd)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q (expected backup or restore)\n", cmd)
	}
	os.Exit(2)
}
