package releasetest

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReleaseDryRunPlansPresentModulesWithoutMutation(t *testing.T) {
	repository := repositoryRoot(t)
	moduleFiles := []string{
		"go.mod",
		"transport/valkey/go.mod",
		"adapters/go-command/go.mod",
	}
	before := readFiles(t, repository, moduleFiles)

	command := taskCommand(t, repository, "release:dry-run", "0.99.0")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("release dry run failed: %v\n%s", err, output)
	}

	wantLines := []string{
		"release-tag: v0.99.0",
		"root-requirement: transport/valkey github.com/goliatone/go-messaging@v0.99.0",
		"root-requirement: adapters/go-command github.com/goliatone/go-messaging@v0.99.0",
		"preserved-requirement: adapters/go-command github.com/goliatone/go-command@v0.23.1",
		"module-tag: transport/valkey/v0.99.0",
		"module-tag: adapters/go-command/v0.99.0",
		"atomic-push-ref: HEAD:main",
		"atomic-push-ref: refs/tags/v0.99.0",
		"atomic-push-ref: refs/tags/transport/valkey/v0.99.0",
		"atomic-push-ref: refs/tags/adapters/go-command/v0.99.0",
	}
	for _, want := range wantLines {
		if !bytes.Contains(output, []byte(want)) {
			t.Errorf("release plan missing %q\n%s", want, output)
		}
	}
	if strings.Contains(string(output), "transport/ws/") {
		t.Errorf("release plan included absent module transport/ws\n%s", output)
	}

	after := readFiles(t, repository, moduleFiles)
	for name, contents := range before {
		if !bytes.Equal(contents, after[name]) {
			t.Errorf("release dry run changed %s", name)
		}
	}
}

func TestReleaseDryRunRejectsUnsuffixedV2Modules(t *testing.T) {
	repository := repositoryRoot(t)
	command := taskCommand(t, repository, "release:dry-run", "2.0.0")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("release dry run unexpectedly accepted v2 module paths\n%s", output)
	}
	if !bytes.Contains(output, []byte("Semantic import-version guard")) {
		t.Fatalf("release dry run returned the wrong failure\n%s", output)
	}
}

func TestReleaseDryRunDefaultDoesNotCreateVersionFile(t *testing.T) {
	repository := repositoryRoot(t)
	versionFile := filepath.Join(repository, ".version")
	if _, err := os.Stat(versionFile); err == nil {
		t.Skip("repository already has a version file")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat version file: %v", err)
	}

	command := taskCommand(t, repository, "release:dry-run")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("default release dry run failed: %v\n%s", err, output)
	}
	if _, err := os.Stat(versionFile); !os.IsNotExist(err) {
		t.Fatalf("default release dry run created %s", versionFile)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	return root
}

func taskCommand(t *testing.T, repository string, arguments ...string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, filepath.Join(repository, "taskfile"), arguments...)
	command.Dir = repository
	return command
}

func readFiles(t *testing.T, root string, names []string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte, len(names))
	for _, name := range names {
		contents, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		files[name] = contents
	}
	return files
}
