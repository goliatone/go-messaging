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
		"removed-replacement: transport/valkey github.com/goliatone/go-messaging",
		"root-requirement: adapters/go-command github.com/goliatone/go-messaging@v0.99.0",
		"removed-replacement: adapters/go-command github.com/goliatone/go-messaging",
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

func TestModuleSyncUsesRootPathAndRemovesWorkspaceReplacement(t *testing.T) {
	repository := repositoryRoot(t)
	sandbox := t.TempDir()
	taskfile, err := os.ReadFile(filepath.Join(repository, "taskfile"))
	if err != nil {
		t.Fatalf("read taskfile: %v", err)
	}
	writeFile(t, sandbox, "taskfile", taskfile, 0o755)
	writeFile(t, sandbox, "go.mod", []byte("module example.com/messaging/v2\n\ngo 1.23.4\n"), 0o644)

	nested := []byte(`module example.com/nested/v2

go 1.23.4

require (
	github.com/goliatone/go-command v0.23.1
	github.com/goliatone/go-messaging v0.0.0
)

replace github.com/goliatone/go-messaging => ../..
`)
	for _, directory := range []string{"transport/valkey", "adapters/go-command"} {
		writeFile(t, sandbox, filepath.Join(directory, "go.mod"), nested, 0o644)
	}

	command := taskCommand(t, sandbox, "modules:sync", "2.1.0")
	command.Env = append(os.Environ(), "GO_NESTED_MODULES=transport/valkey adapters/go-command")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("module sync failed: %v\n%s", err, output)
	}

	for _, directory := range []string{"transport/valkey", "adapters/go-command"} {
		contents, err := os.ReadFile(filepath.Join(sandbox, directory, "go.mod"))
		if err != nil {
			t.Fatalf("read synchronized %s: %v", directory, err)
		}
		text := string(contents)
		if !strings.Contains(text, "example.com/messaging/v2 v2.1.0") {
			t.Errorf("%s does not require the versioned root:\n%s", directory, text)
		}
		if strings.Contains(text, "replace ") || strings.Contains(text, "github.com/goliatone/go-messaging v0.0.0") {
			t.Errorf("%s retained local root wiring:\n%s", directory, text)
		}
		if !strings.Contains(text, "github.com/goliatone/go-command v0.23.1") {
			t.Errorf("%s changed the go-command requirement:\n%s", directory, text)
		}
	}
}

func TestReleasePreflightRejectsUntrackedManagedOutputs(t *testing.T) {
	for _, name := range []string{".version", "CHANGELOG.md"} {
		t.Run(name, func(t *testing.T) {
			sandbox := releaseSandbox(t)
			writeFile(t, sandbox, name, []byte("stale\n"), 0o644)
			command := taskCommand(t, sandbox, "release:preflight")
			output, err := command.CombinedOutput()
			if err == nil || !bytes.Contains(output, []byte("Untracked release-managed output exists")) {
				t.Fatalf("preflight did not reject %s: err=%v\n%s", name, err, output)
			}
		})
	}
}

func TestReleasePreflightReportsMissingToolAndRemote(t *testing.T) {
	t.Run("git-cliff", func(t *testing.T) {
		sandbox := releaseSandbox(t)
		command := taskCommand(t, sandbox, "release:preflight")
		command.Env = append(os.Environ(), "GIT_CLIFF_BIN=missing-git-cliff-for-test")
		output, err := command.CombinedOutput()
		if err == nil || !bytes.Contains(output, []byte("git-cliff is required")) {
			t.Fatalf("preflight did not report missing git-cliff: err=%v\n%s", err, output)
		}
	})
	t.Run("origin", func(t *testing.T) {
		sandbox := releaseSandbox(t)
		command := taskCommand(t, sandbox, "release:preflight")
		command.Env = append(os.Environ(), "GIT_CLIFF_BIN=/usr/bin/true")
		output, err := command.CombinedOutput()
		if err == nil || !bytes.Contains(output, []byte("remote 'origin' is not configured")) {
			t.Fatalf("preflight did not report missing origin: err=%v\n%s", err, output)
		}
	})
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

func releaseSandbox(t *testing.T) string {
	t.Helper()
	repository := repositoryRoot(t)
	sandbox := t.TempDir()
	taskfile, err := os.ReadFile(filepath.Join(repository, "taskfile"))
	if err != nil {
		t.Fatalf("read taskfile: %v", err)
	}
	writeFile(t, sandbox, "taskfile", taskfile, 0o755)
	runGit(t, sandbox, "init", "-b", "main")
	runGit(t, sandbox, "add", "taskfile")
	runGit(t, sandbox, "-c", "user.name=Release Test", "-c", "user.email=release@example.test", "commit", "-m", "initial")
	return sandbox
}

func runGit(t *testing.T, directory string, arguments ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "git", arguments...)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(arguments, " "), err, output)
	}
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

func writeFile(t *testing.T, root, name string, contents []byte, mode os.FileMode) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create parent for %s: %v", name, err)
	}
	if err := os.WriteFile(path, contents, mode); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}
