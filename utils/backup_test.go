package utils

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupCopiesDirectoryAndExcludesFiles(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "backup")

	writeBackupTestFile(t, srcDir, "000000000.data", "active")
	writeBackupTestFile(t, srcDir, "000000001.data", "stale")
	writeBackupTestFile(t, srcDir, "hint-index", "hint")
	writeBackupTestFile(t, srcDir, "flock", "")
	writeBackupTestFile(t, srcDir, "meta/checkpoint", "checkpoint")
	if err := os.Mkdir(filepath.Join(srcDir, "empty"), 0o755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	err := Backup(srcDir, dstDir, []string{"*.data", "checkpoint"})
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}

	assertBackupTestFile(t, dstDir, "hint-index", "hint")
	assertBackupTestFile(t, dstDir, "flock", "")
	assertBackupTestPathAbsent(t, dstDir, "000000000.data")
	assertBackupTestPathAbsent(t, dstDir, "000000001.data")
	assertBackupTestPathAbsent(t, dstDir, "meta/checkpoint")

	info, err := os.Stat(filepath.Join(dstDir, "empty"))
	if err != nil {
		t.Fatalf("Stat(empty directory) error = %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("backup empty path is not a directory: %s", info.Name())
	}
}

func TestBackupAllowsEmptyDestinationDirectory(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeBackupTestFile(t, srcDir, "000000000.data", "data")

	if err := Backup(srcDir, dstDir, nil); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	assertBackupTestFile(t, dstDir, "000000000.data", "data")
}

func TestBackupOverwritesExistingFiles(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	writeBackupTestFile(t, srcDir, "000000000.data", "data")
	writeBackupTestFile(t, dstDir, "000000000.data", "old")

	if err := Backup(srcDir, dstDir, nil); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	assertBackupTestFile(t, dstDir, "000000000.data", "data")
}

func TestBackupRejectsInvalidExclusionPattern(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := filepath.Join(t.TempDir(), "backup")
	writeBackupTestFile(t, srcDir, "000000000.data", "data")

	if err := Backup(srcDir, dstDir, []string{"["}); err == nil {
		t.Fatal("Backup() should reject an invalid exclusion pattern")
	}
	if _, err := os.Stat(dstDir); err != nil {
		t.Fatalf("destination should have been created before walking source, stat error = %v", err)
	}
}

func writeBackupTestFile(t *testing.T, root, name, content string) {
	t.Helper()

	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func assertBackupTestFile(t *testing.T, root, name, want string) {
	t.Helper()

	path := filepath.Join(root, filepath.FromSlash(name))
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("ReadFile(%q) = %q, want %q", path, got, want)
	}
}

func assertBackupTestPathAbsent(t *testing.T, root, name string) {
	t.Helper()

	path := filepath.Join(root, filepath.FromSlash(name))
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("path %q should be absent, stat error = %v", path, err)
	}
}
