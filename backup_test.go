package bitcask

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDBBackupCopiesDatabase(t *testing.T) {
	db, opts := openTestDB(t)
	backupDir := filepath.Join(t.TempDir(), "backup")

	if err := db.Put([]byte("name"), []byte("bitcask")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := db.Backup(backupDir, nil); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	closeTestDB(t, db)

	backupOpts := opts
	backupOpts.DirPath = backupDir
	reopened, err := Open(backupOpts)
	if err != nil {
		t.Fatalf("Open(backup) error = %v", err)
	}
	defer closeTestDB(t, reopened)

	requireValue(t, reopened, "name", "bitcask")
}

func TestDBBackupExcludesDataFiles(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)
	backupDir := filepath.Join(t.TempDir(), "backup")

	if err := db.Put([]byte("name"), []byte("bitcask")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := db.Backup(backupDir, []string{"*.data"}); err != nil {
		t.Fatalf("Backup() with exclusions error = %v", err)
	}

	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatalf("ReadDir(backup) error = %v", err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".data" {
			t.Fatalf("backup contains excluded data file %q", entry.Name())
		}
	}
	if _, err := os.Stat(filepath.Join(backupDir, FILE_LOCK_NAME)); !os.IsNotExist(err) {
		t.Fatalf("DB.Backup() should exclude %q, stat error = %v", FILE_LOCK_NAME, err)
	}
}
