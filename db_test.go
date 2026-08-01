package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testOptions(t *testing.T) Options {
	t.Helper()

	opts := DefaultOptions
	opts.DirPath = t.TempDir()
	opts.DataFileSize = 64
	opts.SyncWrite = true
	return opts
}

func openTestDB(t *testing.T) (*DB, Options) {
	t.Helper()

	opts := testOptions(t)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return db, opts
}

func closeTestDB(t *testing.T, db *DB) {
	t.Helper()
	if db == nil {
		return
	}

	if db.activeFile != nil {
		if err := db.activeFile.Close(); err != nil {
			t.Errorf("close active data file: %v", err)
		}
	}
	for fid, dataFile := range db.olderFiles {
		if err := dataFile.Close(); err != nil {
			t.Errorf("close data file %d: %v", fid, err)
		}
	}
}

func requireValue(t *testing.T, db *DB, key, want string) {
	t.Helper()

	got, err := db.Get([]byte(key))
	if err != nil {
		t.Fatalf("Get(%q) error = %v", key, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func TestOpenRejectsInvalidOptions(t *testing.T) {
	opts := DefaultOptions
	opts.DirPath = ""
	if _, err := Open(opts); err == nil {
		t.Fatal("Open() with an empty directory should fail")
	}

	opts = DefaultOptions
	opts.DirPath = t.TempDir()
	opts.DataFileSize = 0
	if _, err := Open(opts); err == nil {
		t.Fatal("Open() with a non-positive data-file size should fail")
	}
}

func TestOpenCreatesDirectory(t *testing.T) {
	root := t.TempDir()
	opts := DefaultOptions
	opts.DirPath = filepath.Join(root, "database")
	opts.DataFileSize = 64

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer closeTestDB(t, db)

	info, err := os.Stat(opts.DirPath)
	if err != nil {
		t.Fatalf("stat database directory: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("database path is not a directory: %s", opts.DirPath)
	}
}

func TestDBPutGetDelete(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	if err := db.Put(nil, []byte("value")); !errors.Is(err, ErrKeyIsEmpty) {
		t.Fatalf("Put() with an empty key error = %v, want %v", err, ErrKeyIsEmpty)
	}
	if _, err := db.Get(nil); !errors.Is(err, ErrKeyIsEmpty) {
		t.Fatalf("Get() with an empty key error = %v, want %v", err, ErrKeyIsEmpty)
	}
	if err := db.Delete(nil); !errors.Is(err, ErrKeyIsEmpty) {
		t.Fatalf("Delete() with an empty key error = %v, want %v", err, ErrKeyIsEmpty)
	}

	if _, err := db.Get([]byte("missing")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get() for a missing key error = %v, want %v", err, ErrKeyNotFound)
	}

	if err := db.Put([]byte("name"), []byte("bitcask")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	requireValue(t, db, "name", "bitcask")

	if err := db.Put([]byte("name"), []byte("updated")); err != nil {
		t.Fatalf("overwriting Put() error = %v", err)
	}
	requireValue(t, db, "name", "updated")

	if err := db.Delete([]byte("name")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := db.Get([]byte("name")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get() after Delete() error = %v, want %v", err, ErrKeyNotFound)
	}
	if err := db.Delete([]byte("name")); err != nil {
		t.Fatalf("deleting an already deleted key error = %v", err)
	}
}

func TestDBReopenRecoversLatestRecords(t *testing.T) {
	opts := testOptions(t)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		closeTestDB(t, db)
	}()

	if err := db.Put([]byte("name"), []byte("first")); err != nil {
		t.Fatalf("initial Put() error = %v", err)
	}
	if err := db.Put([]byte("name"), []byte("second")); err != nil {
		t.Fatalf("overwrite Put() error = %v", err)
	}
	if err := db.Put([]byte("keep"), []byte("value")); err != nil {
		t.Fatalf("Put() for keep key error = %v", err)
	}
	if err := db.Put([]byte("gone"), []byte("value")); err != nil {
		t.Fatalf("Put() for deleted key error = %v", err)
	}
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	closeTestDB(t, db)
	db = nil

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen database error = %v", err)
	}

	requireValue(t, db, "name", "second")
	requireValue(t, db, "keep", "value")
	if _, err := db.Get([]byte("gone")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get() for deleted key after reopen error = %v, want %v", err, ErrKeyNotFound)
	}
}

func TestDBRotatesDataFilesAndReadsOlderFiles(t *testing.T) {
	opts := testOptions(t)
	opts.DataFileSize = 32

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		closeTestDB(t, db)
	}()

	values := make(map[string]string)
	for i := range 6 {
		key := fmt.Sprintf("key-%d", i)
		value := strings.Repeat("v", 12)
		values[key] = value
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	if len(db.olderFiles) == 0 {
		t.Fatal("expected data-file rotation to create an older data file")
	}
	closeTestDB(t, db)
	db = nil

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen rotated database error = %v", err)
	}

	for key, value := range values {
		requireValue(t, db, key, value)
	}
}
