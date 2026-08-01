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

func requireKeys(t *testing.T, got [][]byte, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("ListKeys() returned %d keys, want %d", len(got), len(want))
	}
	for i, key := range got {
		if string(key) != want[i] {
			t.Fatalf("ListKeys()[%d] = %q, want %q", i, key, want[i])
		}
	}
}

func TestDBListKeys(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	for _, entry := range []struct {
		key   string
		value string
	}{
		{key: "charlie", value: "3"},
		{key: "alpha", value: "1"},
		{key: "bravo", value: "2"},
	} {
		if err := db.Put([]byte(entry.key), []byte(entry.value)); err != nil {
			t.Fatalf("Put(%q) error = %v", entry.key, err)
		}
	}

	requireKeys(t, db.ListKeys(), []string{"alpha", "bravo", "charlie"})

	if err := db.Delete([]byte("bravo")); err != nil {
		t.Fatalf("Delete(%q) error = %v", "bravo", err)
	}
	requireKeys(t, db.ListKeys(), []string{"alpha", "charlie"})
}

type foldEntry struct {
	key   string
	value string
}

func requireFoldEntries(t *testing.T, got, want []foldEntry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("Fold() visited %d entries, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("Fold() entry[%d] = %#v, want %#v", i, got[i], want[i])
		}
	}
}

func TestDBFold(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	for _, entry := range []struct {
		key   string
		value string
	}{
		{key: "charlie", value: "3"},
		{key: "alpha", value: "1"},
		{key: "bravo", value: "2"},
	} {
		if err := db.Put([]byte(entry.key), []byte(entry.value)); err != nil {
			t.Fatalf("Put(%q) error = %v", entry.key, err)
		}
	}
	if err := db.Put([]byte("bravo"), []byte("updated")); err != nil {
		t.Fatalf("updating %q error = %v", "bravo", err)
	}

	var got []foldEntry
	err := db.Fold(func(key, value []byte) bool {
		got = append(got, foldEntry{key: string(key), value: string(value)})
		return true
	})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}

	requireFoldEntries(t, got, []foldEntry{
		{key: "alpha", value: "1"},
		{key: "bravo", value: "updated"},
		{key: "charlie", value: "3"},
	})
}

func TestDBFoldStopsWhenCallbackReturnsFalse(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	for _, key := range []string{"charlie", "alpha", "bravo"} {
		if err := db.Put([]byte(key), []byte(key)); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	var got []string
	err := db.Fold(func(key, value []byte) bool {
		got = append(got, string(key))
		return string(key) != "bravo"
	})
	if err != nil {
		t.Fatalf("Fold() error = %v", err)
	}

	if len(got) != 2 || got[0] != "alpha" || got[1] != "bravo" {
		t.Fatalf("Fold() visited keys = %v, want [alpha bravo]", got)
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
