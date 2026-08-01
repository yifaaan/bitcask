package bitcask

import (
	"errors"
	"testing"
)

func newTestWriteBatch(db *DB, maxBatchNum uint) *WriteBatch {
	return db.NewWriteBatch(WriteBatchOptions{
		MaxBatchNum: maxBatchNum,
		SyncWrite:   true,
	})
}

func TestWriteBatchCommit(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	if err := db.Put([]byte("existing"), []byte("old")); err != nil {
		t.Fatalf("Put(%q) error = %v", "existing", err)
	}

	batch := newTestWriteBatch(db, 3)
	if err := batch.Put([]byte("alpha"), []byte("1")); err != nil {
		t.Fatalf("batch.Put(%q) error = %v", "alpha", err)
	}
	if err := batch.Put([]byte("bravo"), []byte("2")); err != nil {
		t.Fatalf("batch.Put(%q) error = %v", "bravo", err)
	}
	if err := batch.Delete([]byte("existing")); err != nil {
		t.Fatalf("batch.Delete(%q) error = %v", "existing", err)
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit() error = %v", err)
	}
	requireValue(t, db, "alpha", "1")
	requireValue(t, db, "bravo", "2")
	if _, err := db.Get([]byte("existing")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(%q) error = %v, want %v", "existing", err, ErrKeyNotFound)
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("empty batch.Commit() error = %v", err)
	}
}

func TestWriteBatchRepeatedPutUsesLatestValue(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	batch := newTestWriteBatch(db, 1)
	if err := batch.Put([]byte("key"), []byte("first")); err != nil {
		t.Fatalf("first batch.Put() error = %v", err)
	}
	if err := batch.Put([]byte("key"), []byte("second")); err != nil {
		t.Fatalf("second batch.Put() error = %v", err)
	}

	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit() error = %v", err)
	}
	requireValue(t, db, "key", "second")
}

func TestWriteBatchDeletePendingPut(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	batch := newTestWriteBatch(db, 1)
	if err := batch.Put([]byte("temporary"), []byte("value")); err != nil {
		t.Fatalf("batch.Put() error = %v", err)
	}
	if err := batch.Delete([]byte("temporary")); err != nil {
		t.Fatalf("batch.Delete() error = %v", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit() error = %v", err)
	}

	if _, err := db.Get([]byte("temporary")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(%q) error = %v, want %v", "temporary", err, ErrKeyNotFound)
	}
}

func TestWriteBatchRejectsEmptyKeys(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	batch := newTestWriteBatch(db, 1)
	if err := batch.Put(nil, []byte("value")); !errors.Is(err, ErrKeyIsEmpty) {
		t.Fatalf("batch.Put() error = %v, want %v", err, ErrKeyIsEmpty)
	}
	if err := batch.Delete(nil); !errors.Is(err, ErrKeyIsEmpty) {
		t.Fatalf("batch.Delete() error = %v, want %v", err, ErrKeyIsEmpty)
	}
}

func TestWriteBatchRejectsExcessEntries(t *testing.T) {
	db, _ := openTestDB(t)
	defer closeTestDB(t, db)

	batch := newTestWriteBatch(db, 1)
	if err := batch.Put([]byte("alpha"), []byte("1")); err != nil {
		t.Fatalf("batch.Put(%q) error = %v", "alpha", err)
	}
	if err := batch.Put([]byte("bravo"), []byte("2")); err != nil {
		t.Fatalf("batch.Put(%q) error = %v", "bravo", err)
	}

	if err := batch.Commit(); !errors.Is(err, ErrExceedMaxBatchNum) {
		t.Fatalf("batch.Commit() error = %v, want %v", err, ErrExceedMaxBatchNum)
	}
	if _, err := db.Get([]byte("alpha")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(%q) error = %v, want %v", "alpha", err, ErrKeyNotFound)
	}
	if _, err := db.Get([]byte("bravo")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(%q) error = %v, want %v", "bravo", err, ErrKeyNotFound)
	}
}

func TestWriteBatchRecoversAfterReopen(t *testing.T) {
	opts := testOptions(t)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	batch := newTestWriteBatch(db, 2)
	if err := batch.Put([]byte("alpha"), []byte("1")); err != nil {
		t.Fatalf("batch.Put(%q) error = %v", "alpha", err)
	}
	if err := batch.Put([]byte("bravo"), []byte("2")); err != nil {
		t.Fatalf("batch.Put(%q) error = %v", "bravo", err)
	}
	if err := batch.Commit(); err != nil {
		t.Fatalf("batch.Commit() error = %v", err)
	}

	closeTestDB(t, db)
	db = nil

	db, err = Open(opts)
	if err != nil {
		t.Fatalf("reopen database error = %v", err)
	}
	defer closeTestDB(t, db)

	requireValue(t, db, "alpha", "1")
	requireValue(t, db, "bravo", "2")
}
