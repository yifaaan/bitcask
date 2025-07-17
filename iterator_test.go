package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/utils"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-iterator")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-iterator")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.RandomValue(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	assert.Equal(t, utils.GetTestKey(10), iterator.Key())
}

func TestDB_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-iterator")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("abc"), []byte("123"))
	assert.Nil(t, err)
	err = db.Put([]byte("aac"), []byte("12df3"))
	assert.Nil(t, err)
	err = db.Put([]byte("def"), []byte("456"))
	assert.Nil(t, err)
	err = db.Put([]byte("defddd"), []byte("456df"))
	assert.Nil(t, err)
	err = db.Put([]byte("aaaddd"), []byte("456dfdfasd"))
	assert.Nil(t, err)
	err = db.Put([]byte("ghi"), []byte("789"))
	assert.Nil(t, err)

	iter1 := db.NewIterator(DefaultIteratorOptions)
	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log("key=", string(iter1.Key()))
	}
	t.Log("\n")
	iter1.Rewind()
	for iter1.Seek([]byte("abc")); iter1.Valid(); iter1.Next() {
		t.Log("key=", string(iter1.Key()))
	}

	// reverse
	t.Log("\n")
	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		t.Log("key=", string(iter2.Key()))
	}
	t.Log("\n")
	iter1.Rewind()
	for iter1.Seek([]byte("de")); iter1.Valid(); iter1.Next() {
		t.Log("key=", string(iter1.Key()))
	}
	t.Log("\n")

	// prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("a")
	iter3 := db.NewIterator(iterOpts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key=", string(iter3.Key()))
	}
}
