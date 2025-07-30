package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/utils"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-write-batch1")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(11))
	assert.Nil(t, err)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(11))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrKeyNotFound, err)
	assert.Nil(t, val)

}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-write-batch2")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(11))
	assert.Nil(t, err)
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(11))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(3), utils.RandomValue(111))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)
	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.NotNil(t, err)

	assert.Equal(t, uint64(2), db2.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	// opts := DefaultOptions
	// // dir, _ := os.MkdirTemp("", "bitcask-test-write-batch3")
	// dir := "/tmp/bitcask-test-write-batch3"
	// // defer os.RemoveAll(dir)
	// opts.DirPath = dir
	// opts.DataFileSize = 64 * 1024 * 1024
	// db, err := Open(opts)
	// // defer destroyDB(db)
	// assert.Nil(t, err)
	// assert.NotNil(t, db)

	// wbOpts := DefaultWriteBatchOptions
	// wbOpts.MaxBatchNum = 1000000
	// wb := db.NewWriteBatch(wbOpts)

	// for i := 0; i < 500000; i++ {
	// 	err = wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	// 	assert.Nil(t, err)
	// }
	// err = wb.Commit()
	// assert.Nil(t, err)

	// err = db.Close()
	// assert.Nil(t, err)
}
