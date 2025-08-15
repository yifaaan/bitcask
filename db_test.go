package bitcask

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/utils"
)

func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	// dir, _ := filepath.Join(os.TempDir(), "bitcask")
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	ops := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-put")
	// t.Log("dir: ", dir)
	defer os.RemoveAll(dir)
	ops.DirPath = dir
	ops.DataFileSize = 4 * 1024
	ops.IndexType = BTree
	db, err := Open(ops)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	val := utils.RandomValue(22)
	err = db.Put(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	// 2.重复 Put key 相同的数据
	val = utils.RandomValue(33)
	err = db.Put(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(44))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(11), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(11))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.写到数据文件进行了转换
	for i := 0; len(db.olderFiles) < 2; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启
	db2, err := Open(ops)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	// t.Log("val4: ", string(val4))
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	// t.Log("val5: ", string(val5))
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	ops := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-put")
	// t.Log("dir: ", dir)
	defer os.RemoveAll(dir)
	ops.DirPath = dir
	ops.DataFileSize = 4 * 1024
	ops.IndexType = BTree
	db, err := Open(ops)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	val := utils.RandomValue(22)
	err = db.Put(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.重复 Put key 相同的数据
	val = utils.RandomValue(33)
	err = db.Put(utils.GetTestKey(1), val)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, val, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	for i := 100; len(db.olderFiles) < 2; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// close db
	err = db.Close()
	assert.Nil(t, err)

	// 重启
	db2, err := Open(ops)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val6, err := db2.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val3, val6)

	val7, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val7))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-delete")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-ListKeys")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 空数据库
	keys1 := db.ListKeys()
	assert.Equal(t, 0, len(keys1))

	// 一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	keys2 := db.ListKeys()
	assert.Equal(t, 1, len(keys2))

	// 多条数据
	err = db.Put(utils.GetTestKey(11234), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(134), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(153), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(1534), utils.RandomValue(20))
	assert.Nil(t, err)
	keys3 := db.ListKeys()
	assert.Equal(t, 5, len(keys3))
}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-Fold")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11234), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(134), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(153), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(1534), utils.RandomValue(20))
	assert.Nil(t, err)
	keys3 := db.ListKeys()
	assert.Equal(t, 4, len(keys3))

	err = db.Fold(func(key, value []byte) bool {
		// t.Log(string(key))
		// t.Log(string(value))
		return !bytes.Equal(key, utils.GetTestKey(153))
	})
	assert.Nil(t, err)
}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-test-FileLock")
	defer os.RemoveAll(dir)
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(opts)
	assert.Equal(t, ErrDatabaseIsInUsing, err)
}
