package index

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/data"
)

func TestBPlusTree_Put(t *testing.T) {
	path := "/tmp/bptree_test"
	os.MkdirAll(path, os.ModePerm)
	bpt := NewBPlusTree(path, false)
	defer os.RemoveAll(path)

	res1 := bpt.Put([]byte("hello"), &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.True(t, res1)

	res2 := bpt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBPlusTree_Get(t *testing.T) {
	path := "/tmp/bptree_test"
	os.MkdirAll(path, os.ModePerm)
	bpt := NewBPlusTree(path, false)
	defer os.RemoveAll(path)

	res1 := bpt.Put([]byte("hello"), &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.True(t, res1)

	res2 := bpt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)

	pos1 := bpt.Get([]byte("hello"))
	assert.NotNil(t, pos1)
	assert.Equal(t, uint32(0), pos1.Fid)
	assert.Equal(t, int64(0), pos1.Offset)

	// Test getting a non-existent key, which should return nil.
	pos2 := bpt.Get([]byte("not-exist"))
	assert.Nil(t, pos2)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := "/tmp/bptree_test"
	os.MkdirAll(path, os.ModePerm)
	bpt := NewBPlusTree(path, false)
	defer os.RemoveAll(path)
	res1 := bpt.Put([]byte("hello"), &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bpt.Delete(nil)
	assert.False(t, res2)

	res3 := bpt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)

	res4 := bpt.Delete([]byte("aaa"))
	assert.True(t, res4)
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := "/tmp/bptree_test"
	os.MkdirAll(path, os.ModePerm)
	bpt := NewBPlusTree(path, false)
	defer os.RemoveAll(path)

	// 1.BTree为空
	iter1 := bpt.Iterator(false)
	assert.Equal(t, false, iter1.Valid())
	iter1.Close()

	// 2.BTree有数据
	bpt.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 11})
	iter2 := bpt.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	iter2.Close()

	// 3.多条数据
	bpt.Put([]byte("acee"), &data.LogRecordPos{Fid: 2, Offset: 22})
	bpt.Put([]byte("eede"), &data.LogRecordPos{Fid: 3, Offset: 33})
	bpt.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 4, Offset: 44})
	iter3 := bpt.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		// t.Log("key=", string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}
	iter3.Close()
	iter4 := bpt.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		// t.Log("key=", string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

	iter4.Close()

	iter5 := bpt.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		// t.Log(string(iter5.Key()))
		assert.NotNil(t, iter5.Key())
	}

	iter5.Close()
	// 5.反向Seek
	iter6 := bpt.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		// t.Log(string(iter6.Key()))
		assert.NotNil(t, iter6.Key())
	}

	iter6.Close()
}
