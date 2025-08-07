package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/data"
)

func TestArt_Put(t *testing.T) {
	art := NewAdaptiveRadixTree()

	res1 := art.Put(nil, &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.True(t, res1)

	res2 := art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestArt_Get(t *testing.T) {
	art := NewAdaptiveRadixTree()

	res1 := art.Put(nil, &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.True(t, res1)

	pos1 := art.Get(nil)
	assert.Equal(t, uint32(0), pos1.Fid)
	assert.Equal(t, int64(0), pos1.Offset)

	res2 := art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	// 覆盖旧值
	res3 := art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	// 获取新值
	pos2 := art.Get([]byte("abc"))
	t.Log(pos2)
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestArt_Delete(t *testing.T) {
	art := NewAdaptiveRadixTree()
	res1 := art.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := art.Delete(nil)
	assert.False(t, res2)

	res3 := art.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)

	res4 := art.Delete([]byte("aaa"))
	assert.True(t, res4)
}

func TestArt_Iterator(t *testing.T) {
	art1 := NewAdaptiveRadixTree()
	// 1.BTree为空
	iter1 := art1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2.BTree有数据
	art1.Put([]byte("code"), &data.LogRecordPos{Fid: 1, Offset: 11})
	iter2 := art1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	// 3.多条数据
	art1.Put([]byte("acee"), &data.LogRecordPos{Fid: 2, Offset: 22})
	art1.Put([]byte("eede"), &data.LogRecordPos{Fid: 3, Offset: 33})
	art1.Put([]byte("bbcd"), &data.LogRecordPos{Fid: 4, Offset: 44})
	iter3 := art1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key=", string(iter3.Key()))
		assert.NotNil(t, iter3.Key())
	}

	iter4 := art1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		// t.Log("key=", string(iter4.Key()))
		assert.NotNil(t, iter4.Key())
	}

	// 4.Seek
	iter5 := art1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		// t.Log(string(iter5.Key()))
		assert.NotNil(t, iter5.Key())
	}

	// 5.反向Seek
	iter6 := art1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		// t.Log(string(iter6.Key()))
		assert.NotNil(t, iter6.Key())
	}

}
