package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/data"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.True(t, res1)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(0), pos1.Fid)
	assert.Equal(t, int64(0), pos1.Offset)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.True(t, res2)
	// 覆盖旧值
	res3 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.True(t, res3)

	// 获取新值
	pos2 := bt.Get([]byte("abc"))
	t.Log(pos2)
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res1)

	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.True(t, res3)

	res4 := bt.Delete([]byte("aaa"))
	assert.True(t, res4)
}
