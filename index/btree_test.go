package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yifaaan/bitcask/data"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 0, Offset: 0})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(0), pos1.Fid)
	assert.Equal(t, int64(0), pos1.Offset)

	res2 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	// 覆盖旧值
	res3 := bt.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)
	assert.Equal(t, int64(2), res3.Offset)

	// 获取新值
	pos2 := bt.Get([]byte("abc"))
	t.Log(pos2)
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)

	old, deleted := bt.Delete(nil)
	assert.True(t, deleted)
	assert.Equal(t, int64(100), old.Offset)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)

	old, deleted = bt.Delete([]byte("aaa"))
	assert.True(t, deleted)
	assert.Equal(t, int64(33), old.Offset)
}

func newBTreeForIteratorTest() *BTree {
	bt := NewBTree()
	for i, key := range []string{"b", "a", "d"} {
		bt.Put([]byte(key), &data.LogRecordPos{Offset: int64(i)})
	}
	return bt
}

func TestBTreeIterator_IterationOrder(t *testing.T) {
	tests := []struct {
		name    string
		reverse bool
		want    []string
	}{
		{
			name: "forward",
			want: []string{"a", "b", "d"},
		},
		{
			name:    "reverse",
			reverse: true,
			want:    []string{"d", "b", "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := newBTreeForIteratorTest().Iterator(tt.reverse)
			defer it.Close()

			var got []string
			for it.Rewind(); it.Valid(); it.Next() {
				got = append(got, string(it.Key()))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBTreeIterator_Seek(t *testing.T) {
	tests := []struct {
		name      string
		reverse   bool
		seek      string
		wantKey   string
		wantValid bool
	}{
		{
			name:      "forward exact",
			seek:      "b",
			wantKey:   "b",
			wantValid: true,
		},
		{
			name:      "forward next greater",
			seek:      "c",
			wantKey:   "d",
			wantValid: true,
		},
		{
			name:      "forward before first",
			seek:      "0",
			wantKey:   "a",
			wantValid: true,
		},
		{
			name:    "forward after last",
			seek:    "z",
			wantKey: "",
		},
		{
			name:      "reverse exact",
			reverse:   true,
			seek:      "b",
			wantKey:   "b",
			wantValid: true,
		},
		{
			name:      "reverse next smaller",
			reverse:   true,
			seek:      "c",
			wantKey:   "b",
			wantValid: true,
		},
		{
			name:      "reverse after largest",
			reverse:   true,
			seek:      "z",
			wantKey:   "d",
			wantValid: true,
		},
		{
			name:    "reverse before smallest",
			reverse: true,
			seek:    "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := newBTreeForIteratorTest().Iterator(tt.reverse)
			defer it.Close()

			it.Seek([]byte(tt.seek))
			if !tt.wantValid {
				assert.False(t, it.Valid())
				return
			}

			assert.True(t, it.Valid())
			assert.Equal(t, tt.wantKey, string(it.Key()))
		})
	}
}
