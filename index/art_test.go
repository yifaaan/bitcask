package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yifaaan/bitcask/data"
)

func TestAdaptiveRadixTree_PutGetDelete(t *testing.T) {
	tree := NewAdaptiveRadixTree()

	assert.True(t, tree.Put(nil, &data.LogRecordPos{Fid: 0, Offset: 1}))
	assert.True(t, tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 2}))
	assert.Equal(t, 2, tree.Size())

	pos := tree.Get(nil)
	require.NotNil(t, pos)
	assert.Equal(t, uint32(0), pos.Fid)
	assert.Equal(t, int64(1), pos.Offset)

	assert.True(t, tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 3}))
	pos = tree.Get([]byte("abc"))
	require.NotNil(t, pos)
	assert.Equal(t, int64(3), pos.Offset)

	assert.True(t, tree.Delete(nil))
	assert.False(t, tree.Delete(nil))
	assert.Nil(t, tree.Get(nil))
	assert.Equal(t, 1, tree.Size())
}

func TestAdaptiveRadixTree_IteratorOrder(t *testing.T) {
	tree := NewAdaptiveRadixTree()
	for i, key := range []string{"b", "a", "d", ""} {
		tree.Put([]byte(key), &data.LogRecordPos{Offset: int64(i)})
	}

	tests := []struct {
		name    string
		reverse bool
		want    []string
	}{
		{name: "forward", want: []string{"", "a", "b", "d"}},
		{name: "reverse", reverse: true, want: []string{"d", "b", "a", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := tree.Iterator(tt.reverse)
			defer it.Close()

			var got []string
			for it.Rewind(); it.Valid(); it.Next() {
				got = append(got, string(it.Key()))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAdaptiveRadixTree_IteratorSeek(t *testing.T) {
	tree := NewAdaptiveRadixTree()
	for _, key := range []string{"b", "a", "d", ""} {
		tree.Put([]byte(key), &data.LogRecordPos{})
	}

	tests := []struct {
		name      string
		reverse   bool
		seek      string
		wantKey   string
		wantValid bool
	}{
		{name: "forward exact", seek: "b", wantKey: "b", wantValid: true},
		{name: "forward next greater", seek: "c", wantKey: "d", wantValid: true},
		{name: "forward empty exact", seek: "", wantKey: "", wantValid: true},
		{name: "forward after last", seek: "z"},
		{name: "reverse exact", reverse: true, seek: "b", wantKey: "b", wantValid: true},
		{name: "reverse next smaller", reverse: true, seek: "c", wantKey: "b", wantValid: true},
		{name: "reverse after largest", reverse: true, seek: "z", wantKey: "d", wantValid: true},
		{name: "reverse empty exact", reverse: true, seek: "", wantKey: "", wantValid: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it := tree.Iterator(tt.reverse)
			defer it.Close()

			it.Seek([]byte(tt.seek))
			if !tt.wantValid {
				assert.False(t, it.Valid())
				assert.Nil(t, it.Key())
				assert.Nil(t, it.Value())
				return
			}

			assert.True(t, it.Valid())
			assert.Equal(t, tt.wantKey, string(it.Key()))
		})
	}
}

func TestAdaptiveRadixTree_IteratorIsSnapshot(t *testing.T) {
	tree := NewAdaptiveRadixTree()
	tree.Put([]byte("a"), &data.LogRecordPos{Offset: 1})

	it := tree.Iterator(false)
	defer it.Close()

	tree.Put([]byte("b"), &data.LogRecordPos{Offset: 2})
	tree.Delete([]byte("a"))

	it.Rewind()
	require.True(t, it.Valid())
	assert.Equal(t, "a", string(it.Key()))
	it.Next()
	assert.False(t, it.Valid())
}

func TestNewIndexerART(t *testing.T) {
	index := NewIndexer(ART)
	require.NotNil(t, index)
	assert.IsType(t, &AdaptiveRadixTree{}, index)
}
