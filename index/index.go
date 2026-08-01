package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/yifaaan/bitcask/data"
)

type IndexType = byte

const (
	BTREE IndexType = iota
	ART
)

type Indexer interface {
	// 向索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// 根据key取出对应的数据位置信息
	Get(key []byte) *data.LogRecordPos
	// 根据key删除对应的数据位置信息
	Delete(key []byte) bool
}

// for BTree's item
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// 为*Item实现btree.Item接口
func (i *Item) Less(than btree.Item) bool {
	return bytes.Compare(i.key, than.(*Item).key) == -1
}

func NewIndexer(t IndexType) Indexer {
	switch t {
	case BTREE:
		return NewBTree()
	case ART:
		return nil
	}
	return nil
}
