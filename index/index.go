package index

import (
	"bytes"

	"github.com/google/btree"
	"github.com/yifaaan/bitcask/data"
)

type Indexer interface {
	// 向索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool
	// 根据key取出对应的数据位置信息
	Get(key []byte) *data.LogRecordPos
	// 根据key删除对应的数据位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// Btree索引
	Btree IndexType = iota + 1

	// 自适应基数树
	ART
)

func NewIndexer(t IndexType) Indexer {
	switch t {
	case Btree:
		return NewBTree()
	case ART:
		// todo
		return nil
	default:
		panic("unsupported index type")
	}
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
