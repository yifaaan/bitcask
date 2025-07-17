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
	// 索引迭代器
	Iterator(reverse bool) Iterator
	// Close 关闭索引
	Close() error
	// 大小
	Size() int
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

// 索引迭代器接口
type Iterator interface {
	// 重新回到迭代器起点
	Rewind()
	// 根据key找到第一个大于（或小于）等于的目标
	Seek(key []byte)
	// 下一个key
	Next()
	// 是否已经遍历完
	Valid() bool
	// 当前位置的key
	Key() []byte
	// 当前位置的pos
	Value() *data.LogRecordPos
	// 关闭
	Close()
}
