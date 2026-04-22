package index

import (
	"bitcask/data"
	"sync"

	"github.com/google/btree"
)

// BTree 内存索引
type BTree struct {
	tree *btree.BTree
	lock sync.Mutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: sync.Mutex{},
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	item := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(item)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	item := &Item{key: key}
	if t := bt.tree.Get(item); t != nil {
		return t.(*Item).pos
	}
	return nil
}

func (bt *BTree) Delete(key []byte) bool {
	item := &Item{key: key}
	bt.lock.Lock()
	old := bt.tree.Delete(item)
	bt.lock.Unlock()
	return old != nil
}
