package index

import (
	"bytes"
	"slices"
	"sync"

	"github.com/google/btree"
	"github.com/yifaaan/bitcask/data"
)

// BTree 索引
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: &sync.RWMutex{},
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

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	return newBTreeIterator(bt.tree, reverse)
}

var _ Iterator = (*btreeIterator)(nil)

type btreeIterator struct {
	curIndex int
	reverse  bool
	values   []*Item
}

func newBTreeIterator(bt *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, bt.Len())

	saveValues := func(e btree.Item) bool {
		values[idx] = e.(*Item)
		idx++
		return true
	}

	if reverse {
		bt.Descend(saveValues)
	} else {
		bt.Ascend(saveValues)
	}
	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (it *btreeIterator) Rewind() {
	it.curIndex = 0
}

func (it *btreeIterator) Seek(key []byte) {
	if it.reverse {
		it.curIndex, _ = slices.BinarySearchFunc(it.values, key, func(e *Item, t []byte) int {
			return bytes.Compare(t, e.key)
		})
	} else {
		it.curIndex, _ = slices.BinarySearchFunc(it.values, key, func(e *Item, t []byte) int {
			return bytes.Compare(e.key, t)
		})
	}
}

func (it *btreeIterator) Next() {
	it.curIndex++
}

func (it *btreeIterator) Valid() bool {
	return it.curIndex < len(it.values)
}

func (it *btreeIterator) Key() []byte {
	return it.values[it.curIndex].key
}

func (it *btreeIterator) Value() *data.LogRecordPos {
	return it.values[it.curIndex].pos
}
func (it *btreeIterator) Close() {
	it.values = nil
}
