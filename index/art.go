package index

import (
	"bytes"
	"slices"
	"sync"

	art "github.com/plar/go-adaptive-radix-tree/v2"
	"github.com/yifaaan/bitcask/data"
)

// AdaptiveRadixTree is an ART-backed in-memory index.
type AdaptiveRadixTree struct {
	tree        art.Tree
	mu          sync.RWMutex
	emptyPos    *data.LogRecordPos
	hasEmptyKey bool
}

func NewAdaptiveRadixTree() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{tree: art.New()}
}

// Put stores the position associated with key, replacing any existing value.
func (t *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(key) == 0 {
		t.emptyPos = pos
		t.hasEmptyKey = true
		return nil
	}

	old, _ := t.tree.Insert(key, pos)
	if old == nil {
		return nil
	}
	return old.(*data.LogRecordPos)
}

// Get returns the position associated with key, or nil when key is absent.
func (t *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(key) == 0 {
		if !t.hasEmptyKey {
			return nil
		}
		return t.emptyPos
	}

	if t.tree == nil {
		return nil
	}
	value, found := t.tree.Search(key)
	if !found {
		return nil
	}

	pos, _ := value.(*data.LogRecordPos)
	return pos
}

// Delete removes key and reports whether it was present.
func (t *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(key) == 0 {
		if !t.hasEmptyKey {
			return nil, false
		}
		oldPos := t.emptyPos
		t.emptyPos = nil
		t.hasEmptyKey = false
		return oldPos, true
	}

	old, deleted := t.tree.Delete(key)
	oldPos, _ := old.(*data.LogRecordPos)
	return oldPos, deleted
}

func (t *AdaptiveRadixTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()

	size := 0
	if t.tree != nil {
		size = t.tree.Size()
	}
	if t.hasEmptyKey {
		size++
	}
	return size
}

// Iterator returns a snapshot iterator over the index in key order.
func (t *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var iterator *artIterator
	if t.tree == nil {
		iterator = &artIterator{reverse: reverse}
	} else {
		iterator = newArtIterator(t.tree, reverse)
	}

	if t.hasEmptyKey {
		empty := &Item{pos: t.emptyPos}
		if reverse {
			iterator.values = append(iterator.values, empty)
		} else {
			iterator.values = append([]*Item{empty}, iterator.values...)
		}
	}
	return iterator
}

var _ Indexer = (*AdaptiveRadixTree)(nil)
var _ Iterator = (*artIterator)(nil)

type artIterator struct {
	curIndex int
	reverse  bool
	values   []*Item
}

func newArtIterator(tree art.Tree, reverse bool) *artIterator {
	values := make([]*Item, 0, tree.Size())
	options := art.TraverseLeaf
	if reverse {
		options |= art.TraverseReverse
	}

	tree.ForEach(func(node art.Node) bool {
		pos, _ := node.Value().(*data.LogRecordPos)
		key := append([]byte(nil), node.Key()...)
		values = append(values, &Item{key: key, pos: pos})
		return true
	}, options)

	return &artIterator{
		reverse: reverse,
		values:  values,
	}
}

func (it *artIterator) Rewind() {
	it.curIndex = 0
}

func (it *artIterator) Seek(key []byte) {
	if it.reverse {
		it.curIndex, _ = slices.BinarySearchFunc(it.values, key, func(e *Item, target []byte) int {
			return bytes.Compare(target, e.key)
		})
		return
	}

	it.curIndex, _ = slices.BinarySearchFunc(it.values, key, func(e *Item, target []byte) int {
		return bytes.Compare(e.key, target)
	})
}

func (it *artIterator) Next() {
	it.curIndex++
}

func (it *artIterator) Valid() bool {
	return it.curIndex >= 0 && it.curIndex < len(it.values)
}

func (it *artIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.values[it.curIndex].key
}

func (it *artIterator) Value() *data.LogRecordPos {
	if !it.Valid() {
		return nil
	}
	return it.values[it.curIndex].pos
}

func (it *artIterator) Close() {
	it.values = nil
	it.curIndex = 0
}
