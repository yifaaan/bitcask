package index

import (
	"bytes"
	"sort"
	"sync"

	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/yifaaan/bitcask/data"
)

type AdaptiveRadixTree struct {
	tree art.Tree
	mu   *sync.RWMutex
}

func NewAdaptiveRadixTree() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{tree: art.New(), mu: &sync.RWMutex{}}
}

// 向索引中存储key对应的数据位置信息
func (t *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tree.Insert(key, pos)
	return true
}

// 根据key取出对应的数据位置信息
func (t *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	t.mu.RLock()
	defer t.mu.RUnlock()
	value, found := t.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// 根据key删除对应的数据位置信息
func (t *AdaptiveRadixTree) Delete(key []byte) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, deleted := t.tree.Delete(key)
	return deleted
}

// 索引迭代器
func (t *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if t.tree == nil {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return newArtIterator(t.tree, reverse)
}

// Close 关闭索引
func (t *AdaptiveRadixTree) Close() error {
	return nil
}

// 大小
func (t *AdaptiveRadixTree) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tree.Size()
}

// BTree索引迭代器
type artIterator struct {
	// 当前遍历的位置
	currIndex int
	// 是否是反向遍历
	reverse bool
	// BTree中的Item索引：(key,LogRecordPos)
	values []*Item
}

func newArtIterator(tree art.Tree, reverse bool) *artIterator {
	var idx int
	values := make([]*Item, tree.Size())
	if reverse {
		idx = tree.Size() - 1
	}
	saveValues := func(node art.Node) bool {
		item := &Item{key: node.Key(), pos: node.Value().(*data.LogRecordPos)}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)
	return &artIterator{0, reverse, values}
}

func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

func (ai *artIterator) Next() {
	ai.currIndex += 1
}

func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

func (ai *artIterator) Close() {
	ai.values = nil
}
