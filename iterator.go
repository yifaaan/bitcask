package bitcask

import (
	"bytes"

	"github.com/yifaaan/bitcask/index"
)

// 迭代器
type Iterator struct {
	// 索引迭代器
	indexIter index.Iterator
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	return &Iterator{indexIter: indexIter, db: db, options: opts}
}

// 重新回到迭代器起点
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

// 根据key找到第一个大于（或小于）等于的目标
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

// 下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

// 是否已经遍历完
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

// 当前位置的key
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

// 当前位置的数据
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// 关闭
func (it *Iterator) Close() {
	it.indexIter.Close()
}

// 找到以prefix为前缀的项
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	// 前缀为空，不用跳过
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.Key()
		if prefixLen <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixLen]) {
			return
		}
	}
}
