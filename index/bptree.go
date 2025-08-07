package index

import (
	"path/filepath"

	"github.com/yifaaan/bitcask/data"
	"go.etcd.io/bbolt"
)

const bptreeIndexFileName = "bptree.index"

var indexBucketName = []byte("bitcask-index")

// B+树索引
type BPlusTree struct {
	tree *bbolt.DB
}

// 创建B+树索引
func NewBPlusTree(dirPath string) *BPlusTree {
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	if err != nil {
		panic(err)
	}

	if err = bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		return b.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic(err)
	}
	return true
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		value := b.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok bool
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		if value := b.Get(key); len(value) != 0 {
			ok = true
			return b.Delete(key)
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return ok
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPTreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(indexBucketName)
		size = b.Stats().KeyN
		return nil
	}); err != nil {
		panic(err)
	}
	return size
}

type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBPTreeIterator(db *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := db.Begin(false)
	if err != nil {
		panic(err)
	}
	cursor := tx.Bucket(indexBucketName).Cursor()
	bpti := &bptreeIterator{tx: tx, cursor: cursor, reverse: reverse}
	bpti.Rewind()
	return bpti
}

func (bpti *bptreeIterator) Rewind() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Last()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.First()
	}
}

func (bpti *bptreeIterator) Seek(key []byte) {
	bpti.currKey, bpti.currValue = bpti.cursor.Seek(key)
}

func (bpti *bptreeIterator) Next() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Prev()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.Next()
	}
}

func (bpti *bptreeIterator) Valid() bool {
	return len(bpti.currKey) > 0
}

func (bpti *bptreeIterator) Key() []byte {
	return bpti.currKey
}

func (bpti *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpti.currValue)
}

func (bpti *bptreeIterator) Close() {
	_ = bpti.tx.Commit()
}
