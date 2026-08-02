package bitcask

import (
	"os"

	"github.com/yifaaan/bitcask/index"
)

type Options struct {
	DirPath string // 数据目录

	DataFileSize int64 // 数据文件大小阈值

	SyncWrite bool // 每次写入都持久化？

	IndexType    index.IndexType
	BytesPerSync uint
	MMapAtStart  bool // 启动时使用 mmap 加载数据文件
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrite:    false,
	IndexType:    index.BTREE,
	BytesPerSync: 0,
	MMapAtStart:  true,
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	MaxBatchNum uint
	SyncWrite   bool
}
