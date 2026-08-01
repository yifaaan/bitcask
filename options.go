package bitcask

import (
	"os"

	"github.com/yifaaan/bitcask/index"
)

type Options struct {
	DirPath string // 数据目录

	DataFileSize int64 // 数据文件大小阈值

	SyncWrite bool // 每次写入都持久化？

	IndexType index.IndexType
}

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrite:    false,
	IndexType:    index.BTREE,
}
