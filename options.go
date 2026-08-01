package bitcask

import "github.com/yifaaan/bitcask/index"

type Options struct {
	DirPath string // 数据目录

	DataFileSize int64 // 数据文件大小阈值

	SyncWrite bool // 每次写入都持久化？

	IndexType index.IndexType
}
