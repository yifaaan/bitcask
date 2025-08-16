package bitcask

import "os"

// 数据库的配置项
type Options struct {
	// 数据库的数据文件目录
	DirPath string
	// 数据文件的大小阈值
	DataFileSize int64
	// 是否在每次写入的时候都立刻持久化
	SyncWrites bool
	// 使用的索引类型
	IndexType IndexerType
	// 写入多少字节后进行一次持久化
	BytesPerSync uint
	// 是否在启动时使用mmap加载数据文件
	MMapAtStart bool
}

type IndexerType = int8

const (
	BTree IndexerType = iota + 1
	ART
	BPlusTree
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    BPlusTree,
	BytesPerSync: 0,
	MMapAtStart:  true,
}

type IteratorOptions struct {
	// 遍历指定前缀的key，默认为空
	Prefix []byte
	// 是否反向
	Reverse bool
}

var DefaultIteratorOptions = IteratorOptions{Prefix: nil, Reverse: false}

type WriteBatchOptions struct {
	// 一个batch的最大数据量
	MaxBatchNum uint
	SyncWrites  bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  false,
}
