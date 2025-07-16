package bitcask

// 数据库的配置项
type Options struct {
	// 数据库的数据文件目录
	DirPath string

	// 数据文件的大小阈值
	DataFileSize int64
	// 是否在每次写入的时候都立刻持久化
	SyncWrites bool
}
