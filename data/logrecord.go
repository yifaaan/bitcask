package data

// 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	// file id
	Fid uint32
	// file offset
	Offset int64
}
