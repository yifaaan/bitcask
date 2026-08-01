package data

type LogRecordType = byte

const (
	LOG_RECORD_NORMAL LogRecordType = iota
	LOG_RECORD_DELETED
)

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	// file id
	Fid uint32
	// file offset
	Offset int64
}

// LogRecord 写入到数据文件的记录，追加写入
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// EncodeLogRecord 对 LogRecord 编码，返回编码后的字节数组和长度
func EncodeLogRecord(lr *LogRecord) ([]byte, int64) {
	return nil, 0
}
